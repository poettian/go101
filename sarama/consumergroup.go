package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	brokers  = ""
	version  = ""
	group    = ""
	topics   = ""
	assignor = ""
	oldest   = true
	verbose  = false
)

func init() {
	flag.StringVar(&brokers, "brokers", "", "Kafka bootstrap brokers to connect to, as a comma separated list")
	flag.StringVar(&group, "group", "", "Kafka consumer group definition")
	flag.StringVar(&version, "version", sarama.DefaultVersion.String(), "Kafka cluster version")
	flag.StringVar(&topics, "topics", "", "Kafka topics to be consumed, as a comma separated list")
	flag.StringVar(&assignor, "assignor", "range", "Consumer group partition assignment strategy (range, roundrobin, sticky)")
	flag.BoolVar(&oldest, "oldest", true, "Kafka consumer consume initial offset from oldest")
	flag.BoolVar(&verbose, "verbose", false, "Sarama logging")
	flag.Parse()

	if len(brokers) == 0 {
		panic("no Kafka bootstrap brokers defined, please set the -brokers flag")
	}

	if len(topics) == 0 {
		panic("no topics given to be consumed, please set the -topics flag")
	}

	if len(group) == 0 {
		panic("no Kafka consumer group defined, please set the -group flag")
	}
}

func main() {
	keepRunning := true
	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	config := sarama.NewConfig()
	config.Version = version

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	}

	config.Consumer.Return.Errors = true

	// consumer 代表了一个消费者的 handler 实例，它实现了 ConsumerGroupHandler 接口，实现消费消息的逻辑
	// 它持有一个 ConsumerGroupSession 实例，这个实例代表一个消费者 session，
	// 可以通过这个 session，获取消费者被分配到的 topic/partition 信息，以及使用这个 session 来标记消息和管理 offset
	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	go func() {
		for err := range client.Errors() {
			log.Printf("Error from consumer: %v", err)
			cancel()
		}
	}()

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			log.Printf("Consume loop!!!")

			// Consume 方法实现加入消费者组，并开启一个阻塞的 ConsumerGroupSession 实例
			// 当 server-side rebalance 发生时，消费者 session 会被重新创建，以获取新的 claims
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					return
				}
				log.Panicf("Error when consume: %v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				log.Printf("Consume loop cancel!!!")
				return
			}

			consumer.ready = make(chan bool)
		}
	}()

	// 等待 consumer Setup 完成
	<-consumer.ready
	log.Println("Sarama consumer up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	// 等待 ctx Done 或信号，退出循环
	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1: // 自定义信号，用于暂停/恢复消费
			toggleConsumptionFlow(client, &consumptionIsPaused)
		}
	}
	// 触发 cancel，关闭 ctx，停止 Consume 方法
	cancel()

	// 这里需要等待，因为 cancel 后，ConsumerGroupSession 会执行后续的一系列流程
	// 重点看下 Consume 方法的注释说明
	wg.Wait()

	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready chan bool
}

// 在每个 session 开始时，会调用一次 Setup 方法，只会调用一次
func (consumer *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("Setup: %v, %v", session.MemberID(), session.Claims())
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// 在每个 session 结束时，会调用一次 Cleanup 方法，只会调用一次
func (consumer *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Printf("Cleanup: %v, %v", session.MemberID(), session.Claims())
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
// Once the Messages() channel is closed, the Handler must finish its processing
// loop and exit.
// 先解释一个术语 claim：它代表一个消费者被分配到的 topic/partition
// 根据 session 持有的 claims，会创建多个 goroutine，在每个 goroutine 中，会调用 ConsumeClaim 方法
// 如果其中一个 goroutine 中的 ConsumeClaim 方法退出，则其它 goroutine 也会退出
// 等到所有 goroutine 退出后，会调用 Cleanup 方法，退出 Consume 方法
// 但又会在上面的循环中重新调用 Consume 方法，创建新的 session
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	log.Printf("ConsumeClaim: %v, %v", session.MemberID(), session.Claims())

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Printf("message channel was closed")
				return nil
			}
			log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
			session.MarkMessage(message, "")
			if string(message.Value) == "stop" {
				log.Printf("Stop!!!")
				return nil
			}
			if string(message.Value) == "exit" {
				return errors.New("exit")
			}
		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/IBM/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
