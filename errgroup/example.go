package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
)

func main() {
	cancelCtx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(cancelCtx)

	g.Go(func() error {
		i := 0
		for {
			i++
			if i > 100 {
				return errors.New("goroutine 1 finished")
			}
			select {
			case <-ctx.Done():
				err := ctx.Err()
				fmt.Println("goroutine 1 canceled:", err)
				return err
			default:
				fmt.Println("running goroutine 1")
				time.Sleep(time.Second)
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				err := ctx.Err()
				fmt.Println("goroutine 2 canceled:", err)
				return err
			default:
				fmt.Println("running goroutine 2")
				time.Sleep(time.Second * 2)
			}
		}
	})

	// 注册 ctrl+c 信号
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		cancel()
	}()

	if err := g.Wait(); err != nil {
		fmt.Println("error:", err)
	}
}
