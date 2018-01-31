package main

import (
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bitly/go-nsq"
)

type NoopNSQLogger struct{}

func (l *NoopNSQLogger) Output(calldepth int, s string) error {
	log.Print(s)
	return nil
}

type MessageHandler struct{}

func (h *MessageHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		return errors.New("body is blank re-enqueue message")
	}
	log.Print(m.Body)
	return nil
}

func main() {
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer("test", "liu", config)
	if err != nil {
		log.Fatal(err)
	}

	consumer.ChangeMaxInFlight(10)

	consumer.SetLogger(
		&NoopNSQLogger{},
		nsq.LogLevelInfo,
	)

	consumer.AddConcurrentHandlers(
		&MessageHandler{},
		20,
	)

	if err := consumer.ConnectToNSQLookupd("127.0.0.1:4161"); err != nil {
		log.Print(err)
	}

	shutdown := make(chan os.Signal, 2)
	signal.Notify(shutdown, syscall.SIGINT)

	for {
		select {
		case <-consumer.StopChan:
		case <-shutdown:
			consumer.Stop()
		}
	}
}