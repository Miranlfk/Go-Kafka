package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {

	topic := "comments"
	consumersURL := []string{"localhost:29092"}
	consumer, err := connectConsumer(consumersURL)
	if err != nil {
		panic(err)
	}

	consmr, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneChannel := make(chan struct{})

	go func() {
		for {
			select{
			case err := <-consmr.Errors():
				fmt.Println(err)
			
			case msg := <-consmr.Messages():
				msgCount++
				fmt.Printf("Received message Count %d: | Topic(%s) | Message(%s) \n", msgCount, string(msg.Topic), string(msg.Value))
			case <-sigchan:
				fmt.Println("Interruption is detected")
				doneChannel <- struct{}{}
			}
		}
	}()

		<-doneChannel
		fmt.Println("Have processed" , msgCount, " messages" )
		if err := consmr.Close(); err != nil {
			panic(err)
		}
}

func connectConsumer(brokersURL []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	
	conn, err := sarama.NewConsumer(brokersURL, config)
	if err != nil {
		return nil, err
	}
	return conn, nil


}