package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
}

func main() {

	app := fiber.New()

	for true {
		err := createComment(&fiber.Ctx{})
		if err != nil {
			fmt.Println(err)
		}

		time.Sleep(1 * time.Second)
	}

	app.Listen(":3000")
}

func createComment(c *fiber.Ctx) error {
	cmt := "Message from kafka"
	cmtInBytes, err := json.Marshal(cmt)
	PushCommentToQueue("comments", cmtInBytes)

	if err != nil {
		return err
	}

	return err

}

func PushCommentToQueue(topic string, message []byte) error {

	brokersUrl := []string{"localhost:9092"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	fmt.Println("Message Sent")
	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	fmt.Println(err)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
