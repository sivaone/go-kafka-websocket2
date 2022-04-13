package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"
	"log"
	"strings"
)

func main() {
	log.Println("Hello kafka")

	viper.SetConfigFile(".env")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Error reading env file: %w \n", err))
	}

	kafkaUrl := viper.GetString("KAFKA_BOOTSTRAP_SERVERS")
	kafkaTopic := viper.GetString("KAFKA_CONSUMER_TOPIC")
	kafkaConsumerGroupId := viper.GetString("KAFKA_CONSUMER_GROUP")

	reader := getKafkaReader(kafkaUrl, kafkaTopic, kafkaConsumerGroupId)
	defer reader.Close()

	log.Println("start consuming topic ... ", kafkaTopic)
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

}

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:  brokers,
		GroupID:  groupID,
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e4, // 100KB
	})
}
