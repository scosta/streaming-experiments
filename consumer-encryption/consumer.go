package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/awskms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/azurekms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/gcpkms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/hcvault"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/rules/encryption/localkms"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avrov2"
	"github.com/joho/godotenv"
)

func main() {
	// Load environment variables from .env file
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Read environment variables
	kafkaBootstrapServers := os.Getenv("KAFKA_BOOTSTRAP_SERVERS")
	kafkaSaslUsername := os.Getenv("KAFKA_SASL_USERNAME")
	kafkaSaslPassword := os.Getenv("KAFKA_SASL_PASSWORD")
	schemaregistryUrl := os.Getenv("SCHEMA_REGISTRY_URL")
	schemaRegistryUsername := os.Getenv("SCHEMA_REGISTRY_USERNAME")
	schemaRegistryPassword := os.Getenv("SCHEMA_REGISTRY_PASSWORD")

	// Register the KMS drivers and the field-level encryption executor
	awskms.Register()
	azurekms.Register()
	gcpkms.Register()
	hcvault.Register()
	localkms.Register()
	encryption.Register()

	// Kafka consumer configuration
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     kafkaSaslUsername,
		"sasl.password":     kafkaSaslPassword,
		"group.id":          "consumer-group-1",
		"auto.offset.reset": "earliest",
	}

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(kafkaConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	// Subscribe to the Kafka topic
	topic := "users-csfle"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Schema Registry configuration
	srConfig := schemaregistry.NewConfigWithBasicAuthentication(
		schemaregistryUrl,
		schemaRegistryUsername,
		schemaRegistryPassword)

	// Create Schema Registry client
	srClient, err := schemaregistry.NewClient(srConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Create Avro deserializer
	deserializer, err := avrov2.NewDeserializer(srClient, serde.ValueSerde, avrov2.NewDeserializerConfig())
	if err != nil {
		log.Fatal(err)
	}

	// Handle system signals for graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("Consumer started...")
	run := true
	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				user := User{}
				err := deserializer.DeserializeInto(*e.TopicPartition.Topic, e.Value, &user)
				if err != nil {
					log.Printf("Failed to deserialize message: %v\n", err)
				} else {
					userJSON, err := json.Marshal(user)
					if err != nil {
						log.Fatal(err)
					}
					fmt.Println(string(userJSON))
				}
			case kafka.Error:
				log.Printf("Error: %v\n", e)
				run = false
			default:
				// Ignore other events
			}
		}
	}
}
