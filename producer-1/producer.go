package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
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

	// Kafka configuration
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers": kafkaBootstrapServers,
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     kafkaSaslUsername,
		"sasl.password":     kafkaSaslPassword,
	}

	// Create Kafka producer
	producer, err := kafka.NewProducer(kafkaConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

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

	// Read Avro schema
	schemaBytes, err := os.ReadFile("user.avsc")
	if err != nil {
		log.Fatal(err)
	}

	// Create Avro serializer
	serConfig := avrov2.NewSerializerConfig()
	serConfig.AutoRegisterSchemas = false
	serConfig.UseLatestVersion = true
	serializer, err := avrov2.NewSerializer(srClient, serde.ValueSerde, serConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Register schema
	topic := "users"
	schemaInfo := schemaregistry.SchemaInfo{
		Schema:     string(schemaBytes),
		SchemaType: "AVRO",
	}
	schemaID, err := srClient.Register(topic+"-value", schemaInfo, true)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Schema registered with ID: %d\n", schemaID)

	// Sample customer data
	users := []User{
		{FirstName: "John", LastName: "Doe", PhoneNumber: "123-456-7890", Address: "123 Main St, Anytown, USA", Age: 30},
		{FirstName: "Jane", LastName: "Smith", PhoneNumber: "987-654-3210", Address: "456 Elm St, Othertown, USA", Age: 28},
		{FirstName: "Bob", LastName: "Johnson", PhoneNumber: "555-555-5555", Address: "789 Oak St, Somewhere, USA", Age: 35},
	}

	// Produce messages
	fmt.Println("Producing messages... ")
	for _, user := range users {
		payload, err := serializer.Serialize(topic, &user)
		if err != nil {
			log.Fatal(err)
		}

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
		}, nil)

		if err != nil {
			log.Fatal(err)
		}

		userJSON, err := json.Marshal(user)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(userJSON))
	}

	// Wait for messages to be delivered
	producer.Flush(15 * 1000)
	fmt.Println("Messages produced successfully")
}
