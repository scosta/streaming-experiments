package main

type User struct {
	FirstName   string `avro:"first_name"`
	LastName    string `avro:"last_name"`
	PhoneNumber string `avro:"phone_number"`
	Address     string `avro:"address"`
	Age         int    `avro:"age"`
}
