# producer-encryption

Simple producer that writes `User` objects with an encrypted field to a Kafka topic.

Requires a `.env` file in the `producer-1` folder with the following content.
```
# .env
KAFKA_BOOTSTRAP_SERVERS=<YOUR_BOOTSTRAP_SERVER>
KAFKA_SASL_USERNAME=<KAFKA_API_KEY>
KAFKA_SASL_PASSWORD=<KAFKA_API_KEY_SECRET>
SCHEMA_REGISTRY_URL=<YOUR_SCHEMA_REGISTRY_URL>
SCHEMA_REGISTRY_USERNAME=<SR_API_KEY>
SCHEMA_REGISTRY_PASSWORD=<SR_API_KEY_SECRET>
KEK_NAME=<KEK_NAME>
KMS_TYPE=[aws-kms|azure-kms|gcp-kms|hcvault]
KMS_KEY_ID=<KMS_KEY_ID>
```

Build with `go build -o out/producer-encryption`