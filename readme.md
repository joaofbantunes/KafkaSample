# Kafka .NET sample

## About

Simple .NET sample implementation of publisher and consumer applications.

Uses [Confluent Kakfa .NET Client](https://github.com/confluentinc/confluent-kafka-dotnet).

For message serialization, just went with JSON, nothing fancy like Avro and eventually [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) integration.

## Setting up Kafka for testing

To have a target Kafka installation, use the accompanying `docker-compose.yml` file:

```bash
docker-compose up -d
```

To take a look at what goes on inside Kafka (brokers, consumers, ...), go to `http://localhost:9021`, where Confluent Control Center is exposed.

Not everything works, as the setup in Docker Compose is only starting a Kafka broker, ZooKeeper and the Control Center itself, not everything (e.g. KSQL Server).

Additionally, the metrics also don't show, as it appears it's a Confluent Platform Enterprise feature.
