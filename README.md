# Kafka Project: Wikimedia Stream to OpenSearch

## Project Overview

his project uses Kafka to ingest real-time data from the Wikimedia stream, Apache Spark Streaming to process the data continuously, and OpenSearch to index and analyze the data.

---


### Key Components:
1. **Producer** :  Fetches events from Wikimedia and sends them to Kafka.
2. **Spark Streaming** : Continuously processes Kafka data and performs aggregations.
3. **Consumer** : Sends the processed Kafka data to OpenSearch.

---

## Execution Steps

### 1. Start Kafka

1. Download and extract Kafka.

2. **Start Zookeeper:**
   ```bash
   bin/zookeeper-server-start.sh config/zookeeper.properties
3. **Start Kafka broker:**
   ```bash
   bin/kafka-server-start.sh config/server.properties
4. **Create the Kafka topic::**
   ```bash
   bin/kafka-topics.sh --create --topic wikimedia.recentchange --bootstrap-server localhost:9092
5. **Start OpenSearch:**
   ```bash
   ./bin/opensearch
   
### 2. **Run Kafka Producer**
   ```bash
   python kafka_producer.py










