# Kafka Project: Wikimedia Stream to OpenSearch

## Project Overview

This project demonstrates a data pipeline that retrieves real-time event streams from the Wikimedia `RecentChange` API, sends the data to **Apache Kafka**, and then indexes the data into **OpenSearch** for storage, analysis, and visualization.

---

## Architecture

1. **Wikimedia API (SSE):**
   - A real-time stream of page changes (edits, creations, deletions) on Wikimedia projects.

2. **Kafka Producer:**
   - Retrieves events from the Wikimedia API and sends them to a Kafka topic.

3. **Kafka Broker:**
   - Acts as a distributed messaging platform to store and forward events.

4. **Kafka Consumer:**
   - Reads the data from Kafka and sends it to OpenSearch.

5. **OpenSearch:**
   - Stores the data for searching and visualization.

6. **OpenSearch Dashboards:**
   - Creates visualizations and dashboards for real-time monitoring.

---

## Project Files

- **`kafka_producer.py`:**
  Kafka Producer script to send Wikimedia events to Kafka.
- **`kafka_consumer.py`:**
  Kafka Consumer script to read data from Kafka and index it into OpenSearch.
- **`requirements.txt`:**
  List of Python dependencies.

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
### 2. Run Kafka Producer

   ```bash
   python kafka_producer.py
