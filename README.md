# Wikimedia_Stream_to_Opensearch
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

## Setup and Prerequisites

### 1. Install the following software:
- **Apache Kafka:** [Download Kafka](https://kafka.apache.org/downloads)
- **OpenSearch:** [Download OpenSearch](https://opensearch.org/downloads.html)
- **Python 3.7+** with the following libraries:
  - `kafka-python`
  - `sseclient`
  - `opensearch-py`

**Install Python dependencies:**
```bash
pip install kafka-python sseclient opensearch-py
