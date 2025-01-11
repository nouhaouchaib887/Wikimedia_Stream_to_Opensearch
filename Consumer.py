# Import necessary modules
import time  # Provides sleep functionality, although not used here directly
import json  # For handling JSON data, converting Kafka messages from JSON strings to Python dictionaries
from kafka import KafkaConsumer  # KafkaConsumer allows us to consume messages from a Kafka topic
from opensearchpy import OpenSearch, RequestsHttpConnection  # OpenSearch client for connecting to OpenSearch
from opensearchpy.helpers import bulk  # Helper function to perform bulk operations in OpenSearch
import logging  # For logging information and errors
import signal  # For handling termination signals (e.g., Ctrl+C)
import sys  # Provides access to system-related functions, such as exit

# Kafka configuration
KAFKA_SERVER = '127.0.0.1:9092'  # Address of Kafka broker
TOPIC = 'wikimedia.recentchange'  # Kafka topic to consume messages from
GROUP_ID = 'consumer-opensearch-demo'  # Unique identifier for Kafka consumer group

# OpenSearch configuration
OPENSEARCH_HOST = 'localhost'  # OpenSearch server address
OPENSEARCH_PORT = 9200  # OpenSearch server port
INDEX_NAME = 'wikimedia'  # Name of the OpenSearch index to store data

# Set up logging with a custom format for detailed debug information
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("OpenSearchConsumer")  # Logger instance for logging messages from this script

# Function to create an OpenSearch client
def create_opensearch_client():
    # Initialize the OpenSearch client connection with basic settings
    client = OpenSearch(
        hosts=[{'host': OPENSEARCH_HOST, 'port': OPENSEARCH_PORT}],  # Host and port configuration
        http_auth=None,  # No authentication for simplicity; adjust if needed for production
        use_ssl=False,  # SSL not used here; adjust for secure environments
        verify_certs=False,  # Disables certificate verification, generally for testing
        connection_class=RequestsHttpConnection  # Uses Requests-based connection for HTTP compatibility
    )
    return client  # Returns the OpenSearch client instance

# Function to create a Kafka consumer
def create_kafka_consumer():
    # Configure and initialize the Kafka consumer
    consumer = KafkaConsumer(
        TOPIC,  # Subscribes to the specified Kafka topic
        bootstrap_servers=KAFKA_SERVER,  # Kafka broker address
        group_id=GROUP_ID,  # Consumer group ID, for tracking consumption across multiple consumers
        auto_offset_reset='latest',  # Reads from the latest offset if no previous offset is available
        enable_auto_commit=False,  # Manually commit offsets after processing
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))  # Deserializes JSON messages
    )
    return consumer  # Returns the Kafka consumer instance

# Helper function to extract a unique ID from each Kafka record, required for OpenSearch indexing
def extract_id(record_value):
    return record_value.get('meta', {}).get('id', None)  # Extracts 'id' from the 'meta' field in JSON record

# Signal handler for graceful shutdown of Kafka consumer and OpenSearch client
def signal_handler(sig, frame):
    logger.info("Detected a shutdown signal. Exiting gracefully.")
    consumer.close()  # Closes the Kafka consumer connection
    sys.exit(0)  # Exits the script

# Attach the signal handler to the interrupt signal (SIGINT, typically triggered by Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)

# Main script execution
if __name__ == "__main__":
    # Initialize OpenSearch and Kafka clients
    opensearch_client = create_opensearch_client()  # Creates the OpenSearch client
    consumer = create_kafka_consumer()  # Creates the Kafka consumer

    # Check if the OpenSearch index exists; if not, create it
    if not opensearch_client.indices.exists(index=INDEX_NAME):
        opensearch_client.indices.create(index=INDEX_NAME)  # Creates index if it doesn't exist
        logger.info(f"The index '{INDEX_NAME}' has been created.")
    else:
        logger.info(f"The index '{INDEX_NAME}' already exists.")

    try:
        # Continuously poll for messages from Kafka
        while True:
            records = consumer.poll(timeout_ms=3000)  # Polls messages with a 3-second timeout
            record_count = sum(len(record_list) for record_list in records.values())

            # Log received record count for debugging
            if record_count > 0:
                logger.info(f"Received {record_count} record(s) from Kafka.")
            else:
                logger.info("No new records received from Kafka.")

            bulk_data = []  # Initialize list to store data for bulk indexing

            # Iterate through each topic partition and its associated records
            for topic_partition, record_list in records.items():
                for record in record_list:
                    record_id = extract_id(record.value)  # Extracts a unique ID from each record
                    if record_id:
                        # Prepare OpenSearch document with index, ID, and source content
                        doc = {
                            "_index": INDEX_NAME,  # Target index in OpenSearch
                            "_id": record_id,  # Unique document ID in OpenSearch
                            "_source": record.value  # Content of the document from Kafka message
                        }
                        bulk_data.append(doc)  # Adds document to bulk indexing list
                    else:
                        logger.warning("Record ID not found; skipping record.")  # Log if ID extraction fails

            # Perform bulk indexing if there are documents to index
            if bulk_data:
                try:
                    success, _ = bulk(opensearch_client, bulk_data)  # Bulk indexes documents into OpenSearch
                    logger.info(f"Indexed {success} records to OpenSearch.")
                except Exception as e:
                    logger.error(f"Failed to index records to OpenSearch: {e}")  # Log indexing error

                consumer.commit()  # Manually commit offsets after successful processing
                logger.info("Offsets have been committed.")

    except Exception as e:
        logger.error("Unexpected error", exc_info=e)  # Log any unexpected errors that occur during execution

    finally:
        # Ensure resources are closed gracefully upon script exit
        consumer.close()  # Close Kafka consumer
        opensearch_client.close()  # Close OpenSearch client
        logger.info("Consumer and OpenSearch client are now gracefully shut down.")