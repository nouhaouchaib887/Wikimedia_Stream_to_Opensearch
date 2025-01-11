from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StringType, TimestampType

# Initialize the Spark session
spark = SparkSession.builder.appName("WikimediaKafkaStreaming").config("spark.sql.shuffle.partitions", 1).getOrCreate()

# Define the schema for the incoming JSON data
schema = StructType().add("id", StringType()).add("type", StringType()).add("title", StringType()).add("timestamp", TimestampType()).add("user", StringType()).add("bot", StringType()).add("comment", StringType()).add("url", StringType())

# Read from Kafka
wikimedia_raw_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "wikimedia.recentchange").option("startingOffsets", "earliest").load()

# Convert binary values to string and apply schema
wikimedia_df = wikimedia_raw_df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

# Stream processing logic: example aggregations
bot_edits_df = wikimedia_df.filter(col("bot") == "true").groupBy(window(col("timestamp"), "1 minute")).count().withColumnRenamed("count", "bot_count")

website_edits_df = wikimedia_df.groupBy("url").count().withColumnRenamed("count", "website_count")

time_series_df = wikimedia_df.groupBy(window(col("timestamp"), "1 minute"), "type").count().withColumnRenamed("count", "type_count")

# Publish each DataFrame back to Kafka as new topics
bot_edits_df.selectExpr("to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "wikimedia.bot.edits").option("checkpointLocation", "/tmp/checkpoints/bot_count").outputMode("complete").start()

website_edits_df.selectExpr("to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "wikimedia.website.edits").option("checkpointLocation", "/tmp/checkpoints/website_count").outputMode("complete").start()

time_series_df.selectExpr("to_json(struct(*)) AS value").writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("topic", "wikimedia.timeseries.edits").option("checkpointLocation", "/tmp/checkpoints/timeseries_count").outputMode("complete").start()

# Await termination of all streaming queries
spark.streams.awaitAnyTermination()