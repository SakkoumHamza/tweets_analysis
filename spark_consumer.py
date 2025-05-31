from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, from_json
from pyspark.sql.types import StructType, StringType, LongType

spark = SparkSession.builder \
    .appName("RedditKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "reddit_posts") \
    .load()

kafka_df.selectExpr("CAST(value AS STRING)").writeStream \
    .format("console") \
    .outputMode("append") \
    .start() \
    .awaitTermination()
