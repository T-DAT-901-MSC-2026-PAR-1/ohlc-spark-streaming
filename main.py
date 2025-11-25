import os
from pyspark.sql import SparkSession

SPARK_APP_NAME: str = os.environ.get('SPARK_APP_NAME')
SPARK_MASTER_URL: str = os.environ.get('SPARK_MASTER_URL')
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_SUBSCRIBE_TOPICS: str = os.environ.get('KAFKA_SUBSCRIBE_TOPICS')

# Sources:
# https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html
# https://medium.com/@jaya.aiyappan/reading-json-message-from-kafka-topic-and-process-using-spark-structured-streaming-and-write-it-a7fee670c159
# https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10

spark: SparkSession = SparkSession \
    .builder \
    .master(SPARK_MASTER_URL) \
    .appName(SPARK_APP_NAME) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_4.0.1") \
    .getOrCreate()

parsedTrades = spark \
    .readStream \
    .format("kafka") \
    .option("kakfa.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_SUBSCRIBE_TOPICS) \
    .option("startingOffsets", "earliest") \
    .load()


def main():
   ... 


if __name__ == "__main__":
    main()
