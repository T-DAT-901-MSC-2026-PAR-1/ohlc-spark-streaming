import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    window,
    expr,
    to_json,
    struct,
    concat,
    lit,
    from_unixtime,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    LongType,
)

# Configuration via environment variables (reasonable defaults)
SPARK_APP_NAME: str = os.environ.get("SPARK_APP_NAME")
SPARK_MASTER_URL: str = os.environ.get("SPARK_MASTER_URL")
KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
# Peut être une regex (subscribePattern) ou une liste comma-separated (subscribe)
KAFKA_SUBSCRIBE_TOPICS: str = os.environ.get("KAFKA_SUBSCRIBE_TOPICS")
KAFKA_OUTPUT_PREFIX: str = os.environ.get("KAFKA_OUTPUT_PREFIX")
CHECKPOINT_LOCATION: str = os.environ.get("CHECKPOINT_LOCATION")
WINDOW_DURATION: str = os.environ.get("WINDOW_DURATION")
WATERMARK_DELAY: str = os.environ.get("WATERMARK_DELAY")


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder.master(SPARK_MASTER_URL)
        .appName(SPARK_APP_NAME)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1")
        .getOrCreate()
    )
    # Set log level to INFO or DEBUG for detailed output
    spark.sparkContext.setLogLevel("INFO")
    return spark


def get_parsed_trade_schema() -> StructType:
    """Schema for parsing Kafka JSON messages.
    Note: All numeric fields are stored as strings in the JSON.
    We'll convert them to proper types after parsing."""
    return StructType([
        StructField("type", StringType()),
        StructField("market", StringType()),
        StructField("from_symbol", StringType()),
        StructField("to_symbol", StringType()),
        StructField("flags", StringType()),
        StructField("trade_id", StringType()),
        StructField("timestamp", StringType()),  # Will convert to timestamp
        StructField("quantity", StringType()),   # Will convert to double
        StructField("price", StringType()),      # Will convert to double
        StructField("total_value", StringType()),  # Will convert to double
        StructField("received_ts", StringType()),  # Will convert to timestamp
        StructField("ccseq", StringType()),
        StructField("timestamp_ns", StringType()),
        StructField("received_ts_ns", StringType()),
    ])


if __name__ == "__main__":
    # Build Spark session
    spark: SparkSession = build_spark()

    parsed_trade_schema = get_parsed_trade_schema()

    # Read from Kafka
    reader = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)

    # support subscribePattern or subscribe (comma-separated list)
    if "," in KAFKA_SUBSCRIBE_TOPICS or not ("*" in KAFKA_SUBSCRIBE_TOPICS or "." in KAFKA_SUBSCRIBE_TOPICS):
        # if multiple topics separated by commas or a single topic without a pattern
        reader = reader.option("subscribe", KAFKA_SUBSCRIBE_TOPICS)
    else:
        # by default, use pattern subscription
        reader = reader.option("subscribePattern", KAFKA_SUBSCRIBE_TOPICS)

    input_df: DataFrame = reader.option("startingOffsets", "earliest").load()

    # DEBUG: Check raw Kafka messages
    debug_kafka_raw = (
        input_df.selectExpr("topic", "CAST(value AS STRING) as value_str")
        .writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", "3")
        .outputMode("append")
        .queryName("debug_kafka_raw")
        .start()
    )

    # Parse JSON value
    parsed = input_df.select(
        col("topic"),
        from_json(col("value").cast("string"), parsed_trade_schema).alias("data"),
    )

    # DEBUG: Check parsed data structure
    debug_parsed = (
        parsed.selectExpr("topic", "data.*")
        .writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", "3")
        .outputMode("append")
        .queryName("debug_parsed")
        .start()
    )

    trades_df = parsed.select(
        col("topic"),
        col("data.from_symbol").alias("from_symbol"),
        col("data.to_symbol").alias("to_symbol"),
        # Convert Unix epoch seconds (stored as string) to TimestampType
        from_unixtime(col("data.timestamp").cast("long")).cast("timestamp").alias("timestamp"),
        col("data.price").cast("double").alias("price"),
        col("data.quantity").cast("double").alias("quantity"),
    ).where(col("timestamp").isNotNull())

    # Extract base from topic name parsed-trades-<base>-usdt
    # fallback: use from_symbol if regex doesn't match
    trades_df = trades_df.withColumn(
        "base",
        expr(
            f"CASE WHEN topic RLIKE 'parsed-trades-.+-usdt' THEN regexp_extract(topic, 'parsed-trades-([^\\-]+)-usdt', 1) ELSE from_symbol END"
        ),
    )

    # DEBUG: Print trades before aggregation to check data
    debug_raw_trades = (
        trades_df
        .writeStream
        .format("console")
        .option("truncate", "false")
        .option("numRows", "5")
        .outputMode("append")
        .queryName("debug_raw_trades")
        .start()
    )

    # Compute 1-minute OHLC per base
    # collect the list (timestamp,price), sort it, then take the first and last for open/close
    # open: element 1, close: element at size
    open_expr = (
        "element_at(transform(array_sort(collect_list(named_struct('t', timestamp, 'p', price))), x -> x.p), 1)"
    )
    close_expr = (
        "element_at(transform(array_sort(collect_list(named_struct('t', timestamp, 'p', price))), x -> x.p), size(collect_list(named_struct('t', timestamp, 'p', price))))"
    )

    agg_df = (
        trades_df.withWatermark("timestamp", WATERMARK_DELAY)
        .groupBy(window(col("timestamp"), WINDOW_DURATION), col("base"))
        .agg(
            expr(open_expr).alias("open"),
            expr("min(price)").alias("low"),
            expr("max(price)").alias("high"),
            expr(close_expr).alias("close"),
            expr("sum(quantity)").alias("volume"),
        )
    )

    # Build output topic and serialize to JSON
    out = agg_df.select(
        concat(lit(KAFKA_OUTPUT_PREFIX), col("base"), lit("-usdt")).alias("topic"),
        col("base").alias("key"),
        to_json(
            struct(
                col("base"),
                col("window.start").alias("start_ts"),
                col("window.end").alias("end_ts"),
                col("open"),
                col("high"),
                col("low"),
                col("close"),
                col("volume"),
            )
        ).alias("value"),
    )

    # DEBUG: Add console sink to see what's being written
    debug_query = (
        out.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("update")
        .queryName("console_debug")
        .start()
    )

    # Ensure we isolate checkpoint for this particular streaming query to avoid
    # collisions when multiple queries are writing to the same Kafka cluster.
    if not CHECKPOINT_LOCATION:
        raise ValueError("CHECKPOINT_LOCATION environment variable must be set for Kafka sink checkpointing")

    kafka_checkpoint = os.path.join(CHECKPOINT_LOCATION, "kafka_sink")

    query = (
        out.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("checkpointLocation", kafka_checkpoint)
        .queryName("kafka_sink")
        .outputMode("update")
        .start()
    )


    # Block until any query terminates
    spark.streams.awaitAnyTermination()