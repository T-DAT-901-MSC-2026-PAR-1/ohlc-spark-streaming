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
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
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
    return StructType([
        StructField("type", StringType()),
        StructField("market", StringType()),
        StructField("from_symbol", StringType()),
        StructField("to_symbol", StringType()),
        StructField("flags", StringType()),
        StructField("trade_id", StringType()),
        StructField("timestamp", TimestampType()),
        StructField("quantity", DoubleType()),
        StructField("price", DoubleType()),
        StructField("total_value", DoubleType()),
        StructField("received_ts", TimestampType()),
        StructField("ccseq", StringType()),
        StructField("timestamp_ns", TimestampType()),
        StructField("received_ts_ns", TimestampType()),
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

    # Parse JSON value
    parsed = input_df.select(
        col("topic"),
        from_json(col("value").cast("string"), parsed_trade_schema).alias("data"),
    )

    trades_df = parsed.select(
        col("topic"),
        col("data.from_symbol").alias("from_symbol"),
        col("data.to_symbol").alias("to_symbol"),
        col("data.timestamp").alias("timestamp"),
        col("data.price").alias("price"),
        col("data.quantity").alias("quantity"),
    ).where(col("timestamp").isNotNull())

    # Extract base from topic name parsed-trades-<base>-usdt
    # fallback: use from_symbol if regex doesn't match
    trades_df = trades_df.withColumn(
        "base",
        expr(
            f"CASE WHEN topic RLIKE 'parsed-trades-.+-usdt' THEN regexp_extract(topic, 'parsed-trades-([^\\-]+)-usdt', 1) ELSE from_symbol END"
        ),
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

    # Write to Kafka. Kafka sink uses the 'topic' column to route if present.
    # Use 'append' mode since Kafka sink doesn't support 'update' mode
    # Watermark ensures that windows are closed and emitted once they're complete
    # Important: column order must be topic, key, value (as per Kafka documentation)
    
    # DEBUG: Add console sink to see what's being written
    debug_query = (
        out.selectExpr("topic", "CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("console")
        .option("truncate", "false")
        .outputMode("append")
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
        .outputMode("append")
        .start()
    )

    # Print streaming query status periodically for debugging
    import time
    # Print started queries info to map them easily to the Spark UI (name/id/runId)
    try:
        print(f"[DEBUG] Started debug_query name={getattr(debug_query, 'name', None)} id={debug_query.id} runId={debug_query.runId}")
    except Exception:
        print(f"[DEBUG] Started debug_query id={debug_query.id} runId={debug_query.runId}")

    try:
        print(f"[DEBUG] Started kafka query name={getattr(query, 'name', None)} id={query.id} runId={query.runId}")
    except Exception:
        print(f"[DEBUG] Started kafka query id={query.id} runId={query.runId}")

    # Loop and print statuses for each active query
    while debug_query.isActive or query.isActive:
        time.sleep(10)
        for q in (debug_query, query):
            try:
                print(f"[DEBUG] name={getattr(q, 'name', None)} id={q.id} runId={q.runId} isActive={q.isActive} status={q.status}")
                print(f"[DEBUG] Last Progress: {q.lastProgress}")
            except Exception as e:
                print(f"[DEBUG] error reading query info: {e}")

    # Block until any query terminates
    spark.streams.awaitAnyTermination()