#!/usr/bin/env bash
set -euo pipefail

printf "[entrypoint] starting spark submit runner\n"

# Required environment variables (no defaults - fail fast if missing)
: "${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is required}"
: "${KAFKA_SUBSCRIBE_TOPICS:?KAFKA_SUBSCRIBE_TOPICS is required}"
: "${KAFKA_OUTPUT_PREFIX:?KAFKA_OUTPUT_PREFIX is required}"
: "${CHECKPOINT_LOCATION:?CHECKPOINT_LOCATION is required}"
: "${WINDOW_DURATION:?WINDOW_DURATION is required}"
: "${WATERMARK_DELAY:?WATERMARK_DELAY is required}"
: "${SPARK_APP_NAME:?SPARK_APP_NAME is required}"
: "${SPARK_MASTER_URL:?SPARK_MASTER_URL is required}"

# Allow passing an alternative command to the container (for debugging)
if [ "$#" -gt 0 ]; then
  echo "[entrypoint] executing provided command: $@"
  exec "$@"
fi

# Construct spark-submit command
# In client deploy mode, the driver runs in this container but tasks execute on workers.
# Workers need access to Python files. With --py-files we can distribute the main script.
# For a single-file app, we copy main.py or use local[*] mode for testing.
# Note: spark-sql-kafka doesn't require executor-side code for most streaming operations.
SPARK_CMD=(/opt/spark/bin/spark-submit
  --master "${SPARK_MASTER_URL}"
  --deploy-mode client
  --conf "spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
  /opt/app/main.py
)

echo "[entrypoint] running: ${SPARK_CMD[*]}"
exec "${SPARK_CMD[@]}"
