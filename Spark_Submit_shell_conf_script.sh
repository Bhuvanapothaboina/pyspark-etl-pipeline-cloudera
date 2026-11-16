#!/bin/bash

# Load configurations
CONFIG_FILE="/home/cloudera/bhuvana_b20/config.conf"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Configuration file $CONFIG_FILE not found. Exiting."
  exit 1
fi

# Source the configuration file to load variables
source $CONFIG_FILE

# Validate required configurations
REQUIRED_VARS=("SPARK_MASTER" "DEPLOY_MODE" "DYNAMIC_ALLOCATION_ENABLED" "MIN_EXECUTORS" \
               "MAX_EXECUTORS" "INITIAL_EXECUTORS" "EXECUTOR_CORES" "EXECUTOR_MEMORY" \
               "PYSPARK_SCRIPT" "INPUT_PATH" "OUTPUT_PATH" "LOG_FILE")

for VAR in "${REQUIRED_VARS[@]}"; do
  if [ -z "${!VAR}" ]; then
    echo "Error: Missing configuration for $VAR in $CONFIG_FILE. Exiting."
    exit 1
  fi
done
# Run the spark-submit command
echo "Starting Spark job..."
spark-submit \
  --master $SPARK_MASTER \
  --deploy-mode $DEPLOY_MODE \
  --conf spark.dynamicAllocation.enabled=$DYNAMIC_ALLOCATION_ENABLED \
  --conf spark.dynamicAllocation.minExecutors=$MIN_EXECUTORS \
  --conf spark.dynamicAllocation.maxExecutors=$MAX_EXECUTORS \
  --conf spark.dynamicAllocation.initialExecutors=$INITIAL_EXECUTORS \
  --conf spark.executor.cores=$EXECUTOR_CORES \
  --conf spark.executor.memory=$EXECUTOR_MEMORY \
  $PYSPARK_SCRIPT $INPUT_PATH $OUTPUT_PATH > $LOG_FILE 2>&1
# Check if the spark-submit command was successful
if [ $? -eq 0 ]; then
  echo "Spark job completed successfully."
else
  echo "Spark job failed. Check the log file for details: $LOG_FILE"
  exit 1
fi