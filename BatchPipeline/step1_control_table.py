"""
control_v1.py  —  BA Pipeline Control Tables

Defines:
  - Delta table schemas for all 4 control tables
  - S3 paths for control tables
  - Bootstrap function to create tables on first run (idempotent)
  - Spark session factory

Control tables:
  ba_pipeline_control      → one row per pipeline; holds config, load type, status
  ba_pipeline_run          → one row per pipeline run; holds run-level audit
  ba_pipeline_run_stage    → one row per stage per run; holds stage-level audit
  ba_pipeline_watermark    → one row per pipeline; holds CDC high-water mark
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, LongType,
    IntegerType, TimestampType, DateType,
)
from datetime import datetime
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# S3 Paths
# ══════════════════════════════════════════════════════════════════════════════
PIPELINE_USER         = "Airflow"
BASE                  = "s3://my-first-s3-bucket-05082005/delta/medallion/"
PIPELINE_CONTROL_PATH = BASE + "control/ba_pipeline_control/"
PIPELINE_RUN_PATH     = BASE + "control/ba_pipeline_run/"
PIPELINE_RUN_STAGE_PATH = BASE + "control/ba_pipeline_run_stage/"
PIPELINE_WATERMARK_PATH = BASE + "control/ba_pipeline_watermark/"


# ══════════════════════════════════════════════════════════════════════════════
# Schemas
# ══════════════════════════════════════════════════════════════════════════════

# ba_pipeline_control — one config row per pipeline
# NOTE: PIPELINE_ID is IntegerType to match pipeline_1.py usage
PIPELINE_CONTROL_SCHEMA = StructType([
    StructField("PIPELINE_ID",       IntegerType(),  False),  # surrogate key
    StructField("PIPELINE_NAME",     StringType(),   False),  # e.g. "flights"
    StructField("SOURCE_TYPE",       StringType(),   False),  # e.g. "S3"
    StructField("BASE_PATH",         StringType(),   False),  # S3 base folder
    StructField("LOAD_TYPE",         StringType(),   False),  # FULL_LOAD | CDC | BACKFILL | RERUN
    StructField("WATERMARK_COLUMN",  StringType(),   True),   # CDC timestamp column name
    StructField("MERGE_KEY",         StringType(),   True),   # delta merge key, e.g. "booking_id"
    StructField("SCHEDULE_CRON",     StringType(),   True),   # cron expression
    StructField("STATUS",            StringType(),   False),  # READY | RUNNING | DONE | FAILED | FAILED_MAX_RETRY
    StructField("ACTIVE_FLAG",       StringType(),   False),  # Y | N
    StructField("RETRY_COUNT",       IntegerType(),  False),  # current consecutive failure count
    StructField("RETRY_MAX",         IntegerType(),  False),  # max retries before FAILED_MAX_RETRY
    StructField("LOAD_DATE",         DateType(),     True),   # date of latest load (for audit)
    StructField("BACKFILL_FROM",     DateType(),     True),   # backfill start date (BACKFILL mode only)
    StructField("BACKFILL_TO",       DateType(),     True),   # backfill end date   (BACKFILL mode only)
    StructField("SCHEMA_DEFINITION", StringType(),   False),  # Schema Details FULLLOAD
    StructField("CDC_SCHEMA_DEFINITION", StringType(),   False),  # Schema Details CDC
    StructField("CREATED_BY",        StringType(),   False),
    StructField("CREATED_DT",        TimestampType(),False),
    StructField("UPDATED_BY",        StringType(),   False),
    StructField("UPDATED_DT",        TimestampType(),False),
])

# ba_pipeline_run — one row per pipeline run
PIPELINE_RUN_SCHEMA = StructType([
    StructField("RUN_ID",            StringType(),   False),  # timestamp-based integer
    StructField("PIPELINE_ID",       IntegerType(),   False),  # FK → ba_pipeline_control
    StructField("RUN_TYPE",          StringType(),    False),  # SCHEDULED | MANUAL | RETRY | BACKFILL
    StructField("LOAD_TYPE",         StringType(),    False),  # FULL_LOAD | CDC | BACKFILL | RERUN
    StructField("LOAD_DATE",         DateType(),      True),   # date of this load
    StructField("RUN_STATUS",        StringType(),    False),  # RUNNING | SUCCESS | FAILED
    StructField("START_TIME",        TimestampType(), False),
    StructField("END_TIME",          TimestampType(), True),
    StructField("ROWS_EXTRACTED",    IntegerType(),   True),
    StructField("ROWS_INSERTED",     IntegerType(),   True),
    StructField("ROWS_UPDATED",      IntegerType(),   True),
    StructField("ROWS_REJECTED",     IntegerType(),   True),
    StructField("IS_RERUN",          StringType(),    False),  # Y | N
    StructField("RERUN_OF_RUN_ID",   StringType(),    True),
    StructField("IS_BACKFILL",       StringType(),    False),  # Y | N
    StructField("BACKFILL_BATCH_ID", StringType(),    True),
    StructField("ERROR_CODE",        StringType(),    True),
    StructField("ERROR_STEP",        StringType(),    True),
    StructField("ERROR_MESSAGE",     StringType(),    True),   # added — matches pipeline_1.py
    StructField("CREATED_BY",        StringType(),    False),
    StructField("CREATED_DT",        TimestampType(), False),
    StructField("UPDATED_BY",        StringType(),    False),
    StructField("UPDATED_DT",        TimestampType(), False),
])

# ba_pipeline_run_stage — one row per stage per run
PIPELINE_RUN_STAGE_SCHEMA = StructType([
    StructField("STAGE_RUN_ID",      StringType(),   False),  # unique per stage invocation
    StructField("RUN_ID",            StringType(),   False),  # FK → ba_pipeline_run
    StructField("STAGE_NAME",        StringType(),    False),  # BRONZE | SILVER | GOLD
    StructField("STAGE_SEQUENCE",    IntegerType(),   False),  # 1 / 2 / 3
    StructField("STATUS",            StringType(),    False),  # SUCCESS | FAILED
    StructField("RECORDS_COUNT",     IntegerType(),   True),
    StructField("RECORDS_INSERTED",  IntegerType(),   True),
    StructField("RECORDS_FAILED",    IntegerType(),   True),
    StructField("ERROR",             StringType(),    True),
    StructField("START_TIME",        TimestampType(), False),
    StructField("END_TIME",          TimestampType(), True),
    StructField("CREATED_BY",        StringType(),    False),
    StructField("CREATED_DT",        TimestampType(), False),
    StructField("UPDATED_BY",        StringType(),    False),
    StructField("UPDATED_DT",        TimestampType(), False),
])

# ba_pipeline_watermark — one row per pipeline (CDC high-water mark)
PIPELINE_WATERMARK_SCHEMA = StructType([
    StructField("PIPELINE_ID",           IntegerType(),   False),  # FK → ba_pipeline_control
    StructField("LAST_PROCESSED_VALUE",  TimestampType(), True),   # latest ingested watermark
    StructField("PREVIOUS_VALUE",        TimestampType(), True),   # previous watermark (rollback)
    StructField("CREATED_BY",            StringType(),    False),
    StructField("CREATED_DT",            TimestampType(), False),
    StructField("UPDATED_BY",            StringType(),    False),
    StructField("UPDATED_DT",            TimestampType(), False),
])


# ══════════════════════════════════════════════════════════════════════════════
# Bootstrap seed rows  (one per pipeline)
# ══════════════════════════════════════════════════════════════════════════════
BOOTSTRAP_ROWS = [
    Row(
        PIPELINE_ID=1,
        PIPELINE_NAME="flights",
        SOURCE_TYPE="S3",
        BASE_PATH=BASE,
        LOAD_TYPE="FULL_LOAD",
        WATERMARK_COLUMN="ingestion_time",  # CDC watermark column in source CSV
        MERGE_KEY="flight_number",          # composite key handled inside transform
        SCHEDULE_CRON="0 2 * * *",
        STATUS="READY",
        ACTIVE_FLAG="Y",
        RETRY_COUNT=0,
        RETRY_MAX=3,
        LOAD_DATE=None,
        BACKFILL_FROM=None,
        BACKFILL_TO=None,
        SCHEMA_DEFINITION=json.dumps({
            "flight_date":"date",
            "airline":"string",   
            "flight_number":"integer",
            "origin_airport":"string",
            "destination_airport":"string",
            "scheduled_departure":"string",
            "actual_departure":"string",
            "departure_delay":"integer",
            "scheduled_arrival":"string",
            "actual_arrival":"string",
            "arrival_delay":"integer",
            "distance":"integer",
            "cancelled":"integer",
            "diverted":"integer",
            "aircraft_type":"string"
                                    }),
        CDC_SCHEMA_DEFINITION=json.dumps({
            "op": "string",
            "flight_date":"date",
            "airline":"string",   
            "flight_number":"integer",
            "origin_airport":"string",
            "destination_airport":"string",
            "scheduled_departure":"string",
            "actual_departure":"string",
            "departure_delay":"integer",
            "scheduled_arrival":"string",
            "actual_arrival":"string",
            "arrival_delay":"integer",
            "distance":"integer",
            "cancelled":"integer",
            "diverted":"integer",
            "aircraft_type":"string"
                                    }),        
        CREATED_BY=PIPELINE_USER,
        CREATED_DT=datetime.now(),
        UPDATED_BY=PIPELINE_USER,
        UPDATED_DT=datetime.now(),
    ),
    Row(
        PIPELINE_ID=2,
        PIPELINE_NAME="bookings",
        SOURCE_TYPE="S3",
        BASE_PATH=BASE,
        LOAD_TYPE="FULL_LOAD",
        WATERMARK_COLUMN="ingestion_time",  # CDC watermark column in source CSV
        MERGE_KEY="booking_id",
        SCHEDULE_CRON="0 3 * * *",
        STATUS="READY",
        ACTIVE_FLAG="Y",
        RETRY_COUNT=0,
        RETRY_MAX=3,
        LOAD_DATE=None,
        BACKFILL_FROM=None,
        BACKFILL_TO=None,
        SCHEMA_DEFINITION=json.dumps({
            "booking_id":"string",
            "passenger_id":"string",
            "flight_number":"integer",
            "booking_date":"string",
            "travel_date":"string",
            "ticket_price":"double",
            "seat_class":"string",
            "payment_status":"string",
            "booking_channel":"string"
                                    }),
        CDC_SCHEMA_DEFINITION=json.dumps({
            "op": "string",
            "booking_id":"string",
            "passenger_id":"string",
            "flight_number":"integer",
            "booking_date":"string",
            "travel_date":"string",
            "ticket_price":"double",
            "seat_class":"string",
            "payment_status":"string",
            "booking_channel":"string"
                                    }),
        CREATED_BY=PIPELINE_USER,
        CREATED_DT=datetime.now(),
        UPDATED_BY=PIPELINE_USER,
        UPDATED_DT=datetime.now(),
    ),
]


# ══════════════════════════════════════════════════════════════════════════════
# Bootstrap — creates all 4 control tables if they don't exist
# ══════════════════════════════════════════════════════════════════════════════
def bootstrap_control_tables(spark: SparkSession):
    """
    Create all 4 control Delta tables if they don't already exist.
    Idempotent — safe to call on every pipeline start.
    """

    # ── ba_pipeline_control ────────────────────────────────────────────────
    if not DeltaTable.isDeltaTable(spark, PIPELINE_CONTROL_PATH):
        logger.info("Bootstrapping ba_pipeline_control ...")
        (
            spark.createDataFrame(BOOTSTRAP_ROWS, schema=PIPELINE_CONTROL_SCHEMA)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(PIPELINE_CONTROL_PATH)
        )
        logger.info(f"ba_pipeline_control created | rows={len(BOOTSTRAP_ROWS)}")
    else:
        logger.info("ba_pipeline_control exists — skipping.")

    # ── ba_pipeline_run ────────────────────────────────────────────────────
    if not DeltaTable.isDeltaTable(spark, PIPELINE_RUN_PATH):
        logger.info("Bootstrapping ba_pipeline_run ...")
        (
            spark.createDataFrame([], schema=PIPELINE_RUN_SCHEMA)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(PIPELINE_RUN_PATH)
        )
        logger.info("ba_pipeline_run created (empty).")
    else:
        logger.info("ba_pipeline_run exists — skipping.")

    # ── ba_pipeline_run_stage ──────────────────────────────────────────────
    if not DeltaTable.isDeltaTable(spark, PIPELINE_RUN_STAGE_PATH):
        logger.info("Bootstrapping ba_pipeline_run_stage ...")
        (
            spark.createDataFrame([], schema=PIPELINE_RUN_STAGE_SCHEMA)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(PIPELINE_RUN_STAGE_PATH)
        )
        logger.info("ba_pipeline_run_stage created (empty).")
    else:
        logger.info("ba_pipeline_run_stage exists — skipping.")

    # ── ba_pipeline_watermark ──────────────────────────────────────────────
    if not DeltaTable.isDeltaTable(spark, PIPELINE_WATERMARK_PATH):
        logger.info("Bootstrapping ba_pipeline_watermark ...")
        (
            spark.createDataFrame([], schema=PIPELINE_WATERMARK_SCHEMA)
            .write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(PIPELINE_WATERMARK_PATH)
        )
        logger.info("ba_pipeline_watermark created (empty).")
    else:
        logger.info("ba_pipeline_watermark exists — skipping.")


# ══════════════════════════════════════════════════════════════════════════════
# Spark session factory
# ══════════════════════════════════════════════════════════════════════════════
def create_spark_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    logger.info("Spark session created.")
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# Main  (run once to initialise control tables on first deployment)
# ══════════════════════════════════════════════════════════════════════════════
def main():
    spark = None
    try:
        logger.info("BA Initial Setup started ...")
        spark = create_spark_session("BA_Initial_Setup")
        bootstrap_control_tables(spark)
        logger.info("BA Initial Setup completed successfully.")
    except Exception as e:
        logger.error("Fatal error in Initial Setup.")
        logger.exception(e)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


try:
    #Approach 1
    main()
except Exception as e:
    logger.error("Unhandled fatal error.")
    logger.exception(e)
    raise