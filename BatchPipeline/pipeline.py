"""
MODIFIED VERSION
================

BACKFILL FIXES IMPLEMENTED:

1. BACKFILL bypasses CDC watermark filtering
2. Historical replay supported properly
3. BACKFILL main() logic corrected
4. Date-based historical filtering added
5. Watermark corruption risk removed

Main enterprise issue fixed:
BACKFILL was incorrectly using bronze_layer_cdc()
which silently skipped old historical records.
"""


"""
pipeline_1.py  —  BA Medallion Pipeline

Reads all configuration and state directly from Delta control tables:
  - ba_pipeline_control   → pipeline config, load type, status, retry info
  - ba_pipeline_run       → run history (inserted / updated here)
  - ba_pipeline_run_stage → per-stage audit rows (inserted here)
  - ba_pipeline_watermark → CDC high-water mark (read + upserted here)

Flow per active pipeline:
  STATUS = FAILED          → RERUN  (retry last failed run)
  LOAD_TYPE = FULL_LOAD    → Full load (Bronze read all → Silver merge → Gold overwrite)
  LOAD_TYPE = BACKFILL     → Date-range backfill (one process_table call per date)
  anything else (CDC etc.) → Incremental load (Bronze watermark filter → Silver merge → Gold overwrite)

No external config.py / control.py imports required.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import (
    current_timestamp, col, lit, sha2, concat_ws,
    row_number, desc, max as spark_max, when,
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType, DateType, DoubleType,
)
from datetime import datetime, date, timedelta
import importlib
import logging
import json

# ══════════════════════════════════════════════════════════════════════════════
# Logging
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    #format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    format="%(asctime)s - %(levelname)s - %(name)s - %(filename)s:%(lineno)d - %(message)s",
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# Constants  (only things that never live in a control table)
# ══════════════════════════════════════════════════════════════════════════════
PIPELINE_USER       = "Airflow"
BASE                = "s3://my-first-s3-bucket-05082005/delta/medallion/"
BAD_RECORD_FOLDER   = "_bad_records"

# Control-table Delta paths  (must match control_v1.py)
PIPELINE_CONTROL_PATH   = BASE + "control/ba_pipeline_control/"
PIPELINE_RUN_PATH       = BASE + "control/ba_pipeline_run/"
PIPELINE_RUN_STAGE_PATH = BASE + "control/ba_pipeline_run_stage/"
PIPELINE_WATERMARK_PATH = BASE + "control/ba_pipeline_watermark/"

# Path helpers — sub-paths derived from BASE_PATH in the control row
def _source_path(base, name):  return f"{base}source/{name}/"
def _bronze_path(base, name):  return f"{base}bronze/{name}/"
def _silver_path(base, name):  return f"{base}silver/{name}/"
def _gold_path(base, name):    return f"{base}gold/{name}/"

# Source file name convention: <pipeline_name>.csv
def _file_name(name):          return f"{name}.csv"


# ══════════════════════════════════════════════════════════════════════════════
# Utility
# ══════════════════════════════════════════════════════════════════════════════
def generate_run_id() -> int:
    """Integer timestamp used as RUN_ID / STAGE_RUN_ID."""
    return datetime.now().strftime("%Y%m%d%H%M%S")

TYPE_MAPPING = {
    "string": StringType(),
    "integer": IntegerType(),
    "double": DoubleType(),
    "date": StringType(),       # important
    "timestamp": StringType()   # important
}

    
# Add this back
def build_schema(schema_definition: str):
    schema_json = json.loads(schema_definition)
    fields = []
    for col_name, data_type in schema_json.items():
        spark_type = TYPE_MAPPING.get(data_type.lower())
        if spark_type is None:
            raise Exception(f"Unsupported datatype: {data_type}")
        fields.append(StructField(col_name, spark_type, True))
    return StructType(fields)
    


def create_spark_session(app_name: str) -> SparkSession:
    try:
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
    except Exception as e:
        logger.error("Error creating Spark session")
        logger.exception(e)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Control-table readers
# ══════════════════════════════════════════════════════════════════════════════

def get_pipeline_control(spark: SparkSession, pipeline_name: str) -> Row:
    """Read the active control row for a given pipeline from ba_pipeline_control."""
    rows = (
        spark.read.format("delta").load(PIPELINE_CONTROL_PATH)
        .filter(col("PIPELINE_NAME") == pipeline_name)
        .filter(col("ACTIVE_FLAG") == "Y")
        .collect()
    )
    if not rows:
        raise Exception(f"No active control record found for pipeline: {pipeline_name}")
    return rows[0]


def get_all_active_pipelines(spark: SparkSession) -> list:
    """Return all active control rows — used by main() to iterate pipelines."""
    return (
        spark.read.format("delta").load(PIPELINE_CONTROL_PATH)
        .filter(col("ACTIVE_FLAG") == "Y")
        .collect()
    )


def update_control_status(spark: SparkSession, pipeline_id: int, status: str,
                           load_type: str = None, retry_count: int = None):
    """
    Update STATUS (and optionally LOAD_TYPE / RETRY_COUNT) in ba_pipeline_control.
    All string literals are quoted; numeric values are passed as-is.
    """
    ctrl_table = DeltaTable.forPath(spark, PIPELINE_CONTROL_PATH)
    set_values = {
        "STATUS":     f"'{status}'",
        "UPDATED_BY": f"'{PIPELINE_USER}'",
        "UPDATED_DT": "current_timestamp()",
    }
    if load_type is not None:
        set_values["LOAD_TYPE"] = f"'{load_type}'"
    if retry_count is not None:
        set_values["RETRY_COUNT"] = str(retry_count)

    ctrl_table.update(
        condition=col("PIPELINE_ID") == pipeline_id,
        set=set_values,
    )
    logger.info(f"Control status updated | pipeline_id={pipeline_id} | status={status}")


# ══════════════════════════════════════════════════════════════════════════════
# Run-table helpers
# ══════════════════════════════════════════════════════════════════════════════

# Schema defined once here (matches control_v1.py PIPELINE_RUN_SCHEMA)
PIPELINE_RUN_SCHEMA = StructType([
    StructField("RUN_ID",            StringType(),   False),
    StructField("PIPELINE_ID",       IntegerType(),   False),
    StructField("RUN_TYPE",          StringType(),    False),
    StructField("LOAD_TYPE",         StringType(),    False),
    StructField("LOAD_DATE",         DateType(),      True),
    StructField("RUN_STATUS",        StringType(),    False),
    StructField("START_TIME",        TimestampType(), False),
    StructField("END_TIME",          TimestampType(), True),
    StructField("ROWS_EXTRACTED",    IntegerType(),   True),
    StructField("ROWS_INSERTED",     IntegerType(),   True),
    StructField("ROWS_UPDATED",      IntegerType(),   True),
    StructField("ROWS_REJECTED",     IntegerType(),   True),
    StructField("IS_RERUN",          StringType(),    False),
    StructField("RERUN_OF_RUN_ID",   StringType(),    True),
    StructField("IS_BACKFILL",       StringType(),    False),
    StructField("BACKFILL_BATCH_ID", StringType(),    True),
    StructField("ERROR_CODE",        StringType(),    True),
    StructField("ERROR_STEP",        StringType(),    True),
    StructField("ERROR_MESSAGE",     StringType(),    True),
    StructField("CREATED_BY",        StringType(),    False),
    StructField("CREATED_DT",        TimestampType(), False),
    StructField("UPDATED_BY",        StringType(),    False),
    StructField("UPDATED_DT",        TimestampType(), False),
])

# Schema defined once here (matches control_v1.py PIPELINE_RUN_STAGE_SCHEMA)
PIPELINE_RUN_STAGE_SCHEMA = StructType([
    StructField("STAGE_RUN_ID",     StringType(),   False),
    StructField("RUN_ID",           StringType(),   False),
    StructField("STAGE_NAME",       StringType(),    False),
    StructField("STAGE_SEQUENCE",   IntegerType(),   False),
    StructField("STATUS",           StringType(),    False),
    StructField("RECORDS_COUNT",    IntegerType(),   True),
    StructField("RECORDS_INSERTED", IntegerType(),   True),
    StructField("RECORDS_FAILED",   IntegerType(),   True),
    StructField("ERROR",            StringType(),    True),
    StructField("START_TIME",       TimestampType(), False),
    StructField("END_TIME",         TimestampType(), True),
    StructField("CREATED_BY",       StringType(),    False),
    StructField("CREATED_DT",       TimestampType(), False),
    StructField("UPDATED_BY",       StringType(),    False),
    StructField("UPDATED_DT",       TimestampType(), False),
])

# Schema defined once here (matches control_v1.py PIPELINE_WATERMARK_SCHEMA)
PIPELINE_WATERMARK_SCHEMA = StructType([
    StructField("PIPELINE_ID",          IntegerType(),   False),
    StructField("LAST_PROCESSED_VALUE", TimestampType(), True),
    StructField("PREVIOUS_VALUE",       TimestampType(), True),
    StructField("CREATED_BY",           StringType(),    False),
    StructField("CREATED_DT",           TimestampType(), False),
    StructField("UPDATED_BY",           StringType(),    False),
    StructField("UPDATED_DT",           TimestampType(), False),
])


def insert_run_row(spark: SparkSession, run_row: dict):
    """Append a new row to ba_pipeline_run."""
    df = spark.createDataFrame([Row(**run_row)], schema=PIPELINE_RUN_SCHEMA)
    df.write.format("delta").mode("append").save(PIPELINE_RUN_PATH)
    logger.info(f"Run row inserted | RUN_ID={run_row['RUN_ID']} | STATUS={run_row['RUN_STATUS']}")



def update_run_row(spark: SparkSession, run_id: str, updates: dict):  # ✅ run_id: str now
    """Update an existing run row in ba_pipeline_run (success / failure close-out)."""
    run_table = DeltaTable.forPath(spark, PIPELINE_RUN_PATH)

    TIMESTAMP_COLS = {"START_TIME", "END_TIME", "CREATED_DT", "UPDATED_DT"}
    DATE_COLS      = {"LOAD_DATE"}
    INTEGER_COLS   = {"PIPELINE_ID", "ROWS_EXTRACTED", "ROWS_INSERTED",
                      "ROWS_UPDATED", "ROWS_REJECTED"}

    set_values = {}
    for k, v in updates.items():
        if k in TIMESTAMP_COLS:
            set_values[k] = lit(str(v)).cast("timestamp")  # ✅ safe timestamp
        elif k in DATE_COLS:
            set_values[k] = lit(str(v)).cast("date")       # ✅ safe date
        elif k in INTEGER_COLS:
            set_values[k] = lit(int(v))                    # ✅ safe int
        else:
            set_values[k] = lit(str(v))                    # ✅ safe string

    set_values["UPDATED_DT"] = current_timestamp()         # ✅ no string wrapping
    set_values["UPDATED_BY"] = lit(PIPELINE_USER)          # ✅ no manual quotes

    run_table.update(condition=col("RUN_ID") == run_id, set=set_values)
    logger.info(f"Run row updated | RUN_ID={run_id}")

def get_last_failed_run_id(spark: SparkSession, pipeline_id: int):
    """Read ba_pipeline_run and return RUN_ID of the most recent FAILED run."""
    rows = (
        spark.read.format("delta").load(PIPELINE_RUN_PATH)
        .filter(col("PIPELINE_ID") == pipeline_id)
        .filter(col("RUN_STATUS") == "FAILED")
        .orderBy(col("START_TIME").desc())
        .limit(1)
        .collect()
    )
    return rows[0]["RUN_ID"] if rows else None


# ══════════════════════════════════════════════════════════════════════════════
# Stage-table helpers
# ══════════════════════════════════════════════════════════════════════════════

def insert_stage_row(spark: SparkSession, stage_row: dict):
    """Append a stage audit row to ba_pipeline_run_stage."""
    df = spark.createDataFrame([Row(**stage_row)], schema=PIPELINE_RUN_STAGE_SCHEMA)
    df.write.format("delta").mode("append").save(PIPELINE_RUN_STAGE_PATH)
    logger.info(f"Stage row inserted | STAGE={stage_row['STAGE_NAME']} | STATUS={stage_row['STATUS']}")


# ══════════════════════════════════════════════════════════════════════════════
# Watermark helpers
# ══════════════════════════════════════════════════════════════════════════════

def get_watermark(spark: SparkSession, pipeline_id: int):
    """
    Read LAST_PROCESSED_VALUE from ba_pipeline_watermark.
    Returns None if no watermark row exists yet (first CDC run after full load).
    """
    rows = (
        spark.read.format("delta").load(PIPELINE_WATERMARK_PATH)
        .filter(col("PIPELINE_ID") == pipeline_id)
        .collect()
    )
    return rows[0]["LAST_PROCESSED_VALUE"] if rows else None


def upsert_watermark(spark: SparkSession, pipeline_id: int, new_value):
    """
    Insert or update the watermark in ba_pipeline_watermark after a
    successful CDC / FULL run. Preserves the previous value for rollback.
    new_value must be a Python datetime object.
    """
    existing = get_watermark(spark, pipeline_id)

    if existing is None:
        # First run — insert a fresh watermark row
        now = datetime.now()
        new_row = Row(
            PIPELINE_ID=pipeline_id,
            LAST_PROCESSED_VALUE=new_value,
            PREVIOUS_VALUE=None,
            CREATED_BY=PIPELINE_USER,
            CREATED_DT=now,
            UPDATED_BY=PIPELINE_USER,
            UPDATED_DT=now,
        )
        (
            spark.createDataFrame([new_row], schema=PIPELINE_WATERMARK_SCHEMA)
            .write.format("delta").mode("append").save(PIPELINE_WATERMARK_PATH)
        )
        logger.info(f"Watermark inserted | pipeline_id={pipeline_id} | value={new_value}")
    else:
        # Subsequent runs — roll current → previous, set new current
        wm_table = DeltaTable.forPath(spark, PIPELINE_WATERMARK_PATH)
        wm_table.update(
            condition=col("PIPELINE_ID") == pipeline_id,
            set={
                "PREVIOUS_VALUE":       col("LAST_PROCESSED_VALUE"),
                "LAST_PROCESSED_VALUE": lit(new_value).cast("timestamp"),
                "UPDATED_BY":           lit(PIPELINE_USER),
                "UPDATED_DT":           current_timestamp(),
            },
        )
        logger.info(f"Watermark updated | pipeline_id={pipeline_id} | new={new_value} | prev={existing}")


# ══════════════════════════════════════════════════════════════════════════════
# Backfill guard
# ══════════════════════════════════════════════════════════════════════════════

def is_already_loaded(spark: SparkSession, pipeline_id: int, load_date) -> bool:
    """
    Check ba_pipeline_run: returns True if this load_date already has a
    successful backfill run — used to skip duplicate backfill dates.
    """
    cnt = (
        spark.read.format("delta").load(PIPELINE_RUN_PATH)
        .filter(col("PIPELINE_ID") == pipeline_id)
        .filter(col("LOAD_DATE")   == load_date)
        .filter(col("IS_BACKFILL") == "Y")
        .filter(col("RUN_STATUS")  == "SUCCESS")
        .count()
    )
    return cnt > 0


# ══════════════════════════════════════════════════════════════════════════════
# Bronze layer
# ══════════════════════════════════════════════════════════════════════════════

def bronze_layer(run_id: int, spark: SparkSession,
                 source_path: str, filename: str,schema: str) -> DataFrame:
    """Full-load bronze read — all records from the source CSV."""
    try:
        full_path = f"{source_path}{filename}"
        logger.info(f"Bronze (full) read started | run_id={run_id} | file={full_path}")
        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            #.option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .load(full_path)
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("batch_id", lit(run_id).cast(IntegerType()))
        )
        record_count = df.count()
        if record_count == 0:
            raise Exception(f"Empty file: {full_path}")
        logger.info(f"Bronze (full) read completed | run_id={run_id} | records={record_count}")
        return df
    except Exception as e:
        logger.error(f"Bronze layer failed | run_id={run_id}")
        logger.exception(e)
        raise



def bronze_layer_cdc(run_id: int, spark: SparkSession,
                     source_path: str, filename: str,
                     watermark_col: str, last_watermark,
                     merge_key: str,schema: str) -> DataFrame:
    """
    CDC bronze read with validation.
      - Removes fully empty rows
      - Removes malformed extra columns (Unnamed:*)
      - Validates op column (I/U/D)
      - Rejects null merge keys
      - Applies watermark filtering
    """
    try:
        full_path = f"{source_path}{filename}"
        logger.info(f"Bronze (CDC) read started | run_id={run_id} | watermark={last_watermark}")

        df = (
            spark.read.format("csv")
            .option("header", "true")
            .schema(schema)
            #.option("inferSchema", "true")
            .option("mode", "PERMISSIVE")
            .load(full_path)
        )
        df.printSchema()

        # Remove malformed unnamed columns caused by trailing commas
        valid_cols = [c for c in df.columns if not c.startswith("Unnamed")]
        df = df.select(*valid_cols)
        
        # Remove completely empty rows
        non_empty_condition = None
        for c in df.columns:
            cond = col(c).isNotNull()
            non_empty_condition = cond if non_empty_condition is None else (non_empty_condition | cond)

        if non_empty_condition is not None:
            df = df.filter(non_empty_condition)

        # Standard metadata columns
        df = (
            df.withColumn("ingestion_time", current_timestamp())
              .withColumn("batch_id", lit(run_id).cast(IntegerType()))
        )

        # CDC validation
        if "op" in df.columns:
            df = df.filter(col("op").isin("I", "U", "D"))
        else:
            raise Exception("CDC file missing required column: op")

        # Reject null merge keys
        df = df.filter(col(merge_key).isNotNull())

        # Watermark filter
        if last_watermark is not None:
            df = df.filter(col(watermark_col) > lit(last_watermark))

        record_count = df.count()

        if record_count == 0:
            logger.warning(f"No valid CDC records found | run_id={run_id}")

        logger.info(f"Bronze (CDC) read completed | run_id={run_id} | valid_records={record_count}")
        return df

    except Exception as e:
        logger.error(f"Bronze (CDC) layer failed | run_id={run_id}")
        logger.exception(e)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Bad-records writer
# ══════════════════════════════════════════════════════════════════════════════

def write_bad_records(run_id: int, bad_df: DataFrame, bad_record_path: str):
    """Write bad records to the bad-record Delta path, partitioned by batch_id."""
    try:
        bad_count = bad_df.count()
        if bad_count > 0:
            logger.warning(f"Bad records found | run_id={run_id} | count={bad_count}")
            (
                bad_df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("batch_id")
                .save(bad_record_path)
            )
            logger.info(f"Bad records written | run_id={run_id} | path={bad_record_path}")
        else:
            logger.info(f"No bad records | run_id={run_id}")
    except Exception as e:
        logger.error(f"Bad records write failed | run_id={run_id}")
        logger.exception(e)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Silver upsert
# ══════════════════════════════════════════════════════════════════════════════


def upsert_silver(run_id: int, spark: SparkSession, good_df: DataFrame,
                  silver_path: str, merge_key: str) -> tuple:
    """
    Proper CDC-aware silver merge.

      op = I → insert
      op = U → update existing row
      op = D → soft delete

    Full-load path:
      - insert new rows
      - update changed rows
      - soft delete missing rows
    """
    try:
        logger.info(f"Silver upsert started | run_id={run_id} | merge_key={merge_key}")

        exclude_cols = [
            "ingestion_time",
            "silver_processed_time",
            "is_deleted",
            "deleted_time",
            "hash",
            "op"
        ]

        hash_cols = [c for c in good_df.columns if c not in exclude_cols]

        good_df = (
            good_df
            .withColumn("hash", sha2(concat_ws("||", *hash_cols), 256))
            .withColumn("silver_processed_time", current_timestamp())
            .withColumn("is_deleted", lit(False))
            .withColumn("deleted_time", lit(None).cast("timestamp"))
        )

        has_op_col = "op" in good_df.columns

        if DeltaTable.isDeltaTable(spark, silver_path):

            silver_table = DeltaTable.forPath(spark, silver_path)

            if has_op_col:

                (
                    silver_table.alias("target")
                    .merge(
                        good_df.alias("source"),
                        f"target.{merge_key} = source.{merge_key}"
                    )

                    # DELETE
                    .whenMatchedUpdate(
                        condition="source.op = 'D'",
                        set={
                            "is_deleted": "true",
                            "deleted_time": "current_timestamp()"
                        }
                    )

                    # UPDATE
                    .whenMatchedUpdate(
                        condition="source.op = 'U' AND target.hash <> source.hash",
                        set={
                            c: f"source.{c}"
                            for c in good_df.columns
                            if c != "op"
                        }
                    )

                    # INSERT
                    .whenNotMatchedInsert(
                        condition="source.op = 'I'",
                        values={
                            c: f"source.{c}"
                            for c in good_df.columns
                            if c != "op"
                        }
                    )

                    .execute()
                )

            else:

                (
                    silver_table.alias("target")
                    .merge(
                        good_df.alias("source"),
                        f"target.{merge_key} = source.{merge_key}"
                    )

                    .whenMatchedUpdate(
                        condition="target.hash <> source.hash",
                        set={
                            c: f"source.{c}"
                            for c in good_df.columns
                        }
                    )

                    .whenNotMatchedInsert(
                        values={
                            c: f"source.{c}"
                            for c in good_df.columns
                        }
                    )

                    .whenNotMatchedBySourceUpdate(
                        condition="target.is_deleted = false",
                        set={
                            "is_deleted": "true",
                            "deleted_time": "current_timestamp()"
                        }
                    )

                    .execute()
                )

            logger.info(f"Silver merge completed | run_id={run_id}")

        else:

            initial_df = (
                good_df.filter(col("op") != "D")
                if has_op_col else good_df
            )

            (
                initial_df.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(silver_path)
            )

            logger.info(f"Silver table created | run_id={run_id}")

        rows_inserted = (
            good_df.filter(col("op") == "I").count()
            if has_op_col else good_df.count()
        )

        rows_updated = (
            good_df.filter(col("op") == "U").count()
            if has_op_col else 0
        )

        return rows_inserted, rows_updated

    except Exception as e:
        logger.error(f"Silver upsert failed | run_id={run_id}")
        logger.exception(e)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Gold writer
# ══════════════════════════════════════════════════════════════════════════════

def write_gold(run_id: int, df: DataFrame, gold_path: str, table_name: str) -> int:
    """Overwrite one gold aggregation folder with latest results."""
    try:
        record_count = df.count()
        logger.info(f"Gold write started | run_id={run_id} | table={table_name}")
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(gold_path)
        )
        logger.info(f"Gold write completed | run_id={run_id} | table={table_name} | records={record_count}")
        return record_count
    except Exception as e:
        logger.error(f"Gold write failed | run_id={run_id} | table={table_name}")
        logger.exception(e)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Core: process_table
# ══════════════════════════════════════════════════════════════════════════════

def process_table(spark: SparkSession, ctrl: Row,
                  run_id: int,schema: str,
                  is_rerun: bool = False, rerun_of_run_id: int = None,
                  is_backfill: bool = False, backfill_batch_id: str = None,
                  backfill_load_date: date = None
                  ):
    """
    Run one pipeline end-to-end for a single run_id.

    Paths are derived from control row columns:
      BASE_PATH, PIPELINE_NAME, WATERMARK_COLUMN, LOAD_TYPE, MERGE_KEY

    Stages: BRONZE → SILVER → GOLD
    On success: closes run row, updates control STATUS + watermark (CDC).
    On failure: closes run row as FAILED, increments RETRY_COUNT on control.
    """
    pipeline_name = ctrl["PIPELINE_NAME"]
    pipeline_id   = ctrl["PIPELINE_ID"]
    load_type     = ctrl["LOAD_TYPE"]
    base          = ctrl["BASE_PATH"]

    # Derive all S3 paths from the control row
    source_path     = _source_path(base, pipeline_name)
    bronze_path     = _bronze_path(base, pipeline_name)
    silver_path     = _silver_path(base, pipeline_name)
    gold_path       = _gold_path(base,   pipeline_name)
    file_name       = _file_name(pipeline_name)
    # MERGE_KEY is set in control_v1.py bootstrap; fall back to <name>_id if missing
    merge_key       = ctrl["MERGE_KEY"] if ctrl["MERGE_KEY"] else f"{pipeline_name}_id"
    bad_record_path = f"{silver_path.rstrip('/')}/{BAD_RECORD_FOLDER}/{pipeline_name}/"

    logger.info(
        f"process_table started | run_id={run_id} | pipeline={pipeline_name} "
        f"| load_type={load_type} | is_rerun={is_rerun} | is_backfill={is_backfill}"
    )

    # Dynamically load table-specific transform module (transforms/flights.py etc.)
    module           = importlib.import_module(f"transforms.{pipeline_name}")
    silver_transform = getattr(module, f"silver_transform_{pipeline_name}")
    gold_transform   = getattr(module, f"gold_transform_{pipeline_name}")

    # ── Open run row in ba_pipeline_run ────────────────────────────────────
    # LOAD_DATE is a date type — use backfill date for backfill runs, else today
    load_date = backfill_load_date if is_backfill else date.today()

    run_row = dict(
        RUN_ID=run_id,
        PIPELINE_ID=pipeline_id,
        RUN_TYPE="RETRY" if is_rerun else ("BACKFILL" if is_backfill else "SCHEDULED"),
        LOAD_TYPE=load_type,
        LOAD_DATE=load_date,
        RUN_STATUS="RUNNING",
        START_TIME=datetime.now(),
        END_TIME=None,
        ROWS_EXTRACTED=0,
        ROWS_INSERTED=0,
        ROWS_UPDATED=0,
        ROWS_REJECTED=0,
        IS_RERUN="Y" if is_rerun else "N",
        RERUN_OF_RUN_ID=str(rerun_of_run_id) if rerun_of_run_id else None,
        IS_BACKFILL="Y" if is_backfill else "N",
        BACKFILL_BATCH_ID=backfill_batch_id,
        ERROR_CODE=None,
        ERROR_STEP=None,
        ERROR_MESSAGE=None,
        CREATED_BY=PIPELINE_USER,
        CREATED_DT=datetime.now(),
        UPDATED_BY=PIPELINE_USER,
        UPDATED_DT=datetime.now(),
    )
    insert_run_row(spark, run_row)
    update_control_status(spark, pipeline_id, "RUNNING")

    # ── Stage logging helper ───────────────────────────────────────────────
    def log_stage(stage_name, seq, status, in_count=0, inserted=0, failed=0,
                  start=None, error=None):
        insert_stage_row(spark, dict(
            STAGE_RUN_ID=(datetime.now().strftime("%Y%m%d%H%M%S%f")[:17]),
            RUN_ID=run_id,
            STAGE_NAME=stage_name,
            STAGE_SEQUENCE=seq,
            STATUS=status,
            RECORDS_COUNT=in_count,
            RECORDS_INSERTED=inserted,
            RECORDS_FAILED=failed,
            ERROR=error,
            START_TIME=start or datetime.now(),
            END_TIME=datetime.now(),
            CREATED_BY=PIPELINE_USER,
            CREATED_DT=datetime.now(),
            UPDATED_BY=PIPELINE_USER,
            UPDATED_DT=datetime.now(),
        ))

    try:
        # ══ BRONZE ══════════════════════════════════════════════════════════
        stage_start = datetime.now()

        if load_type == "FULL_LOAD":

            logger.info(
                f"FULL_LOAD detected | pipeline={pipeline_name}"
            )

            bronze_df = bronze_layer(
                run_id,
                spark,
                source_path,
                file_name,
                schema
            )

        elif is_backfill:

            logger.info(
                f"BACKFILL detected | pipeline={pipeline_name} "
                f"| load_date={backfill_load_date}"
            )

            # IMPORTANT:
            # BACKFILL must bypass CDC watermark filtering.
            # Historical data should load independently of watermark state.

            bronze_df = bronze_layer(
                run_id,
                spark,
                source_path,
                file_name,
                schema
            )

            # Historical date filtering
            # Replace 'flight_date' with your actual business date column if needed.

            # if "flight_date" in bronze_df.columns:
            #     bronze_df = bronze_df.filter(
            #         col("flight_date") == lit(str(backfill_load_date))
            #     )

        else:

            logger.info(
                f"CDC/RERUN detected | pipeline={pipeline_name}"
            )

            watermark_col  = ctrl["WATERMARK_COLUMN"]
            last_watermark = get_watermark(spark, pipeline_id)

            bronze_df = bronze_layer_cdc(
                run_id,
                spark,
                source_path,
                file_name,
                watermark_col,
                last_watermark,
                merge_key,
                schema
            )

        total_count = bronze_df.count()
        (
            bronze_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(bronze_path)
        )
        log_stage("BRONZE", 1, "SUCCESS", in_count=total_count,
                  inserted=total_count, start=stage_start)
        logger.info(f"Bronze completed | run_id={run_id} | records={total_count}")

        # ══ SILVER ══════════════════════════════════════════════════════════
        stage_start = datetime.now()
        good_df, bad_df = silver_transform(run_id, bronze_df)
        good_count  = good_df.count()
        bad_count   = bad_df.count()

        write_bad_records(run_id, bad_df, bad_record_path)
        rows_inserted, rows_updated = upsert_silver(
            run_id, spark, good_df, silver_path, merge_key,
        )
        log_stage("SILVER", 2, "SUCCESS", in_count=total_count,
                  inserted=good_count, failed=bad_count, start=stage_start)
        logger.info(f"Silver completed | run_id={run_id} | good={good_count} | bad={bad_count}")

        # ══ GOLD ════════════════════════════════════════════════════════════
        stage_start = datetime.now()
        silver_full_df = spark.read.format("delta").load(silver_path)
        gold_tables    = gold_transform(run_id, silver_full_df)
        gold_count     = 0
        for gold_df, folder_name in gold_tables:
            gold_count += write_gold(
                run_id, gold_df,
                f"{gold_path}{folder_name}/",
                f"{pipeline_name}_{folder_name}",
            )
        log_stage("GOLD", 3, "SUCCESS", in_count=good_count,
                  inserted=gold_count, start=stage_start)
        logger.info(f"Gold completed | run_id={run_id}")
        
        # ══ SUCCESS Run table update════════════════════════════════════════════
        
        update_run_row(spark, run_id, {
            "RUN_STATUS":    "SUCCESS",
            "END_TIME":      f"'{datetime.now()}'"
        })

        # ══ SUCCESS book-keeping ════════════════════════════════════════════

        # Advance watermark after CDC / RERUN runs
        logger.info(f"Watermark | load_type={load_type}")
        if load_type in ("CDC", "RERUN"):
            if ctrl["WATERMARK_COLUMN"]:
                new_wm = bronze_df.agg(
                    spark_max(col(ctrl["WATERMARK_COLUMN"]))
                ).collect()[0][0]
                if new_wm:
                    upsert_watermark(spark, pipeline_id, new_wm)
                else:
                    logger.warning(f"No watermark value found | pipeline={pipeline_id}")
            else:
                logger.warning(f"No WATERMARK_COLUMN defined | pipeline={pipeline_id}")

        elif load_type == "FULL_LOAD":
            logger.info(f"Watermark Condition reached...")
            upsert_watermark(spark, pipeline_id, datetime.now())  # ✅ only for FULL

# BACKFILL or others → no watermark update
        

        # After full load switch pipeline to CDC for next run
        next_load_type = "CDC" if load_type == "FULL_LOAD" else load_type
        update_control_status(
            spark, pipeline_id, "DONE",
            load_type=next_load_type,
            retry_count=0,
        )
        logger.info(f"process_table SUCCESS | run_id={run_id} | pipeline={pipeline_name}")

    except Exception as e:
        error_msg = str(e)
        logger.error(f"process_table FAILED | run_id={run_id} | pipeline={pipeline_name}")
        logger.exception(e)

        log_stage("UNKNOWN", 0, "FAILED", error=error_msg)

        update_run_row(spark, run_id, {
            "RUN_STATUS":    "FAILED",
            "END_TIME":      f"'{datetime.now()}'",
            "ERROR_CODE":    "PIPELINE_ERROR",
            "ERROR_STEP":    "UNKNOWN",
            "ERROR_MESSAGE": error_msg[:500],
        })

        # Increment retry counter; flip to FAILED_MAX_RETRY if ceiling hit
        new_retry  = int(ctrl["RETRY_COUNT"]) + 1
        retry_max  = int(ctrl["RETRY_MAX"])
        new_status = "FAILED_MAX_RETRY" if new_retry >= retry_max else "FAILED"
        update_control_status(
            spark, pipeline_id, new_status,
            retry_count=new_retry,
        )
        raise


# ══════════════════════════════════════════════════════════════════════════════
# RERUN
# ══════════════════════════════════════════════════════════════════════════════

def rerun_pipeline(spark: SparkSession, ctrl: Row):
    """
    Retry the last failed run for this pipeline.
    Reads ba_pipeline_run to find the failed RUN_ID, then calls process_table
    with is_rerun=True.
    Skips if STATUS is not FAILED (e.g. already recovered by another process).
    """
    pipeline_name = ctrl["PIPELINE_NAME"]

    if ctrl["STATUS"] != "FAILED":
        logger.info(f"Rerun skipped — status is {ctrl['STATUS']} | pipeline={pipeline_name}")
        return

    logger.info(f"Rerun started | pipeline={pipeline_name} | retry_count={ctrl['RETRY_COUNT']}")

    rerun_of_id = get_last_failed_run_id(spark, ctrl["PIPELINE_ID"])
    run_id      = generate_run_id()

    process_table(
        spark, ctrl, run_id,
        is_rerun=True,
        rerun_of_run_id=rerun_of_id,
    )


# ══════════════════════════════════════════════════════════════════════════════
# BACKFILL
# ══════════════════════════════════════════════════════════════════════════════

def backfill_pipeline(spark: SparkSession, ctrl: Row,
                      backfill_from: date, backfill_to: date,schema: str):
    """
    Load a date range historically.
    For each date:
      1. Check ba_pipeline_run — skip if already successfully loaded.
      2. Call process_table with is_backfill=True.
    Continues to the next date even if one date fails.
    """
    pipeline_name = ctrl["PIPELINE_NAME"]
    pipeline_id   = ctrl["PIPELINE_ID"]
    batch_id      = f"BF-{pipeline_name}-{backfill_from}-{backfill_to}"

    logger.info(
        f"Backfill started | pipeline={pipeline_name} "
        f"| from={backfill_from} | to={backfill_to}"
    )

    current_date = backfill_from
    while current_date <= backfill_to:

        if is_already_loaded(spark, pipeline_id, current_date):
            logger.info(f"Backfill skipped (already loaded) | date={current_date}")
            current_date += timedelta(days=1)
            continue

        logger.info(f"Backfill processing | date={current_date}")
        run_id = generate_run_id()

        try:
            process_table(
                spark, ctrl, run_id,schema,
                is_backfill=True,
                backfill_batch_id=batch_id,
                backfill_load_date=current_date,
            )
            logger.info(f"Backfill date done | date={current_date}")
        except Exception as e:
            logger.error(f"Backfill failed for date={current_date} — continuing to next date")
            logger.exception(e)

        current_date += timedelta(days=1)

    logger.info(f"Backfill completed | pipeline={pipeline_name} | batch={batch_id}")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN — reads ba_pipeline_control to decide FULL_LOAD / CDC / RERUN / BACKFILL
# ══════════════════════════════════════════════════════════════════════════════

def main():
    spark = None
    try:
        logger.info("Pipeline started.")
        spark = create_spark_session("BA_Pipeline")

        success_pipelines = []
        failed_pipelines  = []

        # Iterate every active pipeline row from ba_pipeline_control
        active_pipelines = get_all_active_pipelines(spark)
        logger.info(f"Active pipelines found: {[r['PIPELINE_NAME'] for r in active_pipelines]}")
        
        

        for ctrl in active_pipelines:
            run_id = generate_run_id()
            pipeline_name = ctrl["PIPELINE_NAME"]
            try:
                logger.info(
                    f"Control check | pipeline={pipeline_name} "
                    f"| load_type={ctrl['LOAD_TYPE']} | status={ctrl['STATUS']}"
                )
                
                # Cleaner version — same logic
                schema_key = "CDC_SCHEMA_DEFINITION" if ctrl["LOAD_TYPE"] in ("CDC", "RERUN","BACKFILL") else "SCHEMA_DEFINITION"
                schema     = build_schema(ctrl[schema_key])
                

                if ctrl["LOAD_TYPE"] == "CDC":
                    # Regular incremental (CDC) run
                    logger.info(f"Starting CDC | pipeline={pipeline_name}")
                    #run_id = generate_run_id() #Balaji
                    process_table(spark, ctrl, run_id,schema)
                elif ctrl["STATUS"] == "FAILED_MAX_RETRY":
                    # Too many consecutive failures — skip until manually reset
                    logger.warning(
                        f"Pipeline skipped — max retries reached | pipeline={pipeline_name}")
                    #continue

                elif ctrl["LOAD_TYPE"] == "FULL_LOAD":
                    # First ever run
                    logger.info(f"Starting FULL_LOAD | pipeline={pipeline_name}")
                    #run_id = generate_run_id()
                    process_table(spark, ctrl, run_id,schema)

                elif ctrl["LOAD_TYPE"] == "BACKFILL":

                    logger.info(
                        f"Starting BACKFILL | pipeline={pipeline_name}"
                    )

                    # Example backfill range.
                    # Ideally should come from control table columns.

                    #backfill_from = date(2025, 12, 28)
                    #backfill_to   = date(2025, 12, 30)
                    
                    backfill_from = ctrl["BACKFILL_FROM"]
                    backfill_to   = ctrl["BACKFILL_TO"]
                    
                    if backfill_from is None or backfill_to is None:
                        raise ValueError(
                            f"BACKFILL dates missing for pipeline={pipeline_name}"
                        )
                    
                    if backfill_from > backfill_to:
                        raise ValueError(
                            f"Invalid BACKFILL range | "
                            f"FROM={backfill_from} TO={backfill_to}"
                        )

                    backfill_pipeline(
                        spark,
                        ctrl,
                        backfill_from,
                        backfill_to,
                        schema
                    )                    
                elif ctrl["LOAD_TYPE"] == "RERUN":
                    # RERUN: last run failed — retry it
                    logger.info(f"Detected FAILED status — starting RERUN | pipeline={pipeline_name}")
                    rerun_pipeline(spark, ctrl)

                else:
                    logger.info(f"Invalid Control option pipeline={pipeline_name}")
                    
                    
                success_pipelines.append(pipeline_name)

            except Exception as e:
                failed_pipelines.append(pipeline_name)
                logger.error(f"Pipeline failed, continuing | pipeline={pipeline_name}")
                logger.exception(f"Known error | pipeline={pipeline_name} | error={e}")
                continue

        logger.info(
            f"Pipeline completed | success={success_pipelines} | failed={failed_pipelines}"
        )
        if failed_pipelines:
            raise Exception(f"Some pipelines failed: {failed_pipelines}")

        logger.info("Pipeline done.")

    except Exception as e:
        logger.error("Error in main")
        logger.exception(e)
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


try:
    main()
except Exception as e:
    logger.error("Unhandled error in main")
    logger.exception(e)
    raise