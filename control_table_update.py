from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

BASE                  = "s3://my-first-s3-bucket-05082005/delta/medallion/"
PIPELINE_CONTROL_PATH = BASE + "control/ba_pipeline_control/"

spark = (
    SparkSession.builder
    .appName("update_control_table")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# ✅ Safe targeted update — no overwrite
ctrl_table = DeltaTable.forPath(spark, PIPELINE_CONTROL_PATH)
ctrl_table.update(
    condition=col("PIPELINE_ID") == 1,
    set={
        "LOAD_TYPE":     lit("BACKFILL"),
        "BACKFILL_FROM": lit("2026-05-20").cast("date"),
        "BACKFILL_TO":   lit("2026-05-21").cast("date"),
    }
)

# Verify
spark.read.format("delta").load(PIPELINE_CONTROL_PATH) \
    .filter(col("PIPELINE_ID") == 1) \
    .select("PIPELINE_ID", "LOAD_TYPE", "BACKFILL_FROM", "BACKFILL_TO") \
    .show()
    
# ✅ Safe targeted update — no overwrite
ctrl_table = DeltaTable.forPath(spark, PIPELINE_CONTROL_PATH)
ctrl_table.update(
    condition=col("PIPELINE_ID") == 2,
    set={
        "LOAD_TYPE":     lit("BACKFILL"),
        "BACKFILL_FROM": lit("2026-05-20").cast("date"),
        "BACKFILL_TO":   lit("2026-05-21").cast("date"),
    }
)

# Verify
spark.read.format("delta").load(PIPELINE_CONTROL_PATH) \
    .filter(col("PIPELINE_ID") == 2) \
    .select("PIPELINE_ID", "LOAD_TYPE", "BACKFILL_FROM", "BACKFILL_TO") \
    .show()

spark.stop()