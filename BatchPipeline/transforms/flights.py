"""
transforms/flights.py  —  Silver & Gold transforms for the flights pipeline.

Called dynamically by pipeline_1.py:
    silver_transform_flights(run_id, bronze_df)  → (good_df, bad_df)
    gold_transform_flights(run_id, silver_df)    → [(df, folder_name), ...]

Design notes:
  - bronze_df already contains ingestion_time and batch_id (added by pipeline_1.py).
  - bad_df must retain batch_id so pipeline_1.py can partition bad records by it.
  - Deduplication key: (flight_date, flight_number) — keep latest ingestion_time.
  - Delete ops (op = 'D') bypass full validation and are always routed to good_df.
  - gold aggregations only use active (non-deleted) silver records.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, to_date,
    current_timestamp, lit, row_number, desc,
    count, sum, avg, round, when,concat_ws,
)
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
import logging
from pyspark.storagelevel import StorageLevel


logger = logging.getLogger(__name__)

# ── Valid domain values ────────────────────────────────────────────────────────
VALID_AIRLINES = ["BA", "AA", "UA", "DL", "EK", "QR", "SQ", "AF", "LH"]
VALID_AIRCRAFT = [
    "Boeing777", "Boeing787", "Boeing737",
    "AirbusA320", "AirbusA330", "AirbusA350", "AirbusA380",
]


# ══════════════════════════════════════════════════════════════════════════════
# Silver Transform — Flights
# ══════════════════════════════════════════════════════════════════════════════
def silver_transform_flights(run_id: int, df: DataFrame) -> tuple:
    """
    Silver transform for flights.

    Steps:
      1. Normalise op column (I / U / D) — default to 'I' for Full Load files
      2. Cast data types
      3. Clean & standardise string fields
      4. Deduplicate by (flight_date, flight_number) — keep latest ingestion_time
      5. Split into good / bad:
           - op = D  → always good (soft-delete, no further validation needed)
           - op = I/U → validate all business rules; failures go to bad_df

    Returns:
      good_df  — valid records (including deletes); has silver_processed_time
      bad_df   — invalid I/U records; retains batch_id for partitioned bad-record write
    """
    try:
        logger.info(f"Silver transform started | run_id={run_id} | table=flights")

        # ── Step 1: Normalise op column ──────────────────────────────────────
        if "op" not in df.columns:
            # Full Load — no op column in source file; default every row to Insert
            df = df.withColumn("op", lit("I"))
        else:
            df = df.withColumn("op", trim(upper(col("op"))))

        # ── Step 2: Cast data types ──────────────────────────────────────────
        df = (
            df
            .withColumn("flight_date",     to_date(col("flight_date"), "dd-MM-yyyy"))
            .withColumn("flight_number",   col("flight_number").cast(IntegerType()))
            .withColumn("departure_delay", col("departure_delay").cast(IntegerType()))
            .withColumn("arrival_delay",   col("arrival_delay").cast(IntegerType()))
            .withColumn("distance",        col("distance").cast(IntegerType()))
            .withColumn("cancelled",       col("cancelled").cast(IntegerType()))
            .withColumn("diverted",        col("diverted").cast(IntegerType()))
        )

        # ── Step 3: Clean & standardise ─────────────────────────────────────
        df = (
            df
            .withColumn("airline",             trim(upper(col("airline"))))
            .withColumn("origin_airport",      trim(upper(col("origin_airport"))))
            .withColumn("destination_airport", trim(upper(col("destination_airport"))))
            .withColumn("aircraft_type",       trim(col("aircraft_type")))
            .withColumn("scheduled_departure", trim(col("scheduled_departure")))
            .withColumn("actual_departure",    trim(col("actual_departure")))
            .withColumn("scheduled_arrival",   trim(col("scheduled_arrival")))
            .withColumn("actual_arrival",      trim(col("actual_arrival")))
            #.withColumn("is_deleted",          lit(False))
            .withColumn(
                "is_deleted",
                when(col("op") == "D", lit(True))
                .otherwise(lit(False))
            )
            #.withColumn("deleted_time",        lit(None).cast("timestamp"))
            .withColumn(
                "deleted_time",
                when(col("op") == "D", current_timestamp())
                .otherwise(lit(None).cast("timestamp"))
            )
            .withColumn("pipeline_name", lit("flights"))
            .withColumn("run_id", lit(run_id))
            .withColumn("error_layer", lit("silver"))
        )

        # ── Step 4: Deduplicate ──────────────────────────────────────────────
        # For duplicate (flight_date, flight_number) keep the row with the latest ingestion_time
        before_dedup = df.count()
        window = (
            #Window.partitionBy("flight_date", "flight_number")
            Window.partitionBy(
                col("flight_date"),
                col("flight_number")
            )
            .orderBy(desc("ingestion_time"))
        )
        df = (
            df
            .withColumn("rn", row_number().over(window))
            .filter("rn = 1")
            .drop("rn")
        )
        # Persist after expensive window operation
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        
        after_dedup = df.count()
        
        duplicates = before_dedup - after_dedup


        
        logger.info(f"Duplicates removed | run_id={run_id} | count={duplicates}")

        # ── Step 5: Split — deletes bypass full validation ───────────────────
        delete_df = df.filter(col("op") == "D")   # soft-delete rows — always valid
        #upsert_df = df.filter(col("op") != "D")   # I / U rows — need full validation
        upsert_df = df.filter(
            (col("op") != "D") | col("op").isNull()
        )

        # ── Validation reason columns ──────────────────────────────────────────────
        validated_df = (
            upsert_df
            .withColumn(
                "reject_reason",
                concat_ws(
                    "; ",

                    when(
                        ~col("op").isin(["I", "U", "D"]),
                        lit("Invalid operation type")
                    ),

                    when(col("flight_date").isNull(),
                         lit("Invalid flight_date")),

                    when(
                        col("flight_number").isNull() | (col("flight_number") <= 0),
                           lit("Invalid flight_number")
                    ),

                    when(
                        col("airline").isNull() |
                        ~col("airline").isin(VALID_AIRLINES),
                        lit("Invalid airline")
                    ),

                    when(
                        col("origin_airport").isNull() |
                        (col("origin_airport") == ""),
                        lit("Invalid origin_airport")
                    ),

                    when(
                        col("destination_airport").isNull() |
                        (col("destination_airport") == ""),
                        lit("Invalid destination_airport")
                    ),

                    when(
                        col("origin_airport") == col("destination_airport"),
                        lit("Origin and destination cannot be same")
                    ),

                    when(
                        col("distance").isNull() |
                        (col("distance") <= 0),
                        lit("Invalid distance")
                    ),

                    when(
                        ~col("aircraft_type").isin(VALID_AIRCRAFT),
                        lit("Invalid aircraft_type")
                    ),

                    when(
                        ~col("cancelled").isin([0, 1]),
                        lit("Invalid cancelled flag")
                    ),

                    when(
                        ~col("diverted").isin([0, 1]),
                        lit("Invalid diverted flag")
                    ),

                    when(
                        col("departure_delay").isNull() |
                        (col("departure_delay") < -120),
                        lit("Invalid departure_delay")
                    ),

                    when(
                        col("arrival_delay").isNull() |
                        (col("arrival_delay") < -120),
                        lit("Invalid arrival_delay")
                    )
                )
            )
        )

        validated_df = validated_df.withColumn(
            "reject_reason",
            when(
                col("reject_reason") == "",
                None
            ).otherwise(col("reject_reason"))
        )
        
        validated_df = validated_df.persist(StorageLevel.MEMORY_AND_DISK)

        

        # Good records
        good_upsert = (
            validated_df
            #.filter(col("reject_reason") == "")
            .filter(col("reject_reason").isNull())
            .drop("reject_reason")
            .withColumn("silver_processed_time", current_timestamp())
        )

        # Bad records
        bad_df = (
            validated_df
            #.filter(col("reject_reason") != "")
            .filter(col("reject_reason").isNotNull())
            .withColumn("batch_id", col("batch_id").cast(IntegerType()))
            .withColumn("bad_record_time", current_timestamp())
        )


        # Delete rows are always good — union with valid upsert rows
        good_df = good_upsert.union(
            delete_df.withColumn("silver_processed_time", current_timestamp())
        )

        # good_df = good_upsert.unionByName(
        #     delete_df.withColumn("silver_processed_time", current_timestamp())
        # )
        
            
        good_count = good_df.count()
        bad_count  = bad_df.count()
        
        validated_df.unpersist()

        logger.info(
        f"Silver transform completed | run_id={run_id} | table=flights | "
        f"good={good_count} | bad={bad_count} | duplicates={duplicates}"
        )
        
        good_df.unpersist() #Balaji
        bad_df.unpersist()
        return good_df, bad_df

    except Exception as e:
        logger.error(f"Silver transform failed | run_id={run_id} | table=flights")
        logger.exception(e)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Gold Transform — Flights
# ══════════════════════════════════════════════════════════════════════════════
def gold_transform_flights(run_id: int, incremental_df: DataFrame) -> list:
    """
    Gold aggregations for flights.

    Input  : full silver Delta table (already loaded by pipeline_1.py)
    Output : list of (df, folder_name) tuples — pipeline_1.py writes each one
             to  <gold_path>/<folder_name>/

    Aggregations:
      Airline      — on-time performance, cancellations, avg delay by airline
      Route        — flight counts, avg delay, avg distance by origin/destination
      DailySummary — daily totals: flights, cancellations, avg delay
      Aircraft     — utilisation: flights, avg distance, avg delay by aircraft type

    Only active (non-deleted) records feed into aggregations.
    """
    try:
        logger.info(f"Gold transform started | run_id={run_id} | table=flights")

        # Aggregate active records only
        active_df = incremental_df.filter(col("is_deleted") == False)

        # ── Gold 1: On-time performance by airline ───────────────────────────
        by_airline_df = (
            active_df
            .groupBy("airline")
            .agg(
                count("flight_number").alias("total_flights"),
                sum(when(col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),
                sum(when(col("diverted")  == 1, 1).otherwise(0)).alias("diverted_flights"),
                round(avg("departure_delay"), 1).alias("avg_departure_delay"),
                round(avg("arrival_delay"),   1).alias("avg_arrival_delay"),
                sum(when(col("arrival_delay") <= 15, 1).otherwise(0)).alias("on_time_flights"),
            )
            .orderBy(desc("total_flights"))
            .withColumn("gold_processed_time", current_timestamp())
        )

        # ── Gold 2: Route performance ────────────────────────────────────────
        by_route_df = (
            active_df
            .groupBy("origin_airport", "destination_airport")
            .agg(
                count("flight_number").alias("total_flights"),
                round(avg("departure_delay"), 1).alias("avg_departure_delay"),
                round(avg("arrival_delay"),   1).alias("avg_arrival_delay"),
                round(avg("distance"),        1).alias("avg_distance"),
            )
            .orderBy(desc("total_flights"))
            .withColumn("gold_processed_time", current_timestamp())
        )

        # ── Gold 3: Daily flight summary ─────────────────────────────────────
        daily_summary_df = (
            active_df
            .groupBy("flight_date")
            .agg(
                count("flight_number").alias("total_flights"),
                sum(when(col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),
                round(avg("departure_delay"), 1).alias("avg_departure_delay"),
                round(avg("arrival_delay"),   1).alias("avg_arrival_delay"),
            )
            .orderBy("flight_date")
            .withColumn("gold_processed_time", current_timestamp())
        )

        # ── Gold 4: Aircraft utilisation ─────────────────────────────────────
        by_aircraft_df = (
            active_df
            .groupBy("aircraft_type")
            .agg(
                count("flight_number").alias("total_flights"),
                round(avg("distance"),      1).alias("avg_distance"),
                round(avg("arrival_delay"), 1).alias("avg_arrival_delay"),
                sum(when(col("cancelled") == 1, 1).otherwise(0)).alias("cancelled_flights"),
            )
            .orderBy(desc("total_flights"))
            .withColumn("gold_processed_time", current_timestamp())
        )

        logger.info(f"Gold transform completed | run_id={run_id} | table=flights")

        # folder_name must match the sub-folder convention used in pipeline_1.py:
        #   <gold_path>/<folder_name>/
        
        
        return [
                (by_airline_df,    "Airline",       "airline"),
                (by_route_df,      "Route",         "origin_airport"),   # composite — see note below
                (daily_summary_df, "DailySummary",  "flight_date"),
                (by_aircraft_df,   "Aircraft",      "aircraft_type"),
               ]

    except Exception as e:
        logger.error(f"Gold transform failed | run_id={run_id} | table=flights")
        logger.exception(e)
        raise
