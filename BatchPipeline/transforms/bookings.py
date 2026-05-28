"""
transforms/bookings.py  —  Silver & Gold transforms for the bookings pipeline.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, to_date,
    current_timestamp, lit, row_number, desc,
    count, sum, avg, round, countDistinct,
    when, concat_ws,
)
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.sql.window import Window
import logging
from pyspark.storagelevel import StorageLevel

logger = logging.getLogger(__name__)

# ── Valid domain values ────────────────────────────────────────────────────────
VALID_SEAT_CLASSES     = ["Economy", "Business", "First"]
VALID_PAYMENT_STATUSES = ["Completed", "Pending", "Failed", "Refunded"]
VALID_CHANNELS         = ["Website", "MobileApp", "TravelAgent", "CallCenter"]


# ══════════════════════════════════════════════════════════════════════════════
# Silver Transform — Bookings
# ══════════════════════════════════════════════════════════════════════════════
def silver_transform_bookings(run_id: int, df: DataFrame) -> tuple:

    try:
        logger.info(f"Silver transform started | run_id={run_id} | table=bookings")

        # ── Step 1: Normalise op column ──────────────────────────────────────
        if "op" not in df.columns:
            df = df.withColumn("op", lit("I"))
        else:
            df = df.withColumn("op", trim(upper(col("op"))))

        # ── Step 2: Cast data types ──────────────────────────────────────────
        df = (
            df
            .withColumn("ticket_price",  col("ticket_price").cast(DoubleType()))
            .withColumn("flight_number", col("flight_number").cast(IntegerType()))
            .withColumn("booking_date",  to_date(col("booking_date"), "dd-MM-yyyy"))
            .withColumn("travel_date",   to_date(col("travel_date"),  "dd-MM-yyyy"))
        )

        # ── Step 3: Clean & standardise ─────────────────────────────────────
        df = (
            df
            .withColumn("booking_id",      trim(upper(col("booking_id"))))
            .withColumn("passenger_id",    trim(upper(col("passenger_id"))))
            .withColumn("seat_class",      trim(col("seat_class")))
            .withColumn("payment_status",  trim(col("payment_status")))
            .withColumn("booking_channel", trim(col("booking_channel")))
            .withColumn(
                "is_deleted",
                when(col("op") == "D", lit(True))
                .otherwise(lit(False))
            )
            .withColumn(
                "deleted_time",
                when(col("op") == "D", current_timestamp())
                .otherwise(lit(None).cast("timestamp"))
            )
            .withColumn("pipeline_name", lit("bookings"))
            .withColumn("run_id", lit(run_id))
            .withColumn("error_layer", lit("silver"))
        )

        # ── Step 4: Deduplicate ──────────────────────────────────────────────
        before_dedup = df.count()

        window = (
            Window.partitionBy(
                col("booking_id")
            )
            .orderBy(desc("ingestion_time"))
        )

        df = (
            df
            .withColumn("rn", row_number().over(window))
            .filter("rn = 1")
            .drop("rn")
        )
        
        #df = df.persist(StorageLevel.MEMORY_AND_DISK)

        after_dedup = df.count()

        duplicates = before_dedup - after_dedup

        #duplicates = before_dedup - df.count()

        logger.info(f"Duplicates removed | run_id={run_id} | count={duplicates}")

        # ── Step 5: Split — deletes bypass full validation ───────────────────
        delete_df = df.filter(col("op") == "D")

        upsert_df = df.filter(
            (col("op") != "D") | col("op").isNull()
        )

        # ── Validation reason columns ────────────────────────────────────────
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

                    when(
                        col("booking_id").isNull() |
                        (col("booking_id") == ""),
                        lit("Invalid booking_id")
                    ),

                    when(
                        col("passenger_id").isNull() |
                        (col("passenger_id") == ""),
                        lit("Invalid passenger_id")
                    ),

                    when(
                        col("flight_number").isNull() |
                        (col("flight_number") <= 0),
                        lit("Invalid flight_number")
                    ),

                    when(
                        col("booking_date").isNull(),
                        lit("Invalid booking_date")
                    ),

                    when(
                        col("travel_date").isNull(),
                        lit("Invalid travel_date")
                    ),

                    when(
                        col("ticket_price").isNull() |
                        (col("ticket_price") <= 0),
                        lit("Invalid ticket_price")
                    ),

                    when(
                        ~col("seat_class").isin(VALID_SEAT_CLASSES),
                        lit("Invalid seat_class")
                    ),

                    when(
                        ~col("payment_status").isin(VALID_PAYMENT_STATUSES),
                        lit("Invalid payment_status")
                    ),

                    when(
                        ~col("booking_channel").isin(VALID_CHANNELS),
                        lit("Invalid booking_channel")
                    ),

                    when(
                        col("travel_date") < col("booking_date"),
                        lit("travel_date cannot be before booking_date")
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
        
        # ── Good records ─────────────────────────────────────────────────────
        good_upsert = (
            validated_df
            .filter(col("reject_reason").isNull())
            .drop("reject_reason")
            .withColumn("silver_processed_time", current_timestamp())
        )

        # ── Bad records ──────────────────────────────────────────────────────
        bad_df = (
            validated_df
            .filter(col("reject_reason").isNotNull())
            .withColumn("batch_id", col("batch_id").cast(IntegerType()))
            .withColumn("bad_record_time", current_timestamp())
        )

        # ── Final good dataset ───────────────────────────────────────────────
        #good_df = good_upsert.union(
        #    delete_df.withColumn("  silver_processed_time", current_timestamp())
        #)  Balaji
        
        good_df = good_upsert.unionByName(
        delete_df.withColumn("silver_processed_time", current_timestamp()),
        allowMissingColumns=True
        )

        #good_df = good_df.persist(StorageLevel.MEMORY_AND_DISK)
        #bad_df  = bad_df.persist(StorageLevel.MEMORY_AND_DISK)

        good_count = good_df.count()
        bad_count  = bad_df.count()
        
        validated_df.unpersist()

        logger.info(
            f"Silver transform completed | run_id={run_id} | table=bookings | "
            f"good={good_count} | bad={bad_count} | duplicates={duplicates}"
        )
        
        good_df.unpersist()
        bad_df.unpersist()

        return good_df, bad_df

    except Exception as e:
        logger.error(f"Silver transform failed | run_id={run_id} | table=bookings")
        logger.exception(e)
        raise


# ══════════════════════════════════════════════════════════════════════════════
# Gold Transform — Bookings
# ══════════════════════════════════════════════════════════════════════════════
def gold_transform_bookings(run_id: int, incremental_df: DataFrame) -> list:

    try:
        logger.info(f"Gold transform started | run_id={run_id} | table=bookings")

        # Aggregate active records only
        active_df = incremental_df.filter(col("is_deleted") == False)

        # ── Gold 1: Revenue by seat class ────────────────────────────────────
        by_seat_class_df = (
            active_df
            .groupBy("seat_class")
            .agg(
                count("booking_id").alias("total_bookings"),
                round(sum("ticket_price"), 2).alias("total_revenue"),
                round(avg("ticket_price"), 2).alias("avg_ticket_price"),
            )
            .orderBy(desc("total_revenue"))
            .withColumn("gold_processed_time", current_timestamp())
        )

        # ── Gold 2: Bookings by payment status ──────────────────────────────
        by_payment_status_df = (
            active_df
            .groupBy("payment_status")
            .agg(
                count("booking_id").alias("total_bookings"),
                round(sum("ticket_price"), 2).alias("total_revenue"),
            )
            .orderBy(desc("total_bookings"))
            .withColumn("gold_processed_time", current_timestamp())
        )

        # ── Gold 3: Bookings by channel ─────────────────────────────────────
        by_channel_df = (
            active_df
            .groupBy("booking_channel")
            .agg(
                count("booking_id").alias("total_bookings"),
                round(sum("ticket_price"), 2).alias("total_revenue"),
                countDistinct("passenger_id").alias("unique_passengers"),
            )
            .orderBy(desc("total_bookings"))
            .withColumn("gold_processed_time", current_timestamp())
        )

        # ── Gold 4: Daily booking trend ─────────────────────────────────────
        daily_trend_df = (
            active_df
            .groupBy("booking_date")
            .agg(
                count("booking_id").alias("total_bookings"),
                round(sum("ticket_price"), 2).alias("total_revenue"),
            )
            .orderBy("booking_date")
            .withColumn("gold_processed_time", current_timestamp())
        )

        logger.info(f"Gold transform completed | run_id={run_id} | table=bookings")

        return [
                    (by_seat_class_df,     "SeatClass",     "seat_class"),
                    (by_payment_status_df, "PaymentStatus", "payment_status"),
                    (by_channel_df,        "Channel",       "booking_channel"),
                    (daily_trend_df,       "DailyTrend",    "booking_date"),
                ]

    except Exception as e:
        logger.error(f"Gold transform failed | run_id={run_id} | table=bookings")
        logger.exception(e)
        raise