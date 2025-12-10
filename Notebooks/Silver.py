from pyspark.sql import functions as F
bronze_atm_path  = "wasbs://bronze@bantorage.blob.core.windows.net/atm/"
bronze_upi_path  = "wasbs://bronze@bantorage.blob.core.windows.net/upi/"
silver_atm_path  = "wasbs://silver@bantorage.blob.core.windows.net/atm/"
silver_upi_path  = "wasbs://silver@bantorage.blob.core.windows.net/upi/"
SILVER_DB            = "fraud_silver"
ATM_SILVER_TABLE     = f"{SILVER_DB}.atm_events_silver"
UPI_SILVER_TABLE     = f"{SILVER_DB}.upi_events_silver"
UNIFIED_SILVER_TABLE = f"{SILVER_DB}.transactions_silver"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")
def build_silver_atm():
    df_bronze = (
        spark.read
             .format("delta")
             .load(bronze_atm_path)
    )

    df_silver = (
        df_bronze
        .filter(F.col("txn_id").isNotNull())
        .filter(F.col("amount").isNotNull())
        .withColumn("status_norm", F.upper(F.trim(F.col("status"))))
        .withColumn("txn_date", F.to_date("txn_ts"))
    )

    (
        df_silver.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy("txn_date")
                 .save(silver_atm_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ATM_SILVER_TABLE}
        USING DELTA
        LOCATION '{silver_atm_path}'
    """)

    print(f"ATM silver written to {silver_atm_path} and table {ATM_SILVER_TABLE}")
def build_silver_upi():
    df_bronze = (
        spark.read
             .format("delta")
             .load(bronze_upi_path)
    )

    df_silver = (
        df_bronze
        .filter(F.col("txn_id").isNotNull())
        .filter(F.col("amount").isNotNull())
        .withColumn("status_norm", F.upper(F.trim(F.col("status"))))
        .withColumn("txn_date", F.to_date("txn_ts"))
    )

    (
        df_silver.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy("txn_date")
                 .save(silver_upi_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {UPI_SILVER_TABLE}
        USING DELTA
        LOCATION '{silver_upi_path}'
    """)

    print(f"UPI silver written to {silver_upi_path} and table {UPI_SILVER_TABLE}")
def build_unified_silver_transactions():
    df_atm = spark.table(ATM_SILVER_TABLE)
    df_upi = spark.table(UPI_SILVER_TABLE)

    df_atm_norm = (
        df_atm
        .select(
            F.col("txn_id").alias("txn_id"),
            F.col("txn_ts").alias("txn_ts"),
            F.col("txn_date").alias("txn_date"),
            F.col("amount").alias("amount"),
            F.col("currency").alias("currency"),
            F.col("status_norm").alias("status"),
            F.lit("ATM").alias("txn_type"),
            F.col("card_no").alias("primary_identifier"),
            F.col("atm_id").alias("channel_or_location"),
        )
    )

    df_upi_norm = (
        df_upi
        .select(
            F.col("txn_id").alias("txn_id"),
            F.col("txn_ts").alias("txn_ts"),
            F.col("txn_date").alias("txn_date"),
            F.col("amount").alias("amount"),
            F.lit(None).cast("string").alias("currency"),
            F.col("status_norm").alias("status"),
            F.lit("UPI").alias("txn_type"),
            F.col("upi_id_from").alias("primary_identifier"),
            F.col("channel").alias("channel_or_location"),
        )
    )

    df_unified = df_atm_norm.unionByName(df_upi_norm)

    unified_path = "wasbs://silver@bantorage.blob.core.windows.net/transactions/"

    (
        df_unified.write
                 .format("delta")
                 .mode("overwrite")
                 .partitionBy("txn_date", "txn_type")
                 .save(unified_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {UNIFIED_SILVER_TABLE}
        USING DELTA
        LOCATION '{unified_path}'
    """)

    print(f"Unified silver written to {unified_path} and table {UNIFIED_SILVER_TABLE}")