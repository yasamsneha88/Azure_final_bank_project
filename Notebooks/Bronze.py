from pyspark.sql import functions as F
from pyspark.sql.types import *

raw_atm_path     = "wasbs://raw@bantorage.blob.core.windows.net/atm/"
raw_upi_path     = "wasbs://raw@bantorage.blob.core.windows.net/upi/"

bronze_atm_path  = "wasbs://bronze@bantorage.blob.core.windows.net/atm/"
bronze_upi_path  = "wasbs://bronze@bantorage.blob.core.windows.net/upi/"

BRONZE_DB        = "fraud_bronze"
ATM_BRONZE_TABLE = f"{BRONZE_DB}.atm_events"
UPI_BRONZE_TABLE = f"{BRONZE_DB}.upi_events"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
atm_schema = StructType([
    StructField("txn_id",       StringType(), True),
    StructField("card_no",      StringType(), True),
    StructField("atm_id",       StringType(), True),
    StructField("txn_ts",       StringType(), True),   # cast later
    StructField("amount",       StringType(), True),   # cast later
    StructField("currency",     StringType(), True),
    StructField("status",       StringType(), True),
])
upi_schema = StructType([
    StructField("txn_id",       StringType(), True),
    StructField("upi_id_from",  StringType(), True),
    StructField("upi_id_to",    StringType(), True),
    StructField("txn_ts",       StringType(), True),
    StructField("amount",       StringType(), True),
    StructField("status",       StringType(), True),
    StructField("channel",      StringType(), True),
])
def add_bronze_metadata(df, source_system: str):
    return (
        df.withColumn("ingestion_ts", F.current_timestamp())
          .withColumn("source_system", F.lit(source_system))
          .withColumn("source_file", F.input_file_name())
    )
def run_bronze_atm_batch():
    df_raw = (
        spark.read
             .schema(atm_schema)
             .option("header", "true")
             .csv(raw_atm_path)
    )

    df_bronze = (
        df_raw
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .transform(lambda d: add_bronze_metadata(d, "ATM"))
    )

    (
        df_bronze.write
                 .format("delta")
                 .mode("overwrite")    # change to "append" when you have multiple loads
                 .save(bronze_atm_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ATM_BRONZE_TABLE}
        USING DELTA
        LOCATION '{bronze_atm_path}'
    """)

    print(f"ATM bronze written to {bronze_atm_path} and table {ATM_BRONZE_TABLE}")


def run_bronze_upi_batch():
    df_raw = (
        spark.read
             .schema(upi_schema)
             .option("header", "true")
             .csv(raw_upi_path)
    )

    df_bronze = (
        df_raw
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("txn_ts", F.to_timestamp("txn_ts"))
        .transform(lambda d: add_bronze_metadata(d, "UPI"))
    )

    (
        df_bronze.write
                 .format("delta")
                 .mode("overwrite")
                 .save(bronze_upi_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {UPI_BRONZE_TABLE}
        USING DELTA
        LOCATION '{bronze_upi_path}'
    """)

    print(f"UPI bronze written to {bronze_upi_path} and table {UPI_BRONZE_TABLE}")
