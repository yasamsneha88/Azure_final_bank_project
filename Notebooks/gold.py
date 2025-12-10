from pyspark.sql import functions as F
gold_fact_path   = "wasbs://gold@bantorage.blob.core.windows.net/fact_transactions/"

SILVER_DB            = "fraud_silver"
UNIFIED_SILVER_TABLE = f"{SILVER_DB}.transactions_silver"

GOLD_DB         = "fraud_gold"
FACT_TABLE      = f"{GOLD_DB}.fact_transactions"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")
def build_fact_transactions():
    df = spark.table(UNIFIED_SILVER_TABLE)

    df_fact = (
        df.groupBy("txn_date", "txn_type", "primary_identifier")
          .agg(
              F.count("*").alias("txn_count"),
              F.sum("amount").alias("total_amount"),
              F.max("amount").alias("max_amount"),
              F.min("amount").alias("min_amount"),
          )
    )
    (
        df_fact.write
               .format("delta")
               .mode("overwrite")
               .partitionBy("txn_date", "txn_type")
               .save(gold_fact_path)
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FACT_TABLE}
        USING DELTA
        LOCATION '{gold_fact_path}'
    """)

    print(f"Gold fact written to {gold_fact_path} and table {FACT_TABLE}")
