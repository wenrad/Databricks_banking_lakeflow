"""
Bronze Layer — Raw Ingestion
Lakeflow Spark Declarative Pipelines
"""

import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog_name", "banking_lakehouse_dev")
base_path = f"/Volumes/{catalog}/raw_data/landing"

@dlt.table(
    name="raw_customers",
    comment="Raw customers data - no transformation",
    table_properties={"quality": "bronze"}
)
@dlt.expect("customer_id not null", "customer_id IS NOT NULL")
@dlt.expect("email not null", "email IS NOT NULL")
def raw_customers():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load(f"{base_path}/customers/")
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_source_file", F.input_file_name())
    )


@dlt.table(
    name="raw_accounts",
    comment="Raw accounts data - no transformation",
    table_properties={"quality": "bronze"}
)
@dlt.expect("account_id not null", "account_id IS NOT NULL")
@dlt.expect("customer_id not null", "customer_id IS NOT NULL")
def raw_accounts():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load(f"{base_path}/accounts/")
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_source_file", F.input_file_name())
    )


@dlt.table(
    name="raw_transactions",
    comment="Raw transactions data - no transformation",
    table_properties={"quality": "bronze"}
)
@dlt.expect_or_drop("transaction_id not null", "transaction_id IS NOT NULL")
@dlt.expect_or_drop("account_id not null", "account_id IS NOT NULL")
@dlt.expect("amount positive", "amount > 0")
def raw_transactions():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "json")
             .option("cloudFiles.inferColumnTypes", "true")
             .load(f"{base_path}/transactions/")
             .withColumn("_ingested_at", F.current_timestamp())
             .withColumn("_source_file", F.input_file_name())
    )