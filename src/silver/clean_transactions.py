"""
Silver Layer — Cleaning & Validation
Lakeflow Spark Declarative Pipelines
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="customers",
    comment="Cleaned customers data",
    table_properties={"quality": "silver", "pii": "true"}
)
@dlt.expect_or_drop("valid kyc_status",
    "kyc_status IN ('verified', 'pending', 'rejected')"
)
def customers():
    return (
        dlt.read_stream("raw_customers")
           .withColumn("created_at", F.to_timestamp("created_at"))
           .withColumn("full_name", F.trim(F.upper("full_name")))
           .withColumn("email", F.trim(F.lower("email")))
           .dropDuplicates(["customer_id"])
           .select(
               "customer_id",
               "full_name",
               "email",
               "phone",
               "kyc_status",
               "is_deleted",
               "created_at",
               "_ingested_at"
           )
    )


@dlt.table(
    name="accounts",
    comment="Cleaned accounts data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("positive balance", "balance >= 0")
@dlt.expect_or_drop("valid currency", "currency = 'EUR'")
def accounts():
    return (
        dlt.read_stream("raw_accounts")
           .withColumn("opened_at", F.to_timestamp("opened_at"))
           .withColumn("balance", F.round("balance", 2))
           .withColumn("is_high_balance",
                F.when(F.col("balance") > 20000, True)
                 .otherwise(False))
           .dropDuplicates(["account_id"])
           .select(
               "account_id",
               "customer_id",
               "account_type",
               "balance",
               "is_high_balance",
               "currency",
               "status",
               "opened_at",
               "_ingested_at"
           )
    )


@dlt.table(
    name="transactions",
    comment="Cleaned transactions data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("positive amount", "amount > 0")
@dlt.expect_or_drop("valid currency", "currency = 'EUR'")
def transactions():
    return (
        dlt.read_stream("raw_transactions")
           .withColumn("transaction_date", F.to_timestamp("transaction_date"))
           .withColumn("amount", F.round("amount", 2))
           .withColumn("amount_category",
                F.when(F.col("amount") < 100,  "small")
                 .when(F.col("amount") < 1000, "medium")
                 .when(F.col("amount") < 5000, "large")
                 .otherwise("very_large"))
           .withColumn("is_suspicious",
                F.when(
                    (F.col("is_flagged_fraud") == True) |
                    (F.col("amount") > 8000), True
                ).otherwise(False))
           .dropDuplicates(["transaction_id"])
           .select(
               "transaction_id",
               "account_id",
               "transaction_type",
               "amount",
               "amount_category",
               "currency",
               "counterpart_iban",
               "transaction_date",
               "status",
               "is_flagged_fraud",
               "is_suspicious",
               "fraud_reason",
               "channel",
               "created_at",
               "_ingested_at"
           )
    )