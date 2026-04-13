"""
Gold Layer — Customer 360
Lakeflow Spark Declarative Pipelines
"""

import dlt
from pyspark.sql import functions as F


@dlt.table(
    name="customer_360",
    comment="Unified customer view with account and transaction metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableDeletionVectors": "true"
    },
    cluster_by=["customer_id", "risk_score"]  # liquid clustering
)
def customer_360():

    customers    = dlt.read("customers")
    accounts     = dlt.read("accounts")
    transactions = dlt.read("transactions")

    # métriques par compte
    tx_metrics = (
        transactions
        .groupBy("account_id")
        .agg(
            F.count("transaction_id").alias("total_transactions"),
            F.sum("amount").alias("total_amount_spent"),
            F.avg("amount").alias("avg_transaction_amount"),
            F.max("transaction_date").alias("last_transaction_date"),
            F.sum(F.when(F.col("is_flagged_fraud"), 1)
                   .otherwise(0)).alias("fraud_count")
        )
    )

    # métriques par customer
    account_metrics = (
        accounts
        .join(tx_metrics, on="account_id", how="left")
        .groupBy("customer_id")
        .agg(
            F.count("account_id").alias("total_accounts"),
            F.sum("balance").alias("total_balance"),
            F.sum("total_transactions").alias("total_transactions"),
            F.sum("total_amount_spent").alias("total_amount_spent"),
            F.avg("avg_transaction_amount").alias("avg_transaction_amount"),
            F.max("last_transaction_date").alias("last_transaction_date"),
            F.sum("fraud_count").alias("total_fraud_count")
        )
    )

    # risk score
    account_metrics = (
        account_metrics
        .withColumn("risk_score",
            F.when(F.col("total_fraud_count") > 2,  "high")
             .when(F.col("total_fraud_count") == 1, "medium")
             .otherwise("low"))
    )

    return (
        customers
        .join(account_metrics, on="customer_id", how="left")
        .select(
            "customer_id",
            "full_name",
            "email",
            "kyc_status",
            "is_deleted",
            "total_accounts",
            "total_balance",
            "total_transactions",
            "total_amount_spent",
            "avg_transaction_amount",
            "last_transaction_date",
            "total_fraud_count",
            "risk_score",
            F.current_timestamp().alias("_updated_at")
        )
    )