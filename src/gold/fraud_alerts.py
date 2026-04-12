"""
Gold Layer — Fraud Alerts
Lakeflow Spark Declarative Pipelines
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dlt.table(
    name="fraud_alerts",
    comment="Fraud indicators per customer",
    table_properties={"quality": "gold"}
)
def fraud_alerts():

    transactions = dlt.read("transactions")
    accounts     = dlt.read("accounts")
    customers    = dlt.read("customers")

    # velocity check — nb transactions dans les 24h
    window_24h = (
        Window
        .partitionBy("account_id")
        .orderBy(F.col("transaction_date").cast("long"))
        .rangeBetween(-86400, 0)
    )

    tx_with_velocity = (
        transactions
        .withColumn("tx_count_24h",
            F.count("transaction_id").over(window_24h))
        .withColumn("velocity_alert",
            F.when(F.col("tx_count_24h") > 5, True)
             .otherwise(False))
    )

    fraud_by_account = (
        tx_with_velocity
        .groupBy("account_id")
        .agg(
            F.sum(F.when(F.col("is_flagged_fraud"), 1)
                   .otherwise(0)).alias("total_fraud_txns"),
            F.sum(F.when(F.col("velocity_alert"), 1)
                   .otherwise(0)).alias("velocity_alerts_count"),
            F.max("transaction_date").alias("last_fraud_date")
        )
    )

    return (
        fraud_by_account
        .join(accounts.select("account_id", "customer_id"),
              on="account_id", how="left")
        .join(customers.select("customer_id", "full_name", "kyc_status"),
              on="customer_id", how="left")
        .withColumn("alert_level",
            F.when(F.col("total_fraud_txns") > 3,      "critical")
             .when(F.col("total_fraud_txns") > 1,      "high")
             .when(F.col("velocity_alerts_count") > 3, "medium")
             .otherwise("low"))
        .withColumn("requires_investigation",
            F.when(F.col("alert_level").isin("critical", "high"), True)
             .otherwise(False))
        .select(
            "account_id",
            "customer_id",
            "full_name",
            "kyc_status",
            "total_fraud_txns",
            "velocity_alerts_count",
            "last_fraud_date",
            "alert_level",
            "requires_investigation",
            F.current_timestamp().alias("_updated_at")
        )
    )