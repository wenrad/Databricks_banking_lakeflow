"""
Silver Layer — CDC with APPLY CHANGES
Handles customer updates and deletes (GDPR right to be forgotten)
"""

import dlt
from pyspark.sql import functions as F


# table cible qui va recevoir les changements
dlt.create_streaming_table(
    name="customers_cdc",
    comment="Customer data with CDC - handles updates and deletes",
    table_properties={
        "quality"    : "silver",
        "pii"        : "true"
    }
)

# APPLY CHANGES gère automatiquement :
# - INSERT  → nouvelle ligne
# - UPDATE  → mise à jour de la ligne existante
# - DELETE  → suppression (ou soft delete si is_deleted = TRUE)
dlt.apply_changes(
    target       = "customers_cdc",
    source       = "raw_customers",
    keys         = ["customer_id"],       # clé primaire
    sequence_by  = F.col("created_at"),   # ordre des changements
    apply_as_deletes = F.expr("is_deleted = true"),  # RGPD
    except_column_list = ["_source_file"] # colonnes à ignorer
)