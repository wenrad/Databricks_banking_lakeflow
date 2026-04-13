"""
Data Privacy — PII Masking & Row-Level Security
Unity Catalog policies

Applied on Silver tables:
- Column masking : email, phone, iban
- Row-Level Security : par nationalité
- Right to be forgotten : is_deleted flag
"""

MASKING_POLICIES = """

-- ══════════════════════════════
-- COLUMN MASKING FUNCTIONS
-- ══════════════════════════════

-- email : radhwen@gmail.com → r***@gmail.com
CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('data-engineers') THEN email
    ELSE CONCAT(LEFT(email, 1), '***@', SPLIT(email, '@')[1])
  END;

-- téléphone : +33612345678 → +336*****678
CREATE OR REPLACE FUNCTION mask_phone(phone STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('data-engineers') THEN phone
    ELSE CONCAT(LEFT(phone, 4), '*****', RIGHT(phone, 3))
  END;

-- iban : FR7630006000011234 → FR76***234
CREATE OR REPLACE FUNCTION mask_iban(iban STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_account_group_member('data-engineers') THEN iban
    ELSE CONCAT(LEFT(iban, 4), '***', RIGHT(iban, 3))
  END;

-- ══════════════════════════════
-- APPLY MASKING ON SILVER TABLES
-- ══════════════════════════════

ALTER TABLE banking_lakehouse_dev.silver.customers
  ALTER COLUMN email SET MASK mask_email;

ALTER TABLE banking_lakehouse_dev.silver.customers
  ALTER COLUMN phone SET MASK mask_phone;

ALTER TABLE banking_lakehouse_dev.silver.accounts
  ALTER COLUMN iban SET MASK mask_iban;

ALTER TABLE banking_lakehouse_dev.silver.transactions
  ALTER COLUMN counterpart_iban SET MASK mask_iban;

-- ══════════════════════════════
-- ROW-LEVEL SECURITY
-- ══════════════════════════════

-- un analyste FR voit seulement les clients FR
CREATE OR REPLACE FUNCTION rls_by_nationality(nationality STRING)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_account_group_member('data-engineers') THEN TRUE
    WHEN is_account_group_member('data-analysts-fr')
      THEN nationality = 'FR'
    WHEN is_account_group_member('data-analysts-be')
      THEN nationality = 'BE'
    ELSE FALSE
  END;

ALTER TABLE banking_lakehouse_dev.silver.customers
  SET ROW FILTER rls_by_nationality ON (nationality);

-- ══════════════════════════════
-- RIGHT TO BE FORGOTTEN (RGPD)
-- ══════════════════════════════

UPDATE banking_lakehouse_dev.silver.customers
SET
  full_name = 'ANONYMIZED',
  email     = CONCAT('deleted_', customer_id, '@anonymized.com'),
  phone     = '0000000000'
WHERE is_deleted = TRUE;

"""


def apply_masking_policies(spark):
    """
    à exécuter une seule fois après le premier pipeline run
    """
    statements = [
        s.strip()
        for s in MASKING_POLICIES.split(";")
        if s.strip()
    ]

    for i, stmt in enumerate(statements):
        print(f"[{i+1}/{len(statements)}] applying policy...")
        try:
            spark.sql(stmt)
            print(" ok")
        except Exception as e:
            print(f"{e}")