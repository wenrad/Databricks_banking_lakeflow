"""
Unit Tests — Banking Data Generator
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from utils.data_generator import (
    generate_customers,
    generate_accounts,
    generate_transactions,
)


@pytest.fixture(scope="module")
def customers():
    return generate_customers(100)


@pytest.fixture(scope="module")
def accounts(customers):
    return generate_accounts(customers)


@pytest.fixture(scope="module")
def transactions(accounts):
    return generate_transactions(accounts)


class TestCustomers:

    def test_count(self, customers):
        assert len(customers) == 100

    def test_unique_ids(self, customers):
        ids = [c["customer_id"] for c in customers]
        assert len(ids) == len(set(ids))

    def test_no_null_required_fields(self, customers):
        for c in customers:
            assert c["customer_id"] is not None
            assert c["email"] is not None
            assert c["full_name"] is not None

    def test_valid_kyc_status(self, customers):
        valid = {"verified", "pending", "rejected"}
        for c in customers:
            assert c["kyc_status"] in valid

    def test_email_format(self, customers):
        for c in customers:
            assert "@" in c["email"]

    def test_is_deleted_false_by_default(self, customers):
        for c in customers:
            assert c["is_deleted"] is False


class TestAccounts:

    def test_count(self, accounts):
        assert len(accounts) >= 100

    def test_unique_ids(self, accounts):
        ids = [a["account_id"] for a in accounts]
        assert len(ids) == len(set(ids))

    def test_positive_balance(self, accounts):
        for a in accounts:
            assert a["balance"] >= 0

    def test_valid_currency(self, accounts):
        for a in accounts:
            assert a["currency"] == "EUR"

    def test_valid_account_type(self, accounts):
        valid = {"checking", "savings", "business"}
        for a in accounts:
            assert a["account_type"] in valid

    def test_customer_id_not_null(self, accounts):
        for a in accounts:
            assert a["customer_id"] is not None


class TestTransactions:

    def test_count(self, transactions):
        assert len(transactions) == 5000

    def test_unique_ids(self, transactions):
        ids = [t["transaction_id"] for t in transactions]
        assert len(ids) == len(set(ids))

    def test_positive_amount(self, transactions):
        for t in transactions:
            assert t["amount"] > 0

    def test_valid_currency(self, transactions):
        for t in transactions:
            assert t["currency"] == "EUR"

    def test_valid_channel(self, transactions):
        valid = {"mobile", "web", "atm", "branch"}
        for t in transactions:
            assert t["channel"] in valid

    def test_fraud_rate(self, transactions):
        fraud_count = sum(1 for t in transactions if t["is_flagged_fraud"])
        fraud_rate = fraud_count / len(transactions)
        assert 0.01 <= fraud_rate <= 0.05

    def test_fraud_has_reason(self, transactions):
        valid_reasons = {
            "unusual_amount",
            "unusual_location",
            "velocity_check"
        }
        for t in transactions:
            if t["is_flagged_fraud"]:
                assert t["fraud_reason"] in valid_reasons

    def test_non_fraud_no_reason(self, transactions):
        for t in transactions:
            if not t["is_flagged_fraud"]:
                assert t["fraud_reason"] is None

    def test_account_id_not_null(self, transactions):
        for t in transactions:
            assert t["account_id"] is not None