from faker import Faker
import random
import uuid
import json
from datetime import datetime, timedelta

fake = Faker("fr_FR")

def generate_customers(n):
    customers = []
    for _ in range(n):
        customer = {
            "customer_id": str(uuid.uuid4()),
            "full_name": fake.name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "kyc_status": random.choice(["verified", "pending", "rejected"]),
            "is_deleted": False,
            "created_at": fake.date_time_this_year().isoformat()
        }
        customers.append(customer)
    return customers

def generate_accounts(customers):
    accounts = []
    for customer in customers:
        account = {
            "account_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "account_type": random.choice(["checking", "savings", "business"]),
            "balance": round(random.uniform(0, 50000), 2),
            "currency": "EUR",
            "status": random.choice(["active", "active", "active", "frozen", "closed"]),
            "opened_at": fake.date_time_this_year().isoformat()
        }
        accounts.append(account)
    return accounts

def generate_transactions(accounts, n=5000):
    transactions = []
    account_ids = [a["account_id"] for a in accounts]
    
    for _ in range(n):
        is_fraud = random.random() < 0.03
        transaction = {
            "transaction_id": str(uuid.uuid4()),
            "account_id": random.choice(account_ids),
            "counterpart_iban": fake.iban(),
            "amount": round(random.uniform(500, 9999), 2) if is_fraud else round(random.uniform(1, 3000), 2),
            "currency": "EUR",
            "transaction_type": random.choice(["transfer", "payment", "withdrawal", "deposit"]),
            "status": random.choice(["completed", "completed", "pending", "failed"]),
            "is_flagged_fraud": is_fraud,
            "fraud_reason": random.choice(["unusual_amount", "unusual_location", "velocity_check"]) if is_fraud else None,
            "channel": random.choice(["mobile", "web", "atm", "branch"]),
            "transaction_date": fake.date_time_this_year().isoformat(),
            "created_at": datetime.now().isoformat()
        }
        transactions.append(transaction)
    return transactions

def write_json(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"{filepath} — {len(data)} records")

if __name__ == "__main__":
    customers    = generate_customers(500)
    accounts     = generate_accounts(customers)
    transactions = generate_transactions(accounts)

    write_json(customers,    "data/customers/customers.json")
    write_json(accounts,     "data/accounts/accounts.json")
    write_json(transactions, "data/transactions/transactions.json")