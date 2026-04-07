import json
import random
import csv
from datetime import datetime, timedelta
from pathlib import Path 

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

NUM_ROWS = 5000
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILE = PROJECT_ROOT/"data/products.json"

def random_date(start_date, end_date):
    delta = end_date - start_date
    return start_date + timedelta(days=random.randint(0, delta.days))

def generate_transaction():
    # load products
    with open(OUTPUT_FILE, "r") as f:
        products = json.load(f)["products"]

    product_ids = [p["id"] for p in products]
    product_prices = {p["id"]: p["price"] for p in products}
    logging.info(product_prices)
    start_date = datetime(2025, 10, 1)
    end_date = datetime(2026, 4, 1)

    

    rows = []

    for i in range(NUM_ROWS):
        pid = random.choice(product_ids)
        price = product_prices[pid] * random.uniform(0.9, 1.1)

        rows.append({
            "transaction_id": f"T{i+1:06}",
            "user_id": f"U{random.randint(1,1000):05}",
            "product_id": pid,
            "quantity": random.randint(1,5),
            "unit_price": round(price, 2),
            "transaction_date": random_date(start_date, end_date).strftime("%Y-%m-%d")
        })

    with open("data/transactions.csv", "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print("Generated transactions.csv")

if __name__ == "__main__":
    generate_transaction()