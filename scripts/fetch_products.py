import requests
import json
from pathlib import Path
import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

url = "https://dummyjson.com/products?limit=100"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_FILE = PROJECT_ROOT/"data/products.json"

def main():
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()


        data = response.json()["products"]
        with open(OUTPUT_FILE, "w") as f:
            for p in data:
                f.write(json.dumps(p) + "\n")
        logging.info("save products to data/products.json")
    except requests.exceptions.RequestException as e:
        logging.info(f"API request failed: {e}")

if __name__ == "__main__":
    main()