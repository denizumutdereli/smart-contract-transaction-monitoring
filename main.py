import logging
import os
import time
from datetime import datetime

import influxdb_client
import requests
from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Load environment variables
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
ETHERSCAN_API_URL = os.getenv("ETHERSCAN_API_URL")
CONTRACT_ADDRESS = '0xdac17f958d2ee523a2206206994597c13d831ec7' # possible to get with flags. this is just for testing purposes
ACCOUNT_ADDRESS = '0x4e83362442b8d1bec281594cea3050c8eb01311c' # in case if we want to breakdown my specific address

# InfluxDB configuration
url = 'http://localhost:8086'
token = 'r6jh9OivRb-cml4g2LVLI3fcHC1eFc707bhWsnp8q-BGfsw-Cqt-EyvVrQHdFRxUDSqP6p2VdRvtAH4xJWNBGA=='
org = 'organization'
bucket = 'mybucket'

# Initialize InfluxDB client
client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

def write_test_point():
    point = influxdb_client.Point("test_measurement").field("test_field", 123)
    write_api.write(bucket=bucket, org=org, record=point)

def get_transactions(start_block, end_block):
    url = f"{ETHERSCAN_API_URL}?module=account&action=tokentx&contractaddress={CONTRACT_ADDRESS}&page=1&offset=100&startblock={start_block}&endblock={end_block}&sort=desc&apikey={ETHERSCAN_API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        transactions = response.json()['result']
        return transactions, transactions[-1]['blockNumber'] if transactions else start_block
    else:
        return [], start_block

def insert_transactions_to_influx(transactions):
    points = []
    for tx in transactions:
        try:
            value = int(tx['value']) / (10 ** int(tx['tokenDecimal']))
            timestamp = datetime.utcfromtimestamp(int(tx['timeStamp'])).isoformat() + "Z"

            point = influxdb_client.Point("ethereum_transactions") \
                .tag("contract", CONTRACT_ADDRESS) \
                .field("value", value) \
                .field("from", tx['from']) \
                .field("to", tx['to']) \
                .field("gas", int(tx['gas'])) \
                .field("gasPrice", int(tx['gasPrice'])) \
                .field("gasUsed", int(tx['gasUsed'])) \
                .field("tokenName", tx['tokenName']) \
                .field("tokenSymbol", tx['tokenSymbol']) \
                .time(timestamp)
            
            points.append(point)
        except Exception as e:
            logging.error(f"Error processing transaction: {e}")

    if points:
        try:
            for point in points:
                print(point.to_line_protocol())

            write_api.write(bucket=bucket, org=org, record=points)
            logging.info(f"Inserted {len(points)} points into InfluxDB.")
        except Exception as e:
            logging.error(f"Error writing to InfluxDB: {e}")
    else:
        logging.info("No valid data points to write.")


def main():
    last_block = '4730207'  # Initial block number to start from
    logging.info("Starting script...")
    while True:
        try:
            transactions, new_last_block = get_transactions(last_block, 'latest')
            if transactions:
                logging.info(f"Fetched {len(transactions)} transactions.")
                insert_transactions_to_influx(transactions)
                logging.info("Transactions inserted into InfluxDB.")
                last_block = new_last_block
            else:
                logging.info("No new transactions found.")
            time.sleep(20)  
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            time.sleep(20)  # Wait before retrying

if __name__ == "__main__":
    
    main()
    #write_test_point()