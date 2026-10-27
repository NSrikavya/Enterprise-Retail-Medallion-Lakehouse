import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

# Initialize the Kafka Producer connected to your local Docker container
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
)

TOPIC_NAME = 'retail-transactions'

print(
    f'🚀 Starting Retail Data Producer... Streaming events to Kafka topic:'
    f' "{TOPIC_NAME}"'
)

# Sample mock master data for a retail chain
stores = ['Store_NY_01', 'Store_LA_02', 'Store_CHI_03', 'Store_TX_04']
products = [
    {'item_id': 'P1001', 'name': 'Wireless Mouse', 'price': 25.99},
    {'item_id': 'P1002', 'name': 'Mechanical Keyboard', 'price': 79.99},
    {'item_id': 'P1003', 'name': 'HD Monitor', 'price': 189.50},
    {'item_id': 'P1004', 'name': 'USB-C Hub', 'price': 15.00},
    {'item_id': 'P1005', 'name': 'Ergonomic Chair', 'price': 249.00},
]

try:
  while True:
    # Randomly select a product and a quantity
    product = random.choice(products)
    quantity = random.randint(1, 3)

    # Build a simulated transaction payload
    transaction = {
        'transaction_id': f'TXN-{random.randint(100000, 999999)}',
        'store_id': random.choice(stores),
        'item_id': product['item_id'],
        'item_name': product['name'],
        'quantity': quantity,
        'unit_price': product['price'],
        'total_amount': round(quantity * product['price'], 2),
        'timestamp': datetime.utcnow().isoformat(),
    }

    # Send the event to Kafka
    producer.send(TOPIC_NAME, value=transaction)
    print(
        f"✅ Produced: {transaction['transaction_id']} | Store:"
        f" {transaction['store_id']} | Item: {transaction['item_name']} ($"
        f"{transaction['total_amount']})"
    )

    # Wait 2 seconds before sending the next transaction
    time.sleep(2)

except KeyboardInterrupt:
  print('\n🛑 Producer stopped safely by user.')
finally:
  producer.close()