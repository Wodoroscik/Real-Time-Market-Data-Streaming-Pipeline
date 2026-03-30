import json
import time
import random
from confluent_kafka import Producer
import yfinance as yf

KAFKA_BROKER = '127.0.0.1:9092'
TOPIC = 'market_ticks'
TICKERS = ['AAPL', 'MSFT', 'GOOGL']

producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    """Async callback for producer delivery status."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered: {msg.value().decode('utf-8')} to partition {msg.partition()}")

print("Fetching baseline prices from yfinance...")
base_prices = {}
for ticker in TICKERS:
    stock = yf.Ticker(ticker)
    base_prices[ticker] = stock.history(period="1d")['Close'].iloc[-1]

print("Streaming started. Press Ctrl+C to stop.")

try:
    while True:
        for ticker in TICKERS:
            # Simulate market noise (+/- 0.1%)
            movement = random.uniform(-0.001, 0.001)
            base_prices[ticker] *= (1 + movement)
            
            payload = {
                'ticker': ticker,
                'price': round(base_prices[ticker], 2),
                'timestamp': time.time()
            }
            
            producer.produce(
                TOPIC, 
                value=json.dumps(payload).encode('utf-8'), 
                callback=delivery_report
            )
        
        # Trigger delivery callbacks
        producer.poll(0)
        time.sleep(1)

except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    # Ensure all pending messages are delivered before exit
    producer.flush()
    print("Producer stopped.")