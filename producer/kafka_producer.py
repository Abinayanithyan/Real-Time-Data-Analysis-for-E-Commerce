import argparse
import time
import random
import json
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

fake = Faker()

def create_kafka_producer(bootstrap_servers: list) -> KafkaProducer:
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,  
        linger_ms=10,  # small buffer to improve throughput
        acks='all',  # Ensure message is written to all replicas
        request_timeout_ms=30000,
        retry_backoff_ms=100
    )

def generate_transaction(force_success: bool = False) -> dict:
    """Generate a single e-commerce transaction."""
    
    # For testing, you can bias toward success
    if force_success:
        status_choices = ["Success"] * 7 + ["Failed"] * 2 + ["Pending"] * 1  # 70% success rate
    else:
        status_choices = ["Success", "Failed", "Pending"]
    
    return {
        "transaction_id": fake.uuid4(),
        "user_id": fake.uuid4(),
        "user_name": fake.name(),
        "email": fake.email(),
        "age": random.randint(18, 70),
        "gender": random.choice(["Male", "Female", "Other"]),
        "country": fake.country(),
        "city": fake.city(),
        "product_id": fake.uuid4(),
        "product_name": random.choice(["Laptop", "Phone", "Headphones", "Shoes", "Watch", "Tablet"]),
        "category": random.choice(["Electronics", "Fashion", "Home", "Sports", "Books"]),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 2000), 2),
        "payment_method": random.choice(["Credit Card", "Debit Card", "UPI", "Net Banking", "COD"]),
        "transaction_status": random.choice(status_choices),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

def run_streaming(producer: KafkaProducer, topic: str, interval: float, count: int, force_success: bool = False) -> None:
    """Send transactions continuously at a fixed interval."""
    print(f"🚀 Streaming {count} transactions to topic '{topic}' every {interval}s...")
    
    success_count = 0
    failed_count = 0
    pending_count = 0
    
    for i in range(count):
        transaction = generate_transaction(force_success)
        try:
            # Send with callback to confirm delivery
            future = producer.send(topic, value=transaction)
            record_metadata = future.get(timeout=10)
            
            # Count statuses
            if transaction['transaction_status'] == 'Success':
                success_count += 1
            elif transaction['transaction_status'] == 'Failed':
                failed_count += 1
            else:
                pending_count += 1
                
            print(f"✅ Sent [{i+1}/{count}]: {transaction['transaction_id']} | {transaction['transaction_status']} | {transaction['product_name']} | ${transaction['price']}")
            print(f"   📍 Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
        except KafkaError as e:
            print(f"❌ Failed to send transaction: {e}")
        time.sleep(interval)
    
    print(f"""
📊 BATCH SUMMARY:
   Total Sent: {count}
   Success: {success_count} ({success_count/count*100:.1f}%)
   Failed: {failed_count} ({failed_count/count*100:.1f}%)
   Pending: {pending_count} ({pending_count/count*100:.1f}%)
    """)
    print("✅ Finished streaming transactions.")

def run_batch(producer: KafkaProducer, topic: str, rows: int, force_success: bool = False) -> None:
    """Send a large number of transactions quickly."""
    print(f"📦 Batch sending {rows} transactions to topic '{topic}'...")
    
    success_count = 0
    failed_count = 0
    pending_count = 0
    
    for i in range(rows):
        transaction = generate_transaction(force_success)
        try:
            producer.send(topic, value=transaction)
            
            # Count statuses
            if transaction['transaction_status'] == 'Success':
                success_count += 1
            elif transaction['transaction_status'] == 'Failed':
                failed_count += 1
            else:
                pending_count += 1
                
            if (i + 1) % 100 == 0:
                print(f"📤 Sent {i + 1}/{rows} transactions...")
                
        except KafkaError as e:
            print(f"❌ Failed to send transaction: {e}")
    
    producer.flush()
    print(f"""
📊 BATCH SUMMARY:
   Total Sent: {rows}
   Success: {success_count} ({success_count/rows*100:.1f}%)
   Failed: {failed_count} ({failed_count/rows*100:.1f}%)
   Pending: {pending_count} ({pending_count/rows*100:.1f}%)
    """)
    print(f"✅ Finished batch of {rows} transactions.")

def test_connection(bootstrap_servers: list, topic: str) -> bool:
    """Test Kafka connection and topic accessibility."""
    print(f"🔍 Testing connection to {bootstrap_servers}...")
    try:
        producer = create_kafka_producer(bootstrap_servers)
        
        # Send a test message
        test_transaction = {
            "transaction_id": "test-connection",
            "transaction_status": "Success",
            "test": True,
            "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        future = producer.send(topic, value=test_transaction)
        record_metadata = future.get(timeout=10)
        
        print(f"✅ Connection successful!")
        print(f"   📍 Topic: {topic}")
        print(f"   📍 Partition: {record_metadata.partition}")
        print(f"   📍 Offset: {record_metadata.offset}")
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enhanced Kafka E-commerce Transaction Producer")
    parser.add_argument("--mode", choices=["stream", "batch", "test"], default="stream", help="Mode to run the producer")
    parser.add_argument("--interval", type=float, default=1, help="Interval between messages (stream mode)")
    parser.add_argument("--count", type=int, default=10, help="Number of messages to send (stream mode)")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate (batch mode)")
    parser.add_argument("--topic", type=str, default="transactions", help="Kafka topic to send messages to")
    parser.add_argument("--force-success", action="store_true", help="Bias toward successful transactions (70% success rate)")
    parser.add_argument("--server", type=str, default="localhost:9092", help="Kafka bootstrap server")
    args = parser.parse_args()

    # Create producer with specified server
    bootstrap_servers = [args.server]
    
    if args.mode == "test":
        test_connection(bootstrap_servers, args.topic)
    else:
        producer = create_kafka_producer(bootstrap_servers)
        
        if args.mode == "stream":
            run_streaming(producer, args.topic, args.interval, args.count, args.force_success)
        elif args.mode == "batch":
            run_batch(producer, args.topic, args.rows, args.force_success)

        producer.close()

# USAGE EXAMPLES:
# python generator.py --mode test --server localhost:9092
# python generator.py --mode stream --count 30 --interval 0.5 --force-success
# python generator.py --mode stream --count 30 --server kafka:29092  # if running in container