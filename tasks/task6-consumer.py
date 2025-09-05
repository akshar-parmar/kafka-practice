# filename: dlq_consumer_monitor.py
import json
from confluent_kafka import Consumer, KafkaError

# --- Consumer Configuration ---
# Use a unique group.id so this monitor doesn't interfere with other consumers.
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'dlq-monitor-group',
    # 'earliest' ensures we see all messages in the topic from the very beginning
    # every time we start the script.
    'auto.offset.reset': 'earliest'
}

# --- Create the Consumer Instance ---
consumer = Consumer(conf)

# --- Subscribe to the dead-letter-topic ---
# We use .subscribe() for automatic partition assignment by the group coordinator.
dlq_topic = 'dead-letter-topic'
consumer.subscribe([dlq_topic])

print(f"Subscribed to topic: '{dlq_topic}'")
print("Waiting for failed messages...")

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            # Standard error handling
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break

        # --- Process and Print the DLQ Message ---
        print("\n" + "="*50)
        print("🚨 FAILED MESSAGE DETECTED 🚨")
        print("="*50)

        try:
            # The message value is a JSON string, so we decode and parse it
            message_payload_str = msg.value().decode('utf-8')
            data = json.loads(message_payload_str)

            # Print the enriched information in a readable format
            print(f"      Failure Timestamp: {data.get('failure_timestamp', 'N/A')}")
            print(f"         Failure Reason: {data.get('failure_reason', 'N/A')}")
            print("-" * 50)
            print(f"       Original Message: {data.get('original_message', 'N/A')}")
            print(f"         Original Topic: {data.get('original_topic', 'N/A')}")
            print(f"     Original Partition: {data.get('original_partition', 'N/A')}")
            print(f"        Original Offset: {data.get('original_offset', 'N/A')}")


        except json.JSONDecodeError:
            # Handle cases where the message is not valid JSON
            print("  [ERROR] Could not parse message as JSON.")
            print(f"  Raw Message Value: {msg.value().decode('utf-8')}")
        except Exception as e:
            print(f"  [ERROR] An unexpected error occurred: {e}")

except KeyboardInterrupt:
    print("\nStopping DLQ monitor...")
finally:
    # Cleanly close the consumer connection
    consumer.close()