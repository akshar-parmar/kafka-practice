# filename: consume_specific_partitions.py
import os
from confluent_kafka import Consumer, TopicPartition, KafkaError

# --- Consumer Configuration ---
# A 'group.id' is still required for the client to identify itself to the broker.
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your broker address
    'group.id': 'my-manual-assignment-group',
    'auto.offset.reset': 'earliest'
}

# Create a single consumer instance
consumer = Consumer(conf)

# --- Define the specific partitions to consume from ---
# This is the core of the solution.
topic = 'practice-topic'
partitions_to_assign = [
    TopicPartition(topic, 0),
    TopicPartition(topic, 1)
]

# Manually assign the consumer to these partitions
consumer.assign(partitions_to_assign)
print(f"Consumer assigned to partitions: {partitions_to_assign}")

try:
    while True:
        # Poll for new messages
        msg = consumer.poll(timeout=1.0) # 1-second timeout

        if msg is None:
            # No message received within the timeout period
            continue
        if msg.error():
            # Handle potential errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event is not an error
                print(f"Reached end of partition for {msg.topic()} [{msg.partition()}]")
            else:
                print(f"Error: {msg.error()}")
            continue

        # Successfully received a message
        print(f"Received message from partition {msg.partition()}: key='{msg.key().decode('utf-8') if msg.key() else None}' value='{msg.value().decode('utf-8')}'")

except KeyboardInterrupt:
    print("Stopping consumer...")
finally:
    # Cleanly close the consumer connection
    consumer.close()