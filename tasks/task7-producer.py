# filename: custom_partitioner_producer.py
from confluent_kafka import Producer

# (Copy the custom_partitioner function from above and place it here)
def custom_partitioner(key, partition_count):
    if partition_count < 3:
        raise ValueError("Topic must have at least 3 partitions for this custom partitioner to work.")
    if key is None:
        return 2
    try:
        print(f'key type : {type(key)}')
        key_str = key
        
        if key_str.startswith('A'):
            return 0
        elif key_str.startswith('B'):
            return 1
        else:
            return 2
    except UnicodeDecodeError:
        return 2


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result. """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        # This is the proof! Print the key and the partition it was delivered to.
        print(f"Message with key '{msg.key().decode('utf-8')}' delivered to partition [{msg.partition()}]")

# --- Producer Configuration ---
# The magic happens here by setting the 'partitioner' key.
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    # 'partitioner': custom_partitioner  #not supported in confluent kafka python
}

# --- Create Producer and Send Messages ---
producer = Producer(producer_conf)

messages_to_send = [
    ('Apple-123', 'This message should go to partition 0'),
    ('Banana-456', 'This message should go to partition 1'),
    ('Cherry-789', 'This message should go to partition 2'),
    ('Another-Apple', 'This one also goes to partition 0'),
    ('Zebra-000', 'This message is an "other" and goes to partition 2')
]

topic = 'practice-topic'
print(f"Sending messages to topic '{topic}' with custom partitioner...")

for key, value in messages_to_send:
    partition_calculated = custom_partitioner(key, 3)
    producer.produce(
        topic,
        key=key.encode('utf-8'),
        value=value.encode('utf-8'),
        callback=delivery_report,
        partition = partition_calculated
    )
    # Poll for delivery reports
    producer.poll(0)

# Wait for any outstanding messages to be delivered and delivery reports to be received.
print("Flushing messages...")
producer.flush()
print("All messages sent.")