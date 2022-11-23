from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time


# producer.send(TOPIC_NAME, b'Milo')

# producer.flush()

# # Send to topic items the test message. The b is treat string as binary as kafka needs this.
# producer.send(TOPIC_NAME, b'Test Message!!!')

# # This kafka would wait until multiple messages are ready and send in one request for efficiency.
# # Using flush just sends the message immediately.
# producer.flush()

TOPIC_NAME = 'Users'
KAFKA_SERVER = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                         security_protocol="PLAINTEXT",
                         )


topic = TOPIC_NAME


def write_object(obj: dict = None):
    value = json.dumps(obj).encode("utf-8")
    send(topic, value)


def send(topic, value, key=None):
    print(f"Sending {len(value)} bytes with value {value} to {topic}")
    producer.send(topic, value=value, key=key)


def run(message="Milo"):
    print(f"Start generator {KAFKA_SERVER}/{TOPIC_NAME}")
    obj_id = 0
    while True:
        write_object({
            "id": obj_id,
            "message": message
        })
        obj_id += 1
        time.sleep(10)


try:
    run()
except:
    print("Error connecting")
