import pika
import json
import time
import random
import logging

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "task_queue"

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("producer.log"),
        logging.StreamHandler()
    ]
)

def connect():
    "Establish connection to RabbitMQ with retries."
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(RABBITMQ_HOST)
            )
            logging.info("Connected to RabbitMQ.")
            return connection
        except pika.exceptions.AMQPConnectionError:
            logging.error("Connection failed. Retrying in 5 seconds...")
            time.sleep(5)

def publish_message(channel, message):
    "Send message with persistent delivery."
    headers = {"x-retry": 0}  # initial retry count
    try:
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                headers=headers
            )
        )
        logging.info(f"Sent: {message}")
    except Exception as e:
        logging.error(f"Failed to publish message: {e}")

        
if __name__ == "__main__":
    connection = connect()
    channel = connection.channel()
    
    # Declare queue with durability
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Send messages
    for i in range(10):
        message = {"task_id": i, "data": random.randint(1, 100)}
        publish_message(channel, message)
        time.sleep(1)  # Simulating slow producer

    connection.close()