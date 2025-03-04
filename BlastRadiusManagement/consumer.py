import pika
import json
import time
import random
import logging

RABBITMQ_HOST = "localhost"
QUEUE_NAME = "task_queue"
DLQ_NAME = "dead_letter_queue"

MAX_RETRIES = 3
CIRCUIT_BREAKER_THRESHOLD = 5  # Stop processing if this many errors occur
CIRCUIT_BREAKER_COOLDOWN = 10  # Cooldown period in seconds
RETRY_DELAY_BASE = 2  # Base delay in seconds for exponential backoff

error_count = 0

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("consumer.log"),
        logging.StreamHandler()
    ]
)

def exponential_backoff(retry_count):
    """Returns the delay time using exponential backoff strategy."""
    return min(RETRY_DELAY_BASE * (2 ** retry_count), 60)  # Max delay 60s

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

def process_message(body):
    "Simulate message processing with random failures."
    task = json.loads(body)
    logging.info(f"Processing: {task}")
    if random.random() < 0.2:  # Simulate 20% failure rate
        raise Exception("Random processing error!")

def move_to_dlq(channel, body):
    "Move failed messages to Dead Letter Queue."
    channel.basic_publish(
        exchange='',
        routing_key=DLQ_NAME,
        body=body,
        properties=pika.BasicProperties(delivery_mode=2)
    )
    logging.warning(f"Moved to DLQ: {body}")

def callback(ch, method, properties, body):
    "Consumer callback function with retry and DLQ handling."
    global error_count

    try:
        process_message(body)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logging.info(f"Successfully processed message: {body}")
        error_count = 0  # Reset error counter on success
    except Exception as e:
        retries = (properties.headers or {}).get("x-retry", 0) + 1 #fetch retries from message header or set it to 0 if not present
        logging.error(f"Error processing message: {e}. Retry {retries}/{MAX_RETRIES}")

        if retries >= MAX_RETRIES:
            move_to_dlq(ch, body)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            delay = exponential_backoff(retries)
            logging.info(f"Requeueing message with delay: {delay}s")
            time.sleep(delay)  # Simulate delayed retry
            ch.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(
                    headers={'x-retry': retries}, delivery_mode=2
                )
            )
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        error_count += 1

    if error_count >= CIRCUIT_BREAKER_THRESHOLD:
        logging.critical("Circuit breaker triggered! Stopping consumer.")
        ch.stop_consuming()
        time.sleep(CIRCUIT_BREAKER_COOLDOWN)
        logging.info("Resuming consumer after cooldown.")
        error_count = 0


def start_consumer():
    """Starts the RabbitMQ consumer with auto-recovery."""
    while True:
        try:
            connection = connect()
            channel = connection.channel()
            
            # Declare main queue and Dead Letter Queue
            channel.queue_declare(queue=QUEUE_NAME, durable=True)
            channel.queue_declare(queue=DLQ_NAME, durable=True)

            # Set prefetch count to avoid overwhelming consumers
            channel.basic_qos(prefetch_count=1)

            # Start consuming messages
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
            logging.info("Consumer is running...")
            
            channel.start_consuming()
        except (pika.exceptions.AMQPConnectionError, pika.exceptions.AMQPChannelError):
            logging.error("Connection lost. Restarting consumer...")
            time.sleep(5)  # Avoid infinite fast looping
        except KeyboardInterrupt:
            logging.info("Stopping consumer...")
            channel.stop_consuming()

if __name__ == "__main__":
    start_consumer()
