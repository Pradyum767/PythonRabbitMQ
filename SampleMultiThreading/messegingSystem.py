import pika, threading, signal, sys

# Create a lock object
lock = threading.Lock()

# Function to publish a message
def publish_message(counter):
    print("Entered producer")
#    time.sleep(counter)
    with lock:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='hello')

        for i in range(100):
            channel.basic_publish(exchange='', routing_key='hello', body='Hello World! '+str(counter)+' '+str(i))
        connection.close()
        

# Function to consume messages
def consume_messages():
    print("Entered Consumer")
    with lock:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel=connection.channel()
        channel.queue_declare(queue='hello')

        def callback(ch, method, properties, body):
            print(f"Received {body}")

        channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
        print('Waiting for messages. To exit press CTRL+C')

    channel.start_consuming()


# Create threads for publishing and consuming
publish_thread = [threading.Thread(target=publish_message,args=(counter,)) for counter in range(5)]
consume_thread = threading.Thread(target=consume_messages)

# Start the threads
for thread in publish_thread:
    thread.start()
consume_thread.start()

# Wait for the threads to finish
for thread in publish_thread:
    thread.join()
consume_thread.join()


#Notes:
#Working of Lock in python
#Acquire the Lock: When the with lock statement is encountered, the lock is acquired. If another thread already holds the lock, the current thread will wait until the lock is released.
#Execute the Block: Once the lock is acquired, the block of code within the with statement is executed.
#Release the Lock: After the block of code is executed, the lock is automatically released, even if an exception occurs within the block.