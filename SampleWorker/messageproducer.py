#!/usr/bin/env python
import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

#durable=True means if producer has published the message and consumer hasn't consumed and in between if the rabbitMq is down, the message will not be lost
channel.queue_declare(queue='task_queue', durable=True)

message = ' '.join(sys.argv[1:]) or "Hello World!"

#exhange='' to make sure only one consumer received the message and not all the consumer
#delivery_mode=2 means the message will persisit, so it goes hand in hand with durable=true, so after rabbitMq goes down and cones back it should be able to read the message from persistent storage
channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=2,
    ))
print(" [x] Sent %r" % message)
connection.close()

