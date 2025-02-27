#!/usr/bin/env python
import pika #Client library for rabbitMQ

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost')) #create connection with rabbitMQ
channel = connection.channel() #create a channel to this connection

channel.queue_declare(queue='sampleQueue') #declare a queue, it will create if not exist(can be removed if queue exists)

channel.basic_publish(exchange='', routing_key='sampleQueue', body='Hello World!') #routingKey=queuename
print(" [x] Sent 'Hello World!'")
connection.close() #close the connection