#!/usr/bin/env python
import pika, sys, os

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='sampleQueue')

    def callback(ch, method, properties, body): #defining function to handle callback
        print(" [x] Received %r" % body)

    channel.basic_consume(queue='sampleQueue', on_message_callback=callback, auto_ack=True) #

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)

# Note: If multiple consumers then the message will be consumed in roundrobin manner