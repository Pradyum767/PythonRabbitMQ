
import pika
import random
import time

class PIDController:
    def __init__(self, kp, ki, kd, setpoint):
        self.kp = kp
        self.ki = ki
        self.kd = kd
        self.setpoint = setpoint
        self.integral = 0
        self.previous_error = 0

    def update(self, measured_value):
        error = self.setpoint - measured_value
        self.integral += error
        derivative = error - self.previous_error

        output = self.kp * error + self.ki * self.integral + self.kd * derivative
        self.previous_error = error

        return output
   

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='task_queue', durable=True)

def publish_messages(channel, rate):
    for _ in range(rate):
        message = 'Task'
        channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
            ))
        print(f"Published: {message}")

   # Simulate publishing messages
for _ in range(10):
    publish_rate = random.randint(1, 10)
    publish_messages(channel, publish_rate)
    time.sleep(1)

def consume_messages(channel, method, properties, body):
    print(f"Consumed: {body}")
    channel.basic_ack(delivery_tag=method.delivery_tag)

def adjust_consumption_rate(pid, channel):
    queue_state = channel.queue_declare(queue='task_queue', durable=True, passive=True)
    current_queue_length = queue_state.method.message_count
    control_signal = pid.update(current_queue_length)
    consumption_rate = max(1, int(control_signal))
    return consumption_rate

# Parameters
desired_queue_length = 10
pid = PIDController(kp=1.0, ki=0.1, kd=0.05, setpoint=desired_queue_length)

# Adjust consumption rate based on PID control
for _ in range(10):
    consumption_rate = adjust_consumption_rate(pid, channel)
    for _ in range(consumption_rate):
        method_frame, header_frame, body = channel.basic_get(queue='task_queue')
        if method_frame:
            consume_messages(channel, method_frame, header_frame, body)
    time.sleep(1)

connection.close()

#In this example:
#- We set up a RabbitMQ connection and declare a queue.
#- We simulate publishing messages to the queue at a random rate.
#- We use the PID controller to adjust the rate at which messages are consumed from the queue to maintain the desired queue length.