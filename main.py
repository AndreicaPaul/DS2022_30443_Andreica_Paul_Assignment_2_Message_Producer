import csv
import time
import pika
import environ
import sched

# Setting up the scheduler
scheduler = sched.scheduler(time.time,
                            time.sleep)

# Reading the device_id from the configuration file
env = environ.Env()
environ.Env.read_env()

# Read the device_id from the environment variables
device_id = env('DEVICE_ID')
speed = int(env('SPEED'))

# Set up the connection to RabbitMQ
factory = pika.URLParameters('amqps://hffmdlnp:R8ZzOkgP4SJKlguOWffjC5F6XS0BqGvd@sparrow.rmq.cloudamqp.com/hffmdlnp')
connection = pika.BlockingConnection(factory)
channel = connection.channel()

# Declare the 'sensors' queue
channel.queue_declare(queue='sensors')

def send_message(value):
    print(f"I read: {value}")
    # Get the current timestamp
    timestamp = int(time.time())
    # Construct the message as a JSON string
    json_str = f'{{"timestamp":{timestamp},"device_id":"{device_id}","measurement_value":"{value}"}}'
    # Publish the message to the 'sensors' queue
    channel.basic_publish(exchange='', routing_key='sensors', body=json_str)
    print("sent")

def main():
    file = open('sensor.csv')
    csvreader = csv.reader(file)

    data = []
    for row in csvreader:
        data.append(row)

    for i in range(0, len(data)):
        delay = i * speed
        scheduler.enter(delay, 1, send_message, (data[i][0],))
    scheduler.run()

main()