
from consumer_interface import mqConsumerInterface

import pika
import os
import json

class mqConsumer(mqConsumerInterface): 
    
    def __init__(self,exchange_name, queue_name, binding_key) -> None: # second variable?
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.binding_key = binding_key
        super(self, self.exchange_name)
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        # Set-up Connection to RabbitMQ service

        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)

        # Establish Channel
        self.channel = self.connection.channel()

        # Create the exchange if not already present
        exchange = self.channel.exchange_declare(exchange=self.exchange_name)



    def bindQueueToExchange(self, queueName: str, topic: str) -> None:
        # Bind Binding Key to Queue on the exchange
        self.channel.queue_bind(
            queue= queueName,
            routing_key= topic,
            exchange=self.exchange_name,
        )



    def createQueue(self, queueName: str) -> None:
        # Create Queue if not already present -- check if not present
        self.channel.queue_declare(queue=queueName)


        # Set-up Callback function for receiving messages
        self.channel.basic_consume(
            queueName, self.createQueue, auto_ack=False 
        )



    def on_message_callback(self, channel, method_frame, header_frame, body):
        # De-Serialize JSON message object if Stock Object Sent
        message = json.loads(body)


        # Acknowledge And Print Message
        channel.basic_ack(method_frame.delivery_tag, False)

        print(message)


    def startConsuming(self) -> None:
        self.channel.start_consuming()
    

    # def Del(self) -> None:
    #     self.channel.close()
    #     self.connection.close()


