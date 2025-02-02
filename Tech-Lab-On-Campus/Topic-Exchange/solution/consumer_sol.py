from consumer_interface import mqConsumerInterface
import sys
import pika
import os

class mqConsumer(mqConsumerInterface):

    def __init__(self, binding_key: str, exchange_name: str, queue_name: str) -> None:
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:

        self.channel.exchange_declare(
            exchange=self.exchange_name, exchange_type="topic"
        )
        
        self.channel.queue_declare(queue=self.queue_name)

        self.channel.queue_bind(
            queue= self.queue_name,
            routing_key= self.binding_key,
            exchange = self.exchange_name,
        )
        self.channel.basic_consume(
            self.queue_name, self.on_message_callback, auto_ack=False
        )
    
    def on_message_callback(self, channel, method_frame, header_frame, body) -> None:
        channel.basic_ack(method_frame.delivery_tag, False)
        print(body)

    def startConsuming(self) -> None:
        self.channel.start_consuming()

    def __del__(self) -> None:
        self.channel.close()
        self.connection.close()
        
    

