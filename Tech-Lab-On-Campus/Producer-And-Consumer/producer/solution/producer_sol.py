from producer_interface import mqProducerInterface
import pika
import os

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key,exchange_name):
        # body of constructor
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.setupRMQConnection()

       

    def setupRMQConnection(self):    
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.connection = pika.BlockingConnection(parameters=con_params)
        self.channel = self.connection.channel()

    def publishOrder(self, message:str):
        exchange = self.channel.exchange_declare(exchange="Exchange Name")
        self.channel.basic_publish(
            exchange="Exchange Name", routing_key="Routing Key", body="Message",
            )
        self.channel.close()
        self.connection.close()
