import logging
import logging.config

from confluent_kafka import Consumer
import asyncio

logging.config.fileConfig(fname='logging.ini', disable_existing_loggers=False)
logger = logging.getLogger(__name__)

class ConsumerServer(Consumer):
    BROKERS_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
    SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"

    def __init__(self, topic: str, groupid: str, batch_size:int=5, sleep_secs:int=5):
        properties = {
            'bootstrap.servers': ConsumerServer.BROKERS_URL,
            'group.id': groupid,
        }
        super().__init__(properties)
        self.batch_size=batch_size
        self.sleep_secs=sleep_secs
        self.subscribe([topic])
        logger.info("Consumer built with properties: %s", properties)

    async def __run(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            messages = consumer.consume(self.batch_size)
            for message in messages:
                if message is None:
                    logger.info("No new messages at this time")
                elif message.error() is not None:
                    logger.error("Error retrieving messages: %s", message.error())
                else:
                    logger.info("Message recieved: %s", message.value())    
            await asyncio.sleep(self.sleep_secs)

    def run_consumer(self):
        try:
            asyncio.run((self.__run()))
        except KeyboardInterrupt as e:
            logger.info("CTRL-C, stopping")

if __name__ == '__main__':
    logger.info("Starting ConsumerServer")
    consumer = ConsumerServer(topic="sf_crime_data.police.service.calls", 
                              groupid="sf_data_crime.service.call.consumer")
    consumer.run_consumer()
