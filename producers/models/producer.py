"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient  # , NewTopic
from confluent_kafka.avro import AvroProducer
from confluent_kafka.cimpl import NewTopic

logger = logging.getLogger(__name__)

BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
            self,
            topic_name: str,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """
        Initializes a Producer object with basic settings
        :param topic_name: name of Kafka topic
        :param key_schema: AVRO Schema
        :param value_schema: Default to None
        :param num_partitions: Default to 1
        :param num_replicas: Default to 1
        """
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            "request.required.acks": self.num_replicas  # TODO
            # TODO
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            client = AdminClient(self.broker_properties)
            futures = client.create_topics([
                NewTopic(topic=self.topic_name,
                         num_partitions=self.num_partitions,
                         replication_factor=self.num_replicas)
            ])
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            {
                "bootstrap.servers": BROKER_URL,
                "schema.registry.url": "http://localhost:8086"
            },
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        if self.topic_name not in self.existing_topics:
            self.existing_topics.add(self.topic_name)
            pass
        logger.info("topic creation kafka integration incomplete - skipping")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        self.producer.flush()
        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    @staticmethod
    def sanitize_station_name(name: str) -> str:
        return name.lower() \
            .replace("/", "_and_") \
            .replace(" ", "_") \
            .replace("-", "_") \
            .replace("'", "")
