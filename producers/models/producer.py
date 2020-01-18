"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer
# from confluent_kafka.cimpl import NewTopic

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
            "request.required.acks": self.num_replicas,  # TODO
            "schema.registry.url": "http://localhost:8081"  # TODO
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
            # TODO why did it create a client here?
            # client = AdminClient({"bootstrap.servers": BROKER_URL})
            # if not self.topic_exists(client):
            #     futures = client.create_topics([
            #         NewTopic(topic=self.topic_name,
            #                  num_partitions=self.num_partitions,
            #                  replication_factor=self.num_replicas)
            #     ])
            #     self.create_topic()
            #     Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        self.producer = AvroProducer(
            self.broker_properties,
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


        # See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.admin.NewTopic
        # See: https://docs.confluent.io/current/installation/configuration/topic-configs.html
        client = AdminClient({"bootstrap.servers": BROKER_URL})
        if self.topic_exists(client):
            logger.info(f"topic {self.topic_name} already exists.")
            return

        futures = client.create_topics([
            NewTopic(topic=self.topic_name,
                     num_partitions=self.num_partitions,
                     replication_factor=self.num_replicas)
        ])

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"topic {self.topic_name} created.")
            except Exception as e:
                logger.error(f"failed to create topic {self.topic_name}: {e}", exc_info=e)
                raise

    def topic_exists(self, client) -> bool:
        """Checks if the given topic exists"""
        # TODO: Check to see if the given topic exists
        #       See: https://docs.confluent.io/current/clients/confluent-kafka-python/#confluent_kafka.Consumer.list_topics
        return client.list_topics(topic=self.topic_name, timeout=10.0) is not None

    def delivery_report(self, err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        self.producer.flush()
        logger.info("producer close complete - goodbye")

    def time_millis(self) -> int:
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    @staticmethod
    def sanitize_station_name(name: str) -> str:
        return name.lower() \
            .replace("/", "_and_") \
            .replace(" ", "_") \
            .replace("-", "_") \
            .replace("'", "")
