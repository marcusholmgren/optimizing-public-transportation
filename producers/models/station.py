"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging
from pathlib import Path

from confluent_kafka import avro
from confluent_kafka.avro import ClientError

from .train import Train
from .turnstile import Turnstile
from .producer import Producer

logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")

    #
    # TODO: Define this value schema in `schemas/station_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self, station_id, name: str, color, direction_a=None, direction_b=None):
        self.name = name
        station_name = self.sanitize_station_name(self.name)

        # Producer()
        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #

        super().__init__(
            topic_name=f"mh_station_arrival_{station_name}",  # TODO: Come up with a better topic name
            key_schema=Station.key_schema,
            value_schema=Station.value_schema,  # TODO: Uncomment once schema is defined
            num_partitions=3,  # TODO: why not three
            num_replicas=1,  # TODO: low replicas for simulation
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)

    def run(self, train: Train, direction: str, prev_station_id, prev_direction):
        """Simulates train arrivals at this station"""
        #
        #
        # TODO: Complete this function by producing an arrival message to Kafka
        #
        #
        logger.info(str(self))

        try:
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value={
                    "station_id": self.station_id,
                    "train_id": train.train_id,
                    "direction": direction,
                    "line": self.color.name,
                    "train_status": train.status.name,
                    "prev_station_id": prev_station_id,
                    "prev_direction": prev_direction
                    #
                    #
                    # TODO: Configure this
                    #
                    #
                },
                callback=self.delivery_report
            )
        except ClientError as err:
            print(f"Produce topic-name: {self.topic_name}. Error: {err}")
        except Exception as e:
            print(f"noo- what is this?  Error: {e}")


    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()
