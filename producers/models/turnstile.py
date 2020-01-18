"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro
from confluent_kafka.avro import ClientError

from .producer import Producer
from .turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")

    #
    # TODO: Define this value schema in `schemas/turnstile_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_value.json")

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = self.sanitize_station_name(station.name)

        #
        #
        # TODO: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        super().__init__(
            # topic_name=f"mh_turnstile_{station_name}",  # TODO: Come up with a better topic name
            topic_name="mh_turnstile_for_ksql",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,  # TODO: Uncomment once schema is defined
            num_partitions=3,  # TODO: why not three
            num_replicas=1,  # TODO: low replicas for simulation
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        logger.info(f"turnstile {self.topic_name} kafka processed {num_entries}")
        #
        #
        # TODO: Complete this function by emitting a message to the turnstile topic for the number
        # of entries that were calculated
        #
        #
        for entry in range(num_entries):
            try:
                self.producer.produce(
                    topic=self.topic_name,
                    key={"timestamp": self.time_millis()},
                    value={
                        "station_id": self.station.station_id,
                        "station_name": self.station.name,
                        "line": self.station.color.name
                    },
                    callback=self.delivery_report
                )
            except ClientError as err:
                print(f"Produce topic-name: {self.topic_name}. Error: {err}")
