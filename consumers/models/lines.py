"""Contains functionality related to Lines"""
import json
import logging

from models import Line

logger = logging.getLogger(__name__)


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        message_topic = message.topic()
        # if "org.chicago.cta.station" in message_topic:
        if "mh_station_" in message_topic:
            value = message.value()
            # if message_topic == "org.chicago.cta.stations.table.v1":
            if "mh_station_db_stations" == message_topic:
                station = json.loads(value)
                if station['red']:
                    self.red_line.process_message(message)
                elif station['blue']:
                    self.blue_line.process_message(message)
                elif station['green']:
                    self.green_line.process_message(message)
                return
            line_color = value.get("line", "<unknown>")
            if line_color == "green":
                self.green_line.process_message(message)
            elif line_color == "red":
                self.red_line.process_message(message)
            elif line_color == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", line_color)
        elif "TURNSTILE_SUMMARY" == message_topic:
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message_topic)
