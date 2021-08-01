"""Contains functionality related to Weather"""
import logging


logger = logging.getLogger(__name__)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        value = message.value()
        if message.error() is not None:
            logger.error("weather message had an error")
        elif value is None:
            logger.warning("No message value for weather")
        else:
            self.temperature = value.get("temperature")
            self.status = value.get("status")