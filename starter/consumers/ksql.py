"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8098"


KSQL_STATEMENT = """
CREATE TABLE IF NOT EXISTS turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='turnstile-event',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);
create table turnstile_summary 
with (VALUE_FORMAT='JSON') 
AS 
select station_id, count(station_id) as count from turnstile group by station_id;

"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        # headers={"Content-Type": "application/vnd.ksql.v1+json"},
        headers={
            "Content-Type": "application/vnd.ksql.v1+json; charset=utf-8",
            # "Accept": "application/vnd.ksql.v1+json"
        },
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
