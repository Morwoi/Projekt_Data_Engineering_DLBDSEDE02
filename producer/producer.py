"""
Kafka producer for the municipal environmental sensor pipeline.

Polls the Open-Meteo weather API once per interval for the five stations
in stations.json (Hamburg-Wilhelmsburg) and publishes each reading as JSON to Kafka.

If the API is unreachable, the stream keeps running and FALLBACK_MODE
decides what is published instead:

- "offline" (default): an explicit offline record without any metric
  values, source="offline". This is what a real sensor network should
  do: report that a sensor failed instead of inventing values for it.
- "simulate": a plausible value from a small random walk around the last
  real reading, source="simulated". Only useful to demonstrate the
  pipeline and dashboards without an internet connection.

Every message carries "source" ("api", "offline" or "simulated"), so no
downstream figure can mistake a fallback record for a measurement.

Open-Meteo only publishes a new "current" observation every 15 minutes,
so most 10s polls return the same values again. observed_at carries the
source's own observation time so the planner queries can count each
observation once.
"""

import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka import KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger("producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "sensor-readings")
FETCH_INTERVAL_SECONDS = float(os.environ.get("FETCH_INTERVAL_SECONDS", "10"))
API_TIMEOUT_SECONDS = float(os.environ.get("API_TIMEOUT_SECONDS", "5"))
FALLBACK_MODE = os.environ.get("FALLBACK_MODE", "offline")
if FALLBACK_MODE not in ("offline", "simulate"):
    raise SystemExit(f"FALLBACK_MODE must be 'offline' or 'simulate', not {FALLBACK_MODE!r}")
API_BASE_URL = os.environ.get(
    "OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast"
)
CURRENT_VARS = (
    "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,"
    "precipitation,surface_pressure"
)

# Five fixed points in Hamburg-Wilhelmsburg standing in for real sensor
# stations. Wilhelmsburg is an Elbe island in the district Hamburg-Mitte
# where dense housing, a high-rise estate, port industry, a park and the
# dikes lie within a few kilometres, so heat, heavy rain and wind each
# matter to a different group. Each station stands for one kind of place;
# stations.json records its land use and which user it serves. The points
# are 2-5 km apart, so each falls into its own cell of the ~2 km weather
# model behind Open-Meteo. Kept in a JSON file so the host-side scripts
# read the same station registry.
STATIONS_FILE = os.environ.get(
    "STATIONS_FILE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "stations.json")
)
with open(STATIONS_FILE, encoding="utf-8") as _f:
    STATIONS = json.load(_f)

# Seeds the fallback simulator with each station's last real reading.
_last_good = {}

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    log.info("Shutdown signal received, finishing current cycle...")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_shutdown)
signal.signal(signal.SIGTERM, _handle_shutdown)


def connect_producer(retries=30, delay=5):
    """Create the producer and retry until Kafka is reachable (it starts
    slower than this container on `docker compose up`)."""
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "linger.ms": 200,
    })
    for attempt in range(1, retries + 1):
        try:
            producer.list_topics(timeout=5)
            log.info("Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except KafkaException:
            log.warning(
                "Kafka not reachable yet (attempt %s/%s), retrying in %ss...",
                attempt, retries, delay,
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after repeated retries")


def _delivery_report(err, msg):
    if err is not None:
        log.warning("Message delivery failed for %s: %s", msg.key(), err)


def fetch_from_api(station):
    """Fetch current weather for a station from Open-Meteo. Raises on any
    network/HTTP/parsing problem so the caller can fall back."""
    params = {
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "current": CURRENT_VARS,
        "timezone": "UTC",
    }
    response = requests.get(API_BASE_URL, params=params, timeout=API_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    current = payload["current"]

    return {
        # Open-Meteo returns UTC without an offset ("2026-09-24T19:15").
        "observed_at": datetime.fromisoformat(current["time"])
        .replace(tzinfo=timezone.utc).isoformat(),
        "temperature_c": current["temperature_2m"],
        "humidity_pct": current["relative_humidity_2m"],
        "wind_speed_kmh": current["wind_speed_10m"],
        "wind_gusts_kmh": current["wind_gusts_10m"],
        # Sum over the 15-minute observation interval.
        "precipitation_mm": current["precipitation"],
        "pressure_hpa": current["surface_pressure"],
    }


def simulate_reading(station):
    """Small random walk around the last known-good reading, or a
    sensible default on the first call for a station."""
    seed = _last_good.get(station["station_id"], {
        "temperature_c": 18.0,
        "humidity_pct": 60.0,
        "wind_speed_kmh": 10.0,
        "wind_gusts_kmh": 20.0,
        "precipitation_mm": 0.0,
        "pressure_hpa": 1013.0,
    })
    return {
        # Not a source observation, so the planner never de-duplicates it.
        "observed_at": None,
        "temperature_c": round(seed["temperature_c"] + random.uniform(-0.5, 0.5), 1),
        "humidity_pct": max(0, min(100, round(seed["humidity_pct"] + random.uniform(-2, 2), 1))),
        "wind_speed_kmh": max(0, round(seed["wind_speed_kmh"] + random.uniform(-1.5, 1.5), 1)),
        "wind_gusts_kmh": max(0, round(seed["wind_gusts_kmh"] + random.uniform(-3, 3), 1)),
        "precipitation_mm": max(0, round(seed["precipitation_mm"] + random.uniform(-0.1, 0.2), 2)),
        "pressure_hpa": round(seed["pressure_hpa"] + random.uniform(-0.5, 0.5), 1),
    }


def offline_record(exc):
    """No metric fields at all, so nothing can average them by accident.
    The error type tells an operator why the station went offline."""
    return {"observed_at": None, "error": type(exc).__name__}


def build_reading(station):
    """Try the real API first; on failure publish the FALLBACK_MODE record."""
    try:
        metrics = fetch_from_api(station)
        source = "api"
        _last_good[station["station_id"]] = metrics
    except (requests.RequestException, KeyError, ValueError) as exc:
        log.warning(
            "Open-Meteo unreachable for %s (%s), publishing %s record",
            station["station_id"], exc, FALLBACK_MODE,
        )
        if FALLBACK_MODE == "simulate":
            metrics, source = simulate_reading(station), "simulated"
        else:
            metrics, source = offline_record(exc), "offline"

    return {
        "station_id": station["station_id"],
        "station_name": station["name"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": source,
        **metrics,
    }


def main():
    producer = connect_producer()
    log.info(
        "Starting sensor stream: %s stations, every %ss, topic '%s', fallback '%s'",
        len(STATIONS), FETCH_INTERVAL_SECONDS, KAFKA_TOPIC, FALLBACK_MODE,
    )

    while not _shutdown:
        cycle_start = time.time()
        for station in STATIONS:
            reading = build_reading(station)
            producer.produce(
                KAFKA_TOPIC,
                key=station["station_id"],
                value=json.dumps(reading),
                callback=_delivery_report,
            )
            producer.poll(0)
            log.info("Published %s reading for %s", reading["source"], station["station_id"])

        producer.flush()
        elapsed = time.time() - cycle_start
        time.sleep(max(0.0, FETCH_INTERVAL_SECONDS - elapsed))

    log.info("Flushing producer...")
    producer.flush()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Producer crashed")
        sys.exit(1)
