"""
Builds the sample dataset in sample_data/ from Open-Meteo's historical
forecast archive, so the planner queries can be tried on real past
weather without running the pipeline for days first.

Each line in the output is one document in exactly the shape the live
producer publishes (same fields, source="api"), at the source's native
15-minute resolution. Nothing is interpolated or simulated. The extra
field `dataset` marks the rows so they can be told apart from live data
(and removed again, see load_sample_data.py --remove).

The dataset files are already committed; this script only documents how
they were made and lets anyone rebuild or extend them:

    pip install requests
    python scripts/build_sample_dataset.py
"""

import json
import os
from datetime import datetime, timezone

import requests

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT_DIR = os.path.join(ROOT, "sample_data")
API_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"

with open(os.path.join(ROOT, "producer", "stations.json"), encoding="utf-8") as f:
    STATIONS = json.load(f)

# The recent months from Open-Meteo's ICON-D2 archive for the five
# stations, one file per month, ending a few days before submission so
# it does not overlap with a live stream started afterwards; see README >
# "Sample dataset".
EPISODES = [
    ("2026-07", "2026-07-01", "2026-07-31"),
    ("2026-08", "2026-08-01", "2026-08-31"),
    ("2026-09", "2026-09-01", "2026-09-20"),
]

# ICON-D2 runs on a ~2 km grid, fine enough for the five stations to get
# distinct values. Open-Meteo's default for older dates is a coarser
# model under which several stations share one grid cell.
MODEL = "icon_d2"

# Open-Meteo variable -> field name used by producer/producer.py.
VARIABLES = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "wind_speed_10m": "wind_speed_kmh",
    "wind_gusts_10m": "wind_gusts_kmh",
    "precipitation": "precipitation_mm",
    "surface_pressure": "pressure_hpa",
}


def fetch_episode(name, start_date, end_date):
    response = requests.get(API_URL, params={
        "latitude": ",".join(str(s["latitude"]) for s in STATIONS),
        "longitude": ",".join(str(s["longitude"]) for s in STATIONS),
        "start_date": start_date,
        "end_date": end_date,
        "minutely_15": ",".join(VARIABLES),
        "timezone": "UTC",
        "models": MODEL,
    }, timeout=60)
    response.raise_for_status()

    documents = []
    for station, payload in zip(STATIONS, response.json()):
        series = payload["minutely_15"]
        for i, time in enumerate(series["time"]):
            metrics = {field: series[var][i] for var, field in VARIABLES.items()}
            if any(value is None for value in metrics.values()):
                continue
            observed_at = datetime.fromisoformat(time).replace(tzinfo=timezone.utc).isoformat()
            documents.append({
                "station_id": station["station_id"],
                "station_name": station["name"],
                "latitude": station["latitude"],
                "longitude": station["longitude"],
                # No separate poll time exists for archive data.
                "timestamp": observed_at,
                "source": "api",
                "observed_at": observed_at,
                **metrics,
                "dataset": f"sample:{name}",
            })
    return documents


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    for name, start_date, end_date in EPISODES:
        documents = fetch_episode(name, start_date, end_date)
        path = os.path.join(OUT_DIR, f"{name}.jsonl")
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            for doc in documents:
                f.write(json.dumps(doc) + "\n")
        print(f"{path}: {len(documents)} readings ({start_date} to {end_date})")


if __name__ == "__main__":
    main()
