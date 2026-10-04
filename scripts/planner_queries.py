"""
City-planner query example for the sensor pipeline.

A MongoDB aggregation that rolls raw readings up into one row per
station and day, the unit a planning dashboard lists and compares. A
daily mean alone would hide what a planner needs most, namely how fast
conditions change: a cloudburst is dangerous because of its intensity,
not its daily total, and a cold front shows as a sudden temperature
drop. So each row also carries the heaviest rainfall within one hour and
the largest temperature swing within one hour, computed from the
readings at full resolution. The raw readings stay queryable for
RAW_RETENTION_DAYS for anything finer.

Intended user: the climate adaptation planner in the Hamburg-Mitte
district office, responsible for Wilhelmsburg. They decide which streets
get trees and unsealed surfaces first, where cooling spots are needed,
and where the drainage network needs relief, and argue these decisions
with concrete days: which quarter had hot days (flagged HOT DAY), which
one stayed warm at night (TROPICAL NIGHT), where a cloudburst dropped
the most rain within an hour (HEAVY RAIN). The figures end up in
the district assembly, whose factions weigh trees, drainage and port
interests differently. That is why every row shows its API coverage:
all sides argue from the same numbers, and a day resting on few real
readings is visible as such.

Readings are de-duplicated by observed_at before anything is averaged or
summed: the producer polls every 10s, but Open-Meteo only publishes a new
observation every 15 minutes, so each observation arrives about 90 times.
Without this step the daily precipitation total would count every
15-minute value about 90 times over.

Metric stats are computed from real (source="api") readings only.
Offline records carry no values, and see daily_trend()'s docstring for
why blending in fallback-simulator values would be a problem for a
planner specifically.

Days are UTC days, so a daily minimum stands in for the night minimum
(the coolest hours fall well inside the UTC day in Hamburg).

Raw readings are only kept for RAW_RETENTION_DAYS (see consumer/consumer.py,
default 90 days, TTL-enforced). A dashboard needing history beyond that
window would persist this aggregation's output (see
materialize_daily_stats.py) before the raw documents expire.

Run from the host (requires `pip install pymongo`) while
`docker compose up` is running:

    python scripts/planner_queries.py --days 30
    python scripts/planner_queries.py --days 7 --station station-01

For a fixed date range, e.g. on the sample dataset (see README >
"Sample dataset and example planner ranges"):

    python scripts/planner_queries.py --from 2026-07-28 --to 2026-07-31
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27018")
MONGO_DB = os.environ.get("MONGO_DB", "environment_monitoring")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "sensor_readings")

# Metrics a planner cares about as trends, each rolled up to daily
# mean/min/max. Precipitation is additionally summed, since "total rainfall
# that day" is the planning-relevant figure, not its average.
METRICS = ["temperature_c", "humidity_pct", "wind_speed_kmh", "wind_gusts_kmh", "pressure_hpa"]

# German Weather Service (DWD) climatological definitions of a "hot day"
# (daily max >= 30 C) and a "tropical night" (daily min >= 20 C).
HOT_DAY_MAX_C = 30.0
TROPICAL_NIGHT_MIN_C = 20.0
# Lower bound of the DWD warning for heavy rain ("Starkregen"), 15 l/m2
# (= mm) within one hour.
HEAVY_RAIN_MM_1H = 15.0

IS_API = {"$eq": ["$source", "api"]}
API_TEMPERATURE = {"$cond": [IS_API, "$temperature_c", None]}


def time_window(days=30, date_from=None, date_to=None):
    """(start, end) in UTC. --from/--to are whole UTC days, both
    inclusive; otherwise the last `days` days up to now."""
    if date_from:
        start = datetime.fromisoformat(date_from).replace(tzinfo=timezone.utc)
        last_day = datetime.fromisoformat(date_to).replace(tzinfo=timezone.utc) if date_to else start
        return start, last_day + timedelta(days=1)
    end = datetime.now(timezone.utc)
    return end - timedelta(days=days), end


def daily_trend(collection, days=30, station_id=None, start=None, end=None):
    """Aggregates raw readings into one document per (station, day) with
    mean/min/max per metric, total precipitation, and reading count.

    The mean/min/max/precipitation figures use source="api" readings
    only. Averaging in fallback-simulator readings would make a day's
    trend number less trustworthy without anything in the output showing
    that. citizen_status.py already refuses to do this for a single
    reading; this is the same idea applied to an aggregate, just harder
    to notice there if you don't guard against it. api_reading_count,
    simulated_count and api_coverage_pct expose how much of a day's
    number actually rests on real measurements, so that's something the
    caller can weigh instead of it being decided for them.
    """
    group_fields = {}
    for metric in METRICS:
        api_value = {"$cond": [IS_API, f"${metric}", None]}
        group_fields[f"{metric}_avg"] = {"$avg": api_value}
        group_fields[f"{metric}_min"] = {"$min": api_value}
        group_fields[f"{metric}_max"] = {"$max": api_value}

    if start is None:
        start, end = time_window(days)
    initial_match = {"timestamp": {"$gte": start.isoformat(), "$lt": end.isoformat()}}
    if station_id:
        initial_match["station_id"] = station_id

    pipeline = [
        # Pre-filter on the indexed string fields before parsing dates.
        {"$match": initial_match},
        # One document per source observation. Simulated readings have
        # no observed_at and stay one row each, like readings stored
        # before observed_at existed. "polls" keeps the original count.
        {"$group": {
            "_id": {
                "station_id": "$station_id",
                "obs": {"$cond": [IS_API, {"$ifNull": ["$observed_at", "$timestamp"]}, "$timestamp"]},
            },
            "source": {"$first": "$source"},
            "polls": {"$sum": 1},
            **{m: {"$first": f"${m}"} for m in METRICS + ["precipitation_mm"]},
        }},
        {"$addFields": {"station_id": "$_id.station_id"}},
        # The observation time is an ISO string; parse to a real date so
        # it can be truncated to a calendar day.
        {"$addFields": {"_ts": {"$dateFromString": {"dateString": "$_id.obs"}}}},
        # Rolling one-hour windows over the observations, per station and
        # day so a day's hourly maximum can never exceed its total.
        # Each precipitation value is the sum of the preceding 15 minutes,
        # so the observations from t-45min to t add up to one hour of rain.
        # The temperature window spans t-60min to t, one full hour.
        {"$setWindowFields": {
            "partitionBy": {
                "station_id": "$station_id",
                "day": {"$dateTrunc": {"date": "$_ts", "unit": "day"}},
            },
            "sortBy": {"_ts": 1},
            "output": {
                "rain_1h": {
                    "$sum": {"$cond": [IS_API, "$precipitation_mm", 0]},
                    "window": {"range": [-45, 0], "unit": "minute"},
                },
                "temp_1h_min": {
                    "$min": API_TEMPERATURE,
                    "window": {"range": [-60, 0], "unit": "minute"},
                },
                "temp_1h_max": {
                    "$max": API_TEMPERATURE,
                    "window": {"range": [-60, 0], "unit": "minute"},
                },
            },
        }},
        {"$group": {
            "_id": {
                "station_id": "$station_id",
                "day": {"$dateTrunc": {"date": "$_ts", "unit": "day"}},
            },
            **group_fields,
            "precipitation_mm_total": {"$sum": {"$cond": [IS_API, "$precipitation_mm", 0]}},
            "precipitation_mm_max_1h": {"$max": "$rain_1h"},
            "temperature_c_max_swing_1h": {
                "$max": {"$subtract": ["$temp_1h_max", "$temp_1h_min"]}
            },
            "reading_count": {"$sum": "$polls"},
            "api_reading_count": {"$sum": {"$cond": [IS_API, "$polls", 0]}},
            "simulated_count": {
                "$sum": {"$cond": [{"$eq": ["$source", "simulated"]}, "$polls", 0]}
            },
            "offline_count": {
                "$sum": {"$cond": [{"$eq": ["$source", "offline"]}, "$polls", 0]}
            },
        }},
        {"$sort": {"_id.station_id": ASCENDING, "_id.day": ASCENDING}},
    ]
    rows = list(collection.aggregate(pipeline, allowDiskUse=True))
    for row in rows:
        row["api_coverage_pct"] = (
            round(100 * row["api_reading_count"] / row["reading_count"], 1)
            if row["reading_count"] else 0.0
        )
        tmax, tmin = row["temperature_c_max"], row["temperature_c_min"]
        row["hot_day"] = tmax is not None and tmax >= HOT_DAY_MAX_C
        row["tropical_night"] = tmin is not None and tmin >= TROPICAL_NIGHT_MIN_C
        row["heavy_rain"] = (row["precipitation_mm_max_1h"] or 0) >= HEAVY_RAIN_MM_1H
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days", type=int, default=30, help="how many days back to aggregate (default: 30, ignored with --from)")
    parser.add_argument("--from", dest="date_from", help="first UTC day, YYYY-MM-DD (inclusive)")
    parser.add_argument("--to", dest="date_to", help="last UTC day, YYYY-MM-DD (inclusive, default: same as --from)")
    parser.add_argument("--station", default=None, help="restrict to one station_id (default: all stations)")
    args = parser.parse_args()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    collection = client[MONGO_DB][MONGO_COLLECTION]
    start, end = time_window(args.days, args.date_from, args.date_to)
    rows = daily_trend(collection, station_id=args.station, start=start, end=end)

    if not rows:
        print(f"No readings between {start:%Y-%m-%d %H:%M} and {end:%Y-%m-%d %H:%M} UTC"
              + (f" for {args.station}" if args.station else "") + ".")
        print("For historical examples, load the sample dataset first: "
              "python scripts/load_sample_data.py")
        return

    def fmt(value, width, decimals=1):
        if value is None:
            return "n/a".rjust(width)
        return f"{value:{width}.{decimals}f}"

    header = (f"{'Station':<14} {'Day':<12} {'Temp avg':>9} {'Temp min/max':>14} "
              f"{'Swing 1h':>9} {'Humidity avg':>13} {'Wind avg':>9} {'Gust max':>9} "
              f"{'Precip total':>13} {'Max 1h':>8} {'#Readings':>10} {'API cov.':>9}")
    print(header)
    print("-" * len(header))
    for row in rows:
        station_id = row["_id"]["station_id"]
        day = row["_id"]["day"].strftime("%Y-%m-%d")
        flags = [label for key, label in (("hot_day", "HOT DAY"),
                                          ("tropical_night", "TROPICAL NIGHT"),
                                          ("heavy_rain", "HEAVY RAIN")) if row[key]]
        print(
            f"{station_id:<14} {day:<12} "
            f"{fmt(row['temperature_c_avg'], 6)}C "
            f"{fmt(row['temperature_c_min'], 4)}/{fmt(row['temperature_c_max'], 4)}C "
            f"{fmt(row['temperature_c_max_swing_1h'], 8)}C "
            f"{fmt(row['humidity_pct_avg'], 12)}% "
            f"{fmt(row['wind_speed_kmh_avg'], 9)} "
            f"{fmt(row['wind_gusts_kmh_max'], 9)} "
            f"{row['precipitation_mm_total']:>11.1f}mm "
            f"{fmt(row['precipitation_mm_max_1h'], 6)}mm "
            f"{row['reading_count']:>10} "
            f"{row['api_coverage_pct']:>8.0f}%"
            + ("  " + ", ".join(flags) if flags else "")
        )

    print()
    low_coverage_days = sum(1 for row in rows if row["api_coverage_pct"] < 50)
    print("Temp/Humidity/Wind/Precip figures above are computed from real "
          "(source=\"api\") readings only; offline records and fallback-simulator "
          "values never enter these numbers. 'Swing 1h' is the largest "
          "temperature change within one hour, 'Max 1h' the heaviest rainfall "
          "within one hour. 'API cov.' is the share of that day's readings "
          "that were real. A day at 0% had no real readings at all, so its "
          "figures show as 'n/a' rather than a synthetic-only number.")
    if low_coverage_days:
        print(f"[!] {low_coverage_days} day(s) below 50% API coverage: treat "
              "those trend points as low-confidence even though a number is shown.")


if __name__ == "__main__":
    main()
