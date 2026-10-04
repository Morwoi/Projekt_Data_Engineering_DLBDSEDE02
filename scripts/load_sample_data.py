"""
Loads the committed sample dataset (sample_data/*.jsonl) into the
running pipeline's MongoDB collection, so the planner queries have a few
days of real historical weather to work on right away.

Rows are upserted on the same (station_id, timestamp) key the consumer
uses, so loading twice changes nothing. They get a fresh `stored_at`,
which means the raw-data TTL (RAW_RETENTION_DAYS) expires them the same
number of days after loading. Run materialize_daily_stats.py on the
sample ranges to keep the rollups permanently.

    python scripts/load_sample_data.py
    python scripts/load_sample_data.py --remove
"""

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

sys.path.insert(0, os.path.dirname(__file__))
from planner_queries import MONGO_COLLECTION, MONGO_DB, MONGO_URI  # noqa: E402

SAMPLE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "sample_data")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--remove", action="store_true", help="delete all sample rows instead of loading them")
    args = parser.parse_args()

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except PyMongoError as exc:
        print(f"Cannot reach MongoDB at {MONGO_URI}: {exc}")
        print("Is `docker compose up` running?")
        sys.exit(1)

    collection = client[MONGO_DB][MONGO_COLLECTION]

    if args.remove:
        result = collection.delete_many({"dataset": {"$regex": "^sample:"}})
        print(f"Removed {result.deleted_count} sample reading(s).")
        return

    # Same unique index the consumer creates on startup. Without it, each
    # upsert scans the whole collection and loading takes very long if
    # this runs before the consumer ever has.
    collection.create_index([("station_id", 1), ("timestamp", 1)], unique=True)

    now = datetime.now(timezone.utc)
    for path in sorted(glob.glob(os.path.join(SAMPLE_DIR, "*.jsonl"))):
        ops = []
        with open(path, encoding="utf-8") as f:
            for line in f:
                doc = json.loads(line)
                key = {"station_id": doc["station_id"], "timestamp": doc["timestamp"]}
                ops.append(UpdateOne(key, {"$set": {**doc, "stored_at": now}}, upsert=True))
        if ops:
            result = collection.bulk_write(ops, ordered=False)
            print(f"{os.path.basename(path)}: {len(ops)} readings "
                  f"({result.upserted_count} new, {result.modified_count} updated)")


if __name__ == "__main__":
    main()
