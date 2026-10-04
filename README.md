# Municipal Environmental Sensor Streaming Pipeline

Course: DLBDSEDE02 - Project: Data Engineering | Task 2: Design and implement a stream processing pipeline

## System description

This project simulates a municipal environmental sensor network for
Hamburg-Wilhelmsburg. Five stations, each standing for a different kind
of place on the Elbe island (dense residential blocks, a high-rise
estate, port industry, a park, the dike), continuously report
environmental measurements (temperature, humidity, wind speed, wind
gusts, precipitation, air pressure). Which user each station serves is
recorded in [producer/stations.json](producer/stations.json). A Kafka
producer polls the public
[Open-Meteo](https://open-meteo.com) weather API once every 10 seconds per
station and publishes each reading as a JSON message to the Kafka topic
`sensor-readings`. A Kafka consumer reads the stream and persists every
reading into MongoDB, where it becomes available to downstream
applications such as planner dashboards or a citizen warning app.

If the public API is temporarily unreachable (network outage, timeout,
rate limiting), the stream does not stall. By default
(`FALLBACK_MODE=offline`) the producer publishes an explicit offline
record for the station, `"source": "offline"` with no measurement
values, which is how a real sensor network should report a failed
sensor: nothing is invented, and downstream users see the gap. For a
demonstration without an internet connection, `FALLBACK_MODE=simulate`
publishes plausible synthetic values instead, tagged
`"source": "simulated"`:

```bash
FALLBACK_MODE=simulate docker compose up -d
```

In both modes, neither the planner figures nor the citizen status ever
treat a fallback record as a measurement.

On the storage side, the consumer commits Kafka offsets only after a
successful MongoDB write (at-least-once delivery) and retries transient
MongoDB errors with backoff before giving up on a single message. Writes
are upserts keyed on `(station_id, timestamp)`, so a redelivered message
(possible under at-least-once delivery) updates the existing document
instead of creating a duplicate. If all retries on a message are
exhausted, it is parked in a `sensor_readings_dead_letters` collection
instead of being dropped; malformed messages go there too instead of
crashing the consumer. If MongoDB is down for longer, so that even the
dead-letter write fails, the consumer does not commit the offset and
rewinds to the same message. Kafka buffers everything behind it until
the database is back, so a database outage delays data but does not
lose it.

### Expected usages / end-user context

The users are described in the docstrings of the scripts that serve
them (`scripts/planner_queries.py`, `scripts/citizen_status.py`):

- The climate adaptation planner in the Hamburg-Mitte district office
  queries MongoDB (directly or through a BI/dashboard tool) to find hot
  days, warm nights and cloudburst rainfall per quarter, and to decide
  where trees, cooling spots and drainage relief go first. The figures
  are argued in the district assembly, whose factions weigh trees,
  drainage and port interests differently, so every figure carries its
  data coverage.
- A citizen-facing warning app (not part of this prototype) would read
  the latest readings per station and push alerts when values exceed
  recommended thresholds. Its users range from a care service for
  elderly residents in Kirchdorf-Süd (strict profile) to a port terminal
  supervisor who stops outdoor work above a gust limit, to residents in
  general.
- Future sensor types (CO2, noise, fine dust) can be added by extending
  the `metrics` fields in a reading without changing the pipeline
  architecture, since Kafka messages and MongoDB documents are both
  schema-flexible.

### Reliability requirements: planners versus the citizen-facing app

Kafka guarantees delivery between producer and MongoDB, but this does not
by itself determine what a planner or a citizen actually needs from the
data on a day-to-day basis. The two cases are handled differently:

- Planners look at how conditions change, over days and weeks but also
  within an hour: a cloudburst matters because of its intensity, a cold
  front because of its sudden drop. Raw readings stay queryable at full
  resolution for 90 days. `planner_queries.py` rolls them up into daily
  per-station rows that keep this information (the heaviest rainfall
  within one hour, the largest temperature swing within one hour, and
  flags for DWD hot days, tropical nights and heavy rain), and
  `materialize_daily_stats.py` persists that rollup into
  `daily_station_stats` so the history survives past the 90-day TTL on raw
  readings (see `RAW_RETENTION_DAYS` under "Configuration" below).
  Open-Meteo only publishes a new observation every 15 minutes, so the
  10-second polls return each observation about 90 times. The producer
  stores the source's own observation time as `observed_at`, and the
  aggregation counts each observation once, otherwise the daily rainfall
  total would be about 90 times too high. The
  mean/min/max/precipitation figures use only real (`source="api"`)
  readings; mixing in fallback-simulator values would make a day's figures
  less trustworthy without that being visible in the output. Each daily
  row also reports `api_coverage_pct`, the share of that day's readings
  that were real (offline records lower it), so a planner can decide
  independently whether to discount a low-confidence day, instead of that
  decision being made implicitly inside the average.
- A citizen deciding whether to go outside needs to know that the current
  reading is trustworthy, not just that some record was stored. During an
  API outage, records keep arriving on schedule (offline records, or
  synthetic values in simulate mode), so message age alone would report
  a broken sensor as fine. `citizen_status.py`
  therefore tracks the age of the last real (`source="api"`) reading
  separately and classifies each station as `ok` / `stale` / `unavailable`
  based on that (see the module docstring for the full logic). It also
  takes a `--risk-profile` option (`standard`, default, or `vulnerable`).
  For most users a slightly older reading may still be acceptable, but for
  someone with a health condition it may not be, so the `vulnerable`
  profile halves the thresholds and instructs the application to warn
  against relying on a stale reading instead of only reporting its age.

`verify_citizen_failover.py` forces a real outage against the running
stack and checks that the status flips to `unavailable` and back to `ok`,
rather than relying on the logic being correct on paper. Try it yourself
against the running stack:

```bash
docker compose up -d
python scripts/verify_citizen_failover.py
```

## Architecture

```
[Open-Meteo API]   [fallback: offline record
        \           or simulator]
         v                   v
        +---------------------+
        |   Kafka Producer     |
        +---------------------+
                   |
                   v
        +---------------------+
        | Kafka topic:          |
        | sensor-readings       |
        +---------------------+
                   |
                   v
        +---------------------+
        |   Kafka Consumer     |
        +---------------------+
                   |
                   v
        +---------------------+
        |   MongoDB            |
        | environment_monitoring.sensor_readings |
        +---------------------+
```

Kafka runs in single-node KRaft mode (no ZooKeeper dependency), which keeps
the local prototype simple while still using the same broker software and
wire protocol as a distributed, multi-broker production deployment, so
moving to a cloud-hosted, multi-node cluster later is a configuration
change, not a rewrite. The `sensor-readings` topic is created with 5
partitions (matching the number of stations) and messages are keyed by
`station_id`, so additional consumers in the same consumer group would
parallelize by station once more than one is deployed. Running only one
broker locally does mean there's no replication or fault tolerance for the
data topic itself; that's a limitation of the local prototype, not of the
architecture.

## How to run

Requirements: Docker Desktop (or Docker Engine + Compose plugin).

```bash
git clone [https://github.com/Morwoi/Projekt-Data-Engineering-DLBDSEDE02.git](https://github.com/Morwoi/Projekt_Data_Engineering_DLBDSEDE02.git)
cd Projekt-Data-Engineering-DLBDSEDE02
docker compose up --build
```

This builds the producer and consumer images, starts Kafka and MongoDB,
waits for both to report healthy, and then starts streaming
sensor readings into MongoDB automatically. No manual setup steps needed.

Kafka UI ([provectuslabs/kafka-ui](https://github.com/provectus/kafka-ui))
starts alongside the pipeline and is reachable at
<http://localhost:8081>. It shows the `sensor-readings` topic (partitions,
offsets, message contents) and its current throughput, without requiring
the Kafka CLI. Broker JMX metrics are enabled so the throughput figures
reflect actual traffic instead of showing zero. The overview also lists
`__consumer_offsets`, an internal Kafka topic with a fixed 50 partitions;
together with `sensor-readings` (5 partitions) this results in 2 topics
and 55 partitions being shown, which is expected and not a
misconfiguration.

To inspect the stored data:

```bash
docker exec -it mongo mongosh environment_monitoring --eval "db.sensor_readings.find().sort({timestamp:-1}).limit(5).pretty()"
```

To check whether any message failed all retries and was parked in the
dead-letter collection:

```bash
docker exec -it mongo mongosh environment_monitoring --eval "db.sensor_readings_dead_letters.find().pretty()"
```

To check the pipeline's health from the host machine (latest reading per
station, how stale each one is, and dead-letter count) instead of querying
Mongo by hand:

```bash
pip install pymongo
python scripts/check_status.py
```

To see the query strategy a city planner would use (daily
per-station trend aggregates computed from real readings only, with an
API-coverage figure for each day):

```bash
python scripts/planner_queries.py --days 30
python scripts/planner_queries.py --from 2026-07-28 --to 2026-07-31
```

A freshly started pipeline has only minutes of data. For multi-day
examples, load the sample dataset (next section).

To see the reliability contract a citizen-facing app backend would call
(ok / stale / unavailable per station, never presenting a stale reading
as current conditions), for the general public or for a risk-sensitive
profile with tighter thresholds:

```bash
python scripts/citizen_status.py
python scripts/citizen_status.py --risk-profile vulnerable
```

To persist the daily aggregation into a `daily_station_stats` collection
(needed once history is wanted beyond `RAW_RETENTION_DAYS`, since raw
readings expire on a TTL index, see `consumer/consumer.py`):

```bash
python scripts/materialize_daily_stats.py --days 2
```

To verify that the citizen-status contract catches a real Open-Meteo
outage, rather than being fooled by fallback records that keep arriving
on schedule (works in both `FALLBACK_MODE`s; requires `docker compose up -d` running and the
`docker` CLI on PATH; takes a few minutes, forces the producer's API
timeout to ~0 and back via `API_TIMEOUT_SECONDS`):

```bash
python scripts/verify_citizen_failover.py
```

### Sample dataset and example planner ranges

`sample_data/` contains the recent history for the five stations, 1 July
to 20 September 2026, one file per month and 39,360 readings in total,
in exactly the document format the producer publishes (`source="api"`,
plus a `dataset` tag). It ends a few days before this version was
submitted, so it does not overlap with a live stream started from it. They come from
Open-Meteo's archive of the ICON-D2 weather model (~2 km grid) at its
native 15-minute resolution. Nothing is interpolated or simulated.
`scripts/build_sample_dataset.py` shows how they were fetched and can
rebuild them.

Load them into the running stack (repeatable, upserts on the same key as
the consumer; `--remove` deletes them again):

```bash
docker compose up -d
pip install pymongo
python scripts/load_sample_data.py
```

Example ranges that work with the dataset:

| Range | What happens | Command | What to look at |
|---|---|---|---|
| 2026-07-01 to 2026-09-20 | The whole summer | `python scripts/planner_queries.py --from 2026-07-01 --to 2026-09-20 --station station-01` | `HOT DAY`s cluster at the end of July and in mid-August; September is wetter and windier |
| 2026-07-28 to 2026-07-31 | Heat spike and its end | `python scripts/planner_queries.py --from 2026-07-28 --to 2026-07-31` | the daily maximum goes 25 → 32 → 37 → 25 °C within four days, `HOT DAY` on 29 and 30 July at all stations |
| 2026-07-30 | Cold front | `python scripts/planner_queries.py --from 2026-07-30 --station station-02` | `Swing 1h` 8.5 °C: in Kirchdorf-Süd the temperature falls from 36.2 to 27.7 °C between 15:00 and 16:00 UTC, a change the daily mean (24.7 °C) does not show |
| 2026-08-05 | Warm night | `python scripts/planner_queries.py --from 2026-08-05` | `TROPICAL NIGHT` at four stations: daily minimum 21.0 °C in Reiherstieg (dense housing), but 19.5 °C in Kirchdorf-Süd |
| 2026-09-04 | Rain and wind | `python scripts/planner_queries.py --from 2026-09-04` | 14.2 mm at the dike (Georgswerder) vs. 9.4 mm in Kirchdorf-Süd; gusts of 60-67 km/h at all stations, 64.8 km/h at the port (Hohe Schaar) |
| 2026-09-15 | Evening cloudburst | `python scripts/planner_queries.py --from 2026-09-15` | 11.8 of the day's 12.1 mm at the Inselpark fell within one hour shortly before midnight UTC (`Max 1h`); the daily total alone would look like steady rain |

`--from`/`--to` are whole UTC days, both inclusive. Rolling one-hour
windows stay within their day, so rain falling across midnight is split
between both days. To keep the rollup of
an episode permanently (sample rows expire with the raw-data TTL like any
other reading):

```bash
python scripts/materialize_daily_stats.py --from 2026-07-01 --to 2026-09-20
```

Every sample day shows 96 readings (one per 15 minutes) and 100 % API
coverage, since archive data has no outages. The live stream shows the
other case: a lower coverage figure on days when the API timed out and
offline records were stored instead.

The differences between the stations are real model output but small
(on 29 and 30 July, the five daily maxima lie within 0.7 °C of each other): a
2 km weather model smooths out the urban heat island that makes a dense
quarter warmer than a park. Real sensors on site would show larger
differences. The pipeline and queries stay the same; only the data
source changes.

### Ports

| Service | Container port | Host port (default) | Why not the tool's own default |
|---|---|---|---|
| MongoDB | 27017 | 27018 | Avoids clashing with a MongoDB service some machines already run locally on 27017; connecting to the wrong database there produces no error, just wrong or empty results. |
| Kafka UI | 8080 | 8081 | Port 8080 is a common default for other local development tools, so collisions are frequent. |

Container-to-container traffic (e.g. producer/consumer -> `mongo:27017`,
`kafka-ui` -> `kafka:9092`) is unaffected by this, since services address
each other by container name over the `sensor-net` network and do not go
through the published host ports.

Both host ports are overridable via environment variables
(`MONGO_HOST_PORT`, `KAFKA_UI_HOST_PORT`) instead of being hardcoded, so if
even 27018 or 8081 happen to be taken on a given machine, that's a one-line
`.env` entry or `export` before `docker compose up`, not a compose-file
edit:

```bash
MONGO_HOST_PORT=27019 KAFKA_UI_HOST_PORT=8082 docker compose up -d
```

To stop everything:

```bash
docker compose down
```

Add `-v` to also remove the persisted Kafka/MongoDB volumes.

## Repository layout

```
docker-compose.yml      Orchestrates Kafka, Kafka UI, MongoDB, producer, consumer
producer/                Kafka producer: fetches Open-Meteo data, publishes offline records (or simulated values) on failure;
                          stations.json: the five Wilhelmsburg stations (shared with the scripts)
consumer/                Kafka consumer: writes readings into MongoDB with retry
scripts/                 check_status.py: host-side pipeline health check (fails on staleness or dead letters)
                          planner_queries.py: daily per-station rows with hourly intensity and DWD flags (real readings only, with API coverage) for city planners
                          materialize_daily_stats.py: persists the daily aggregation past raw-data TTL expiry
                          citizen_status.py: ok/stale/unavailable reliability contract for a citizen app, with a --risk-profile option
                          verify_citizen_failover.py: end-to-end test that the contract catches a real outage
                          load_sample_data.py: loads (or removes) the sample dataset in MongoDB
                          build_sample_dataset.py: rebuilds sample_data/ from the Open-Meteo archive
sample_data/             July to September 2026, one JSON Lines file per month, see "Sample dataset" above
docs/                    Portfolio submission texts (Phase 1 concept, Phase 2 explanation,
                          Phase 3 abstract and final product)
```

## Configuration

All settings are environment variables with sensible defaults (see
`docker-compose.yml`), e.g. `FETCH_INTERVAL_SECONDS`, `FALLBACK_MODE`
(`offline`, default, or `simulate`), `KAFKA_TOPIC`,
`MONGO_DB`, `RAW_RETENTION_DAYS` (how long raw readings are kept before a
MongoDB TTL index expires them; default 90), and `MONGO_HOST_PORT` /
`KAFKA_UI_HOST_PORT` (see "Ports" above). No API key is required for
Open-Meteo.
