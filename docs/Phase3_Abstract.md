Wohlgenannt-Markus_IU14080395_DataEngineering_P3_S

# Abstract: Stream Processing Pipeline for Municipal Environmental Sensors

GitHub repository: https://github.com/Morwoi/Projekt-Data-Engineering-DLBDSEDE02

## Objective and scenario

The project builds the data side of a municipal environmental sensor
network: a pipeline that continuously ingests measurements, stores them
reliably and makes them usable for planning dashboards and a citizen
warning app. To give it a concrete operational purpose, it is placed in
Hamburg-Wilhelmsburg, an Elbe island that combines dense housing, a
high-rise estate, port industry, a park and dikes within a few
kilometres. Five stations stand for these five kinds of place, each
serving a specific user: a climate adaptation planner in the district
office (heat days, cloudbursts), a port terminal shift supervisor (wind
gusts), a care service for elderly residents (heat, with stricter
reliability rules) and residents in general. The planner's figures feed
a district assembly whose factions weigh trees, drainage and port
interests differently. The system succeeds if every reading is stored
exactly once and each user can tell how far a figure can be trusted. It
fails if the stream stalls on a temporary fault, if data is lost, or if
a number looks more certain than it is.

## Technical approach

Since the municipality's sensors are not available, the Open-Meteo API
stands in for them. A Python producer polls it every ten seconds per
station and publishes each reading as JSON to a Kafka topic partitioned
by station. Kafka runs as a single KRaft node locally; a multi-broker
cluster is a configuration change, not a code change. A Python consumer
writes each message to MongoDB, whose schema-less documents can take new
sensor types such as CO2 or noise without migrations. Offsets are
committed only after a successful write (at-least-once), writes are
upserts on station and timestamp so redelivery cannot create
duplicates, and failed writes are retried and then parked in a
dead-letter collection. If MongoDB is down even for that, the consumer
does not commit and rewinds, so Kafka buffers the data until the
database is back. If the API is
unreachable, the producer publishes an explicit offline record without
values instead of inventing any; a simulator mode remains only for
offline demonstrations. Everything starts with `docker compose up`.

## Integration into the end-user landscape

Host-side Python scripts act as the backend a user interface would
call. `planner_queries.py` aggregates daily statistics per station from
real readings only, reports how much of each day rests on real
measurements, and keeps how fast conditions change: the heaviest
rainfall and the largest temperature swing within one hour, plus the
DWD flags for hot days, tropical nights and heavy rain.
`materialize_daily_stats.py` keeps these rollups beyond the 90-day
raw-data retention. `citizen_status.py` classifies each station as ok, stale or
unavailable from the age of the last real reading, with a stricter
profile for vulnerable people. The repository contains the real weather
of July to September 2026 for the five stations, with documented
example ranges: on 30 July a cold front dropped the temperature in
Kirchdorf-Süd by 8.5 °C within one hour, which the daily mean hides; on
15 September almost all of the Inselpark's 12.1 mm fell within one
hour. The differences between stations are small, because a 2 km model
smooths out the urban heat island; real sensors would show more.

## Result

The pipeline processes a continuous stream from several stations and
does not break when the data source or the database is temporarily
unavailable. A test script forces a real API outage against the running
stack and confirms that the citizen status switches to unavailable and
back. Stopping MongoDB for 75 seconds, longer than all write retries,
lost no reading: every ten-second cycle of the outage was stored once
the database returned.


## Reflection

The most instructive problems produced no error at all. The
`kafka-python` client hung silently, which switching to
`confluent-kafka` solved. The consumer received nothing because Kafka's
default replication factor of 3 for internal topics cannot be met by a
single broker; setting it to 1 solved this. A local MongoDB
installation occupied port 27017, so a host script could silently have
read the wrong database; the stack now uses port 27018. Open-Meteo only
publishes a new observation every 15 minutes, so the daily rainfall sum
counted each value about 90 times; the aggregation now counts each
observation once. The first hourly rainfall figures exceeded some daily
totals because the window reached across midnight. In every case the
pipeline "ran"; only comparing stored values with expectations revealed
the fault. The same held for my claim that no data is lost: only
stopping the database for longer than the retries showed that a message
could be committed without even a dead-letter entry.

The tutor's feedback shaped both later phases. After phase 1 the concept
was too close to the data and too far from its real use, so I
implemented the users' queries and reliability rules as scripts instead
of adding description. After phase 2 I placed the system in a real
district with concrete users and added sample data with example ranges.
I also took up the two critical remarks in the code. Inventing values
for a broken sensor was wrong, so the default fallback is now an
explicit offline record. And the planner is right that change matters
more than the daily level, so the daily rows now carry hourly intensity.

I learned that a data system has to be judged by what its users will do
with the numbers, not by whether it runs. For similar projects I will
define the user and the decision first, test end to end with known
values, and state limitations where the reader will see them.

---

## Final product

### What the repository contains

A containerized pipeline (Kafka in KRaft mode, MongoDB, a Python
producer and a Python consumer) that streams environmental readings for
five stations in Hamburg-Wilhelmsburg from the Open-Meteo API into
MongoDB. It also contains host-side scripts for planners and a citizen
app backend, and a sample dataset of real weather from July to
September 2026. The README describes the architecture and configuration in full;
`producer/stations.json` and the script docstrings describe the area
and the users.

### How to use it

Requirements: Docker Desktop (or Docker Engine with the Compose plugin),
and Python 3 with `pymongo` for the host-side scripts.

1. Start the pipeline:

       git clone https://github.com/Morwoi/Projekt-Data-Engineering-DLBDSEDE02.git
       cd Projekt-Data-Engineering-DLBDSEDE02
       docker compose up --build -d

   Readings are stored in `environment_monitoring.sensor_readings`
   automatically. Kafka UI is available at http://localhost:8081.

2. Check the pipeline's health:

       pip install pymongo
       python scripts/check_status.py

3. Load the sample dataset and run the planner queries on it:

       python scripts/load_sample_data.py
       python scripts/planner_queries.py --from 2026-07-28 --to 2026-07-31
       python scripts/planner_queries.py --from 2026-09-15

   The README lists further example ranges and what to look for.

4. Query the citizen-app status, for the general public or for
   vulnerable people:

       python scripts/citizen_status.py
       python scripts/citizen_status.py --risk-profile vulnerable

5. Optionally, verify that the citizen status catches a real outage
   (takes a few minutes):

       python scripts/verify_citizen_failover.py

6. Stop everything with `docker compose down` (add `-v` to delete the
   stored data).
