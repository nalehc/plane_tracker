import json
import polars as pl
import socket
import time
from datetime import datetime, timezone
from geopy.distance import geodesic
from pathlib import Path
from zoneinfo import ZoneInfo

from flights import Aircraft, CurrentFlights
from config.local import *

SBS_FIELDS = {
    "message_type": 1,
    "icao": 4,
    "flight": 10,
    "altitude_ft": 11,
    "ground_speed_kt": 12,
    "track_deg": 13,
    "lat": 14,
    "lon": 15,
    "vertical_rate_fpm": 16,
}

def blank_to_none(value):
    value = value.strip()
    return value if value else None

def to_int(value):
    value = value.strip()
    return int(value) if value else None

def to_float(value):
    value = value.strip()
    return float(value) if value else None

def connect_stream(host=HOST, port=PORT):
    return socket.create_connection((host,port))

def parse_line(line):
    fields = line.strip().split(",")

    if len(fields) < 22 or fields[0] != "MSG":
        return None

    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(LOCAL_TZ)
    
    record = {
        "received_at_utc": now_utc.isoformat(),
        "received_at_local": now_local.isoformat(),
        "message_type": fields[SBS_FIELDS["message_type"]],
        "icao": blank_to_none(fields[SBS_FIELDS["icao"]]),
        "flight": blank_to_none(fields[SBS_FIELDS["flight"]]),
        "altitude_ft": to_int(fields[SBS_FIELDS["altitude_ft"]]),
        "ground_speed_kt": to_float(fields[SBS_FIELDS["ground_speed_kt"]]),
        "track_deg": to_float(fields[SBS_FIELDS["track_deg"]]),
        "lat": to_float(fields[SBS_FIELDS["lat"]]),
        "lon": to_float(fields[SBS_FIELDS["lon"]]),
        "vertical_rate_fpm": to_int(fields[SBS_FIELDS["vertical_rate_fpm"]]),
    }

    return record

def write_raw(record, path=RAW_DIR):
    """Write each parsed record to JSONL file at location path/date/hour in local timezone."""
    received_at = datetime.fromisoformat(record["received_at_local"])

    outdir = path / f"{received_at:%Y-%m-%d}" / f"{received_at:%H}"
    outdir.mkdir(parents=True, exist_ok=True)

    outfile_path = outdir / "aircraft.jsonl"

    with open(outfile_path, "a") as outfile:
        outfile.write(json.dumps(record) + "\n")

def write_parquet(current_flights, path=SNAPSHOT_DIR):
    now_local = datetime.now(LOCAL_TZ)

    outdir = path / f"{now_local:%Y-%m-%d}" / f"{now_local:%H}"
    outdir.mkdir(parents=True, exist_ok=True)

    outfile_path = outdir / f"aircraft_state_{now_local:%H-%M-%S}.parquet"

    records = current_flights.records()
    if not records:
        return

    df = pl.DataFrame(records)
    df.write_parquet(outfile_path)

    print(f"Wrote snapshot: {outfile_path}")


def run():
    current_flights = CurrentFlights()
    last_snapshot_time = time.monotonic()

    while True:
        try:
            with connect_stream() as sock:
                stream = sock.makefile()

                for line in stream:
                    record = parse_line(line)
                    if record is None:
                        continue

                    write_raw(record)
                    current_flights.update(record)
                    aircraft = current_flights.data.get(record["icao"])
                    if aircraft:
                        aircraft.update_distance(
                            HOME_LAT,
                            HOME_LON,
                        )

                    for aircraft in current_flights.visible(VISIBLE_DISTANCE_MI, VISIBLE_ALTITUDE_FT):
                        print(aircraft.data)

                    if time.monotonic() - last_snapshot_time > 5:
                        write_parquet(current_flights)
                        last_snapshot_time = time.monotonic()

        except OSError as e:
            print(f"Connection failed: {e}. Retrying...")
            time.sleep(0.5)


if __name__ == "__main__":
    run()