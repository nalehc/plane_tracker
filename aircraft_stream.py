import json
import socket
import time
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import polars as pl

HOST = "127.0.0.1"
PORT = 30003
RAW_DIR = Path("data/raw")
SNAPSHOT_DIR = Path("data/snapshots")
LOCAL_TZ = ZoneInfo("America/Los_Angeles")

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
    parts = line.strip().split(",")

    if len(parts) < 22 or parts[0] != "MSG":
        return None

    now_utc = datetime.now(timezone.utc)
    now_local = now_utc.astimezone(LOCAL_TZ)

    return {
        "received_at_utc": now_utc.isoformat(),
        "received_at_local": now_local.isoformat(),
        "message_type": parts[1],
        "icao": blank_to_none(parts[4]),
        "flight": blank_to_none(parts[10]),
        "altitude_ft": to_int(parts[11]),
        "ground_speed_kt": to_float(parts[12]),
        "track_deg": to_float(parts[13]),
        "lat": to_float(parts[14]),
        "lon": to_float(parts[15]),
        "vertical_rate_fpm": to_int(parts[16]),
    }


def write_raw(record, path=RAW_DIR):
    """Write each parsed record to JSONL file at location path/date/hour in local timezone."""
    received_at = datetime.fromisoformat(record["received_at_local"])

    outdir = path / f"{received_at:%Y-%m-%d}" / f"{received_at:%H}"
    outdir.mkdir(parents=True, exist_ok=True)

    outfile_path = outdir / "aircraft.jsonl"

    with open(outfile_path, "a") as outfile:
        outfile.write(json.dumps(record) + "\n")

class CurrentFlights():
    def __init__(self):
        self.data = {}
    
    def update(self, record):
        icao = record["icao"]
        if not icao:
            return

        aircraft = self.data.setdefault(icao, {"icao": icao})

        for key, value in record.items():
            if value is not None:
                aircraft[key] = value

        aircraft["last_seen_utc"] = record["received_at_utc"]
        aircraft["last_seen_local"] = record["received_at_local"]

    def records(self):
        return list(self.data.values())

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

                    if time.monotonic() - last_snapshot_time > 5:
                        write_parquet(current_flights)
                        last_snapshot_time = time.monotonic()

        except OSError as e:
            print(f"Connection failed: {e}. Retrying...")
            time.sleep(0.5)


if __name__ == "__main__":
    run()