import gzip
import json
import sqlite3
from pathlib import Path

DB_PATH = "data/schedule.db"

def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        PRAGMA journal_mode = WAL;

        CREATE TABLE IF NOT EXISTS tiploc (
            tiploc TEXT PRIMARY KEY,
            name TEXT,
            nlc TEXT,
            crs TEXT,
            stanox TEXT
        );

        CREATE TABLE IF NOT EXISTS service (
            uid TEXT PRIMARY KEY,
            toc TEXT,
            retail_service_id TEXT,
            origin_tiploc TEXT,
            destination_tiploc TEXT,
            valid_from TEXT,
            valid_to TEXT,
            days_running TEXT,
            stp_indicator TEXT
        );

        CREATE TABLE IF NOT EXISTS service_stop (
            uid TEXT,
            sequence INTEGER,
            tiploc TEXT,
            crs TEXT,
            arrival TEXT,
            departure TEXT,
            public_arrival TEXT,
            public_departure TEXT,
            activity TEXT,
            PRIMARY KEY (uid, sequence)
        );

        CREATE TABLE IF NOT EXISTS association (
            assoc_id TEXT PRIMARY KEY,
            main_uid TEXT,
            assoc_uid TEXT,
            assoc_type TEXT,
            is_passenger INTEGER
        );
    """)
    conn.commit()

def process_schedule_file(json_gz_path: str):
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    cur = conn.cursor()

    with gzip.open(json_gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            record_type = record.get("record_type")

            if record_type == "TIPLOC":
                cur.execute("""
                    INSERT OR REPLACE INTO tiploc
                    (tiploc, name, nlc, crs, stanox)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    record.get("tiploc"),
                    record.get("name"),
                    record.get("nlc"),
                    record.get("crs"),
                    record.get("stanox"),
                ))

            elif record_type == "SCHEDULE":
                # Full files have no 'action'; updates have 'action': 'create'/'delete'
                action = record.get("action", "create")
                if action == "delete":
                    cur.execute("DELETE FROM service_stop WHERE uid = ?", (record.get("uid"),))
                    cur.execute("DELETE FROM service WHERE uid = ?", (record.get("uid"),))
                    continue

                cur.execute("""
                    INSERT OR REPLACE INTO service
                    (uid, toc, retail_service_id, origin_tiploc, destination_tiploc,
                     valid_from, valid_to, days_running, stp_indicator)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    record.get("uid"),
                    record.get("toc"),
                    record.get("retail_service_id"),
                    record.get("origin_tiploc"),
                    record.get("destination_tiploc"),
                    record.get("valid_from"),
                    record.get("valid_to"),
                    record.get("days_running"),
                    record.get("stp_indicator"),
                ))

                # Locations are nested in the schedule record
                locations = record.get("locations", [])
                for seq, loc in enumerate(locations, start=1):
                    cur.execute("""
                        INSERT OR REPLACE INTO service_stop
                        (uid, sequence, tiploc, crs, arrival, departure,
                         public_arrival, public_departure, activity)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        record.get("uid"),
                        seq,
                        loc.get("tiploc"),
                        loc.get("crs"),
                        loc.get("arrival"),
                        loc.get("departure"),
                        loc.get("public_arrival"),
                        loc.get("public_departure"),
                        loc.get("activity"),
                    ))

            elif record_type == "ASSOCIATION":
                action = record.get("action", "create")
                if action == "delete":
                    cur.execute("DELETE FROM association WHERE assoc_id = ?", (record.get("assoc_id"),))
                    continue

                cur.execute("""
                    INSERT OR REPLACE INTO association
                    (assoc_id, main_uid, assoc_uid, assoc_type, is_passenger)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    record.get("assoc_id"),
                    record.get("main_uid"),
                    record.get("assoc_uid"),
                    record.get("assoc_type"),
                    1 if record.get("is_passenger") else 0,
                ))

            # Ignore HEADER and EOF

            # Commit in batches to avoid huge transactions
            if cur.rowcount % 10000 == 0:
                conn.commit()

    conn.commit()
    conn.close()

if __name__ == "__main__":
    process_schedule_file("data/schedule.json.gz")
