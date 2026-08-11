import gzip
import json
import sqlite3
from pathlib import Path

DB_PATH = Path("data/schedule.db")


def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;

        DROP TABLE IF EXISTS service_stop;
        DROP TABLE IF EXISTS service;
        DROP TABLE IF EXISTS association;
        DROP TABLE IF EXISTS tiploc;

        CREATE TABLE tiploc (
            tiploc TEXT PRIMARY KEY,
            name TEXT,
            nlc TEXT,
            crs TEXT,
            stanox TEXT
        );

        -- Schedules are unique on (uid, stp, start date), not uid alone
        CREATE TABLE service (
            uid TEXT NOT NULL,
            toc TEXT,
            retail_service_id TEXT,
            origin_tiploc TEXT,
            destination_tiploc TEXT,
            valid_from TEXT NOT NULL,
            valid_to TEXT,
            days_running TEXT,
            stp_indicator TEXT NOT NULL,
            PRIMARY KEY (uid, stp_indicator, valid_from)
        );

        CREATE TABLE service_stop (
            uid TEXT NOT NULL,
            stp_indicator TEXT NOT NULL,
            valid_from TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            tiploc TEXT,
            crs TEXT,
            arrival TEXT,
            departure TEXT,
            public_arrival TEXT,
            public_departure TEXT,
            activity TEXT,
            PRIMARY KEY (uid, stp_indicator, valid_from, sequence)
        );

        CREATE TABLE association (
            assoc_id TEXT PRIMARY KEY,
            main_uid TEXT,
            assoc_uid TEXT,
            assoc_type TEXT,
            location TEXT,
            valid_from TEXT,
            valid_to TEXT,
            days_running TEXT,
            stp_indicator TEXT,
            is_passenger INTEGER
        );
    """)
    conn.commit()


def _unwrap(record: dict) -> tuple[str | None, dict | None]:
    """Return (record_kind, payload) for a Network Rail JSON line."""
    if "TiplocV1" in record:
        return "TIPLOC", record["TiplocV1"]
    if "JsonScheduleV1" in record:
        return "SCHEDULE", record["JsonScheduleV1"]
    if "JsonAssociationV1" in record:
        return "ASSOCIATION", record["JsonAssociationV1"]
    return None, None


def _assoc_id(payload: dict) -> str:
    return "|".join([
        payload.get("main_train_uid") or "",
        payload.get("assoc_train_uid") or "",
        payload.get("location") or "",
        payload.get("assoc_start_date") or "",
        payload.get("CIF_stp_indicator") or "",
        payload.get("category") or "",
        payload.get("base_location_suffix") or "",
        payload.get("assoc_location_suffix") or "",
    ])


def process_schedule_file(json_gz_path: str):
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    cur = conn.cursor()

    processed = 0
    inserted = {"tiploc": 0, "service": 0, "service_stop": 0, "association": 0}
    deleted = {"service": 0, "association": 0}

    with gzip.open(json_gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            kind, payload = _unwrap(record)
            if kind is None or payload is None:
                continue  # HEADER / EOF

            tx = (payload.get("transaction_type") or "Create").lower()

            if kind == "TIPLOC":
                cur.execute(
                    """
                    INSERT OR REPLACE INTO tiploc
                    (tiploc, name, nlc, crs, stanox)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        payload.get("tiploc_code"),
                        payload.get("tps_description") or payload.get("description"),
                        payload.get("nalco"),
                        payload.get("crs_code"),
                        payload.get("stanox"),
                    ),
                )
                inserted["tiploc"] += 1

            elif kind == "SCHEDULE":
                uid = payload.get("CIF_train_uid")
                stp = payload.get("CIF_stp_indicator")
                valid_from = payload.get("schedule_start_date")

                if tx == "delete":
                    cur.execute(
                        """
                        DELETE FROM service_stop
                        WHERE uid = ? AND stp_indicator = ? AND valid_from = ?
                        """,
                        (uid, stp, valid_from),
                    )
                    cur.execute(
                        """
                        DELETE FROM service
                        WHERE uid = ? AND stp_indicator = ? AND valid_from = ?
                        """,
                        (uid, stp, valid_from),
                    )
                    deleted["service"] += 1
                else:
                    segment = payload.get("schedule_segment") or {}
                    locations = segment.get("schedule_location") or []
                    origin = locations[0].get("tiploc_code") if locations else None
                    destination = locations[-1].get("tiploc_code") if locations else None

                    cur.execute(
                        """
                        INSERT OR REPLACE INTO service
                        (uid, toc, retail_service_id, origin_tiploc, destination_tiploc,
                         valid_from, valid_to, days_running, stp_indicator)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            uid,
                            payload.get("atoc_code"),
                            segment.get("RSID"),
                            origin,
                            destination,
                            valid_from,
                            payload.get("schedule_end_date"),
                            payload.get("schedule_days_runs"),
                            stp,
                        ),
                    )
                    inserted["service"] += 1

                    # Replace stops for this schedule identity
                    cur.execute(
                        """
                        DELETE FROM service_stop
                        WHERE uid = ? AND stp_indicator = ? AND valid_from = ?
                        """,
                        (uid, stp, valid_from),
                    )
                    for seq, loc in enumerate(locations, start=1):
                        cur.execute(
                            """
                            INSERT INTO service_stop
                            (uid, stp_indicator, valid_from, sequence, tiploc, crs,
                             arrival, departure, public_arrival, public_departure, activity)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                uid,
                                stp,
                                valid_from,
                                seq,
                                loc.get("tiploc_code"),
                                loc.get("crs_code"),
                                loc.get("arrival"),
                                loc.get("departure"),
                                loc.get("public_arrival"),
                                loc.get("public_departure"),
                                loc.get("location_type"),
                            ),
                        )
                        inserted["service_stop"] += 1

            elif kind == "ASSOCIATION":
                assoc_id = _assoc_id(payload)
                if tx == "delete":
                    cur.execute("DELETE FROM association WHERE assoc_id = ?", (assoc_id,))
                    deleted["association"] += 1
                else:
                    category = payload.get("category")
                    cur.execute(
                        """
                        INSERT OR REPLACE INTO association
                        (assoc_id, main_uid, assoc_uid, assoc_type, location,
                         valid_from, valid_to, days_running, stp_indicator, is_passenger)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            assoc_id,
                            payload.get("main_train_uid"),
                            payload.get("assoc_train_uid"),
                            category,
                            payload.get("location"),
                            payload.get("assoc_start_date"),
                            payload.get("assoc_end_date"),
                            payload.get("assoc_days"),
                            payload.get("CIF_stp_indicator"),
                            1 if category in ("JJ", "VV") else 0,
                        ),
                    )
                    inserted["association"] += 1

            processed += 1
            if processed % 10000 == 0:
                conn.commit()
                print(f"processed {processed:,} ... {inserted}")

    conn.commit()
    conn.close()
    print(f"done. processed={processed:,}")
    print(f"inserted={inserted}")
    print(f"deleted={deleted}")


if __name__ == "__main__":
    process_schedule_file("data/schedule.json.gz")
