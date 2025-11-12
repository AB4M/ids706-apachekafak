import os, json, time, psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import execute_values
from datetime import datetime, timezone

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot_readings")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "iot-consumer")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "streamdb")
PG_USER = os.getenv("PG_USER", "stream")
PG_PASSWORD = os.getenv("PG_PASSWORD", "stream")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))

DDL = """
CREATE TABLE IF NOT EXISTS iot_readings (
    id BIGSERIAL PRIMARY KEY,
    event_ts TIMESTAMPTZ NOT NULL,
    device_id TEXT NOT NULL,
    building TEXT NOT NULL,
    floor INT NOT NULL,
    temperature_c DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    air_quality_index INT,
    battery_pct INT,
    status TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    firmware TEXT,
    latency_ms INT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""

INSERT_SQL = """
INSERT INTO iot_readings (
  event_ts, device_id, building, floor, temperature_c, humidity_pct,
  air_quality_index, battery_pct, status, latitude, longitude, firmware, latency_ms
) VALUES %s
"""

def connect_pg(retries=30, wait=2):
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=PG_HOST, port=PG_PORT, dbname=PG_DB,
                user=PG_USER, password=PG_PASSWORD
            )
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(DDL)
            cur.close()
            conn.commit()
            return conn
        except Exception as e:
            print(f"[consumer] PG not ready ({e}), retry {i+1}/{retries}")
            time.sleep(wait)
    raise SystemExit("Postgres connection failed")

def connect_kafka(retries=30, wait=2):
    for i in range(retries):
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id=GROUP_ID,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                consumer_timeout_ms=10000
            )
            return consumer
        except Exception as e:
            print(f"[consumer] Kafka not ready ({e}), retry {i+1}/{retries}")
            time.sleep(wait)
    raise SystemExit("Kafka consumer failed")

def main():
    conn = connect_pg()
    kconsumer = connect_kafka()
    buffer = []

    while True:
        try:
            for msg in kconsumer:
                data = msg.value
                # 计算延迟（毫秒）
                now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                latency_ms = None
                if "producer_ts" in data:
                    latency_ms = max(0, now_ms - int(data["producer_ts"]))
                row = (
                    data["event_ts"],
                    data["device_id"],
                    data["building"],
                    data["floor"],
                    data.get("temperature_c"),
                    data.get("humidity_pct"),
                    data.get("air_quality_index"),
                    data.get("battery_pct"),
                    data.get("status"),
                    data.get("latitude"),
                    data.get("longitude"),
                    data.get("firmware"),
                    latency_ms
                )
                buffer.append(row)

                if len(buffer) >= BATCH_SIZE:
                    with conn.cursor() as cur:
                        execute_values(cur, INSERT_SQL, buffer)
                    buffer.clear()

            # flush if timeout
            if buffer:
                with conn.cursor() as cur:
                    execute_values(cur, INSERT_SQL, buffer)
                buffer.clear()

        except Exception as e:
            print(f"[consumer] error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    main()
