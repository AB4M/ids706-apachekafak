import os, json, time, random
from datetime import datetime, timezone
from faker import Faker
import numpy as np
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "iot_readings")
RATE = float(os.getenv("PRODUCER_RATE_PER_SEC", "20"))     # 每秒消息数
NUM_DEVICES = int(os.getenv("NUM_DEVICES", "50"))

faker = Faker()
random.seed(42)
np.random.seed(42)

def make_devices(n):
    devices = []
    for i in range(n):
        building = random.choice(list("ABC"))
        floor = random.randint(1, 6)
        lat = 36.0 + np.random.randn() * 0.01
        lon = -78.94 + np.random.randn() * 0.01
        devices.append({
            "device_id": f"sensor-bld{building}-{floor:01d}{i:02d}",
            "building": building,
            "floor": floor,
            "latitude": lat,
            "longitude": lon,
            "firmware": random.choice(["1.4.2", "1.5.0", "1.5.1"])
        })
    return devices

DEVICES = make_devices(NUM_DEVICES)

def gen_reading(device):
    base_temp = 23 + (ord(device["building"]) % 5)  # 不同楼轻微偏移
    temperature_c = np.clip(np.random.normal(base_temp, 1.2), 18, 35)
    humidity_pct = np.clip(np.random.normal(45, 8), 20, 80)
    aqi = int(np.clip(np.random.normal(60, 15), 10, 200))
    battery_pct = int(np.clip(np.random.normal(90, 8), 10, 100))
    status = random.choices(["online", "online", "online", "fault", "offline"],
                            weights=[80, 10, 5, 3, 2])[0]

    now = datetime.now(timezone.utc)
    producer_ts = int(now.timestamp() * 1000)

    return {
        "event_ts": now.isoformat(),
        "device_id": device["device_id"],
        "building": device["building"],
        "floor": device["floor"],
        "temperature_c": round(float(temperature_c), 2),
        "humidity_pct": round(float(humidity_pct), 2),
        "air_quality_index": aqi,
        "battery_pct": battery_pct,
        "status": status,
        "latitude": device["latitude"] + np.random.randn() * 0.0002,
        "longitude": device["longitude"] + np.random.randn() * 0.0002,
        "firmware": device["firmware"],
        "producer_ts": producer_ts
    }

def main():
    # 重试连接 Kafka，避免容器启动时序问题
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=50
            )
            break
        except Exception as e:
            print(f"[producer] Kafka not ready ({e}), retry {attempt+1}/30")
            time.sleep(2)
    else:
        raise SystemExit("Kafka connection failed")

    interval = 1.0 / RATE
    idx = 0
    while True:
        device = DEVICES[idx % NUM_DEVICES]
        msg = gen_reading(device)
        try:
            producer.send(TOPIC, msg)
        except Exception as e:
            print(f"[producer] send error: {e}")
        idx += 1
        time.sleep(interval)

if __name__ == "__main__":
    main()
