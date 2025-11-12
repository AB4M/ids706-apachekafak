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

CREATE INDEX IF NOT EXISTS idx_iot_ts ON iot_readings (event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_iot_device ON iot_readings (device_id);
