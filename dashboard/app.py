# dashboard/app.py
import os
import time
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ✅ 1) 必须是脚本里的第一个 Streamlit 调用，且只调用一次
st.set_page_config(page_title="IoT Streaming Dashboard", layout="wide")

# ---- Env ----
PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "streamdb")
PG_USER = os.getenv("PG_USER", "stream")
PG_PASSWORD = os.getenv("PG_PASSWORD", "stream")
REFRESH_SEC = int(os.getenv("DASH_REFRESH_SEC", "2"))  # 自动刷新秒数

# ---- DB engine (缓存资源) ----
@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

# ---- 查询函数（2 秒缓存）----
@st.cache_data(ttl=2)
def load_recent(limit_rows=5000):
    # 每台设备最新一条
    q_latest = text("""
        SELECT DISTINCT ON (device_id)
               device_id, event_ts, temperature_c, humidity_pct, air_quality_index,
               battery_pct, status, latitude, longitude, building, floor, latency_ms
        FROM iot_readings
        ORDER BY device_id, event_ts DESC;
    """)
    df_latest = pd.read_sql(q_latest, engine)

    # 最近 30 分钟，按分钟 + 楼宇聚合（更平滑）
    q_ts = text("""
        SELECT date_trunc('minute', event_ts) AS ts_minute,
               building,
               AVG(temperature_c)      AS avg_temp,
               AVG(humidity_pct)       AS avg_hum,
               AVG(latency_ms)         AS avg_latency,
               COUNT(*)                AS events
        FROM iot_readings
        WHERE event_ts > NOW() - INTERVAL '30 minutes'
        GROUP BY ts_minute, building
        ORDER BY ts_minute
        LIMIT :limit_rows;
    """)
    df_ts = pd.read_sql(q_ts, engine, params={"limit_rows": limit_rows})

    return df_latest, df_ts

# ---- UI ----
st.title("🏢 IoT 实时楼宇监测（Kafka → PostgreSQL → Streamlit）")
st.caption(f"自动刷新：每 {REFRESH_SEC} 秒（可通过环境变量 DASH_REFRESH_SEC 调整）")

df_latest, df_ts = load_recent()

# 顶部 KPI
online_cnt = int((df_latest["status"] == "online").sum()) if not df_latest.empty else 0
events_30m = int(df_ts["events"].sum()) if not df_ts.empty else 0
avg_latency = int(df_ts["avg_latency"].mean()) if not df_ts.empty else 0
low_battery = int((df_latest["battery_pct"] <= 20).sum()) if not df_latest.empty else 0

k1, k2, k3, k4 = st.columns(4)
k1.metric("在线设备数", online_cnt)
k2.metric("最近30分钟事件数", events_30m)
k3.metric("平均延迟 (ms)", avg_latency)
k4.metric("低电量设备 (≤20%)", low_battery)

st.markdown("---")

# 左右布局：时序图 | 地图 + 状态
lc, rc = st.columns([2, 1])

with lc:
    st.subheader("按楼宇实时平均温湿度（最近 30 分钟）")
    if not df_ts.empty:
        temp_pivot = df_ts.pivot_table(index="ts_minute", columns="building", values="avg_temp", aggfunc="mean")
        hum_pivot  = df_ts.pivot_table(index="ts_minute", columns="building", values="avg_hum",  aggfunc="mean")
        st.line_chart(temp_pivot, height=260, use_container_width=True)
        st.line_chart(hum_pivot,  height=160, use_container_width=True)
    else:
        st.info("暂无数据，等待流入…")

with rc:
    st.subheader("设备地图（最新位置）")
    if not df_latest.empty and {"latitude", "longitude"}.issubset(df_latest.columns):
        st.map(
            df_latest.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]],
            use_container_width=True
        )
    else:
        st.info("暂无位置数据")

    st.subheader("状态分布（最新）")
    if not df_latest.empty and "status" in df_latest.columns:
        st.bar_chart(df_latest["status"].value_counts(), use_container_width=True)
    else:
        st.info("暂无状态数据")

st.markdown("---")
st.caption("数据流：Producer → Kafka → Consumer → PostgreSQL → Streamlit")

# ---- 简单自动刷新：在页面底部睡眠后重跑 ----
# （避免放在前面阻塞渲染；需要持续刷新就保持此循环）
time.sleep(REFRESH_SEC)
st.rerun()
