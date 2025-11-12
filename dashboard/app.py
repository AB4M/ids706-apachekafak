import os, pandas as pd, time
import streamlit as st
from sqlalchemy import create_engine, text

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "streamdb")
PG_USER = os.getenv("PG_USER", "stream")
PG_PASSWORD = os.getenv("PG_PASSWORD", "stream")

@st.cache_resource
def get_engine():
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    return create_engine(url, pool_pre_ping=True)

engine = get_engine()

st.set_page_config(page_title="IoT Streaming Dashboard", layout="wide")
st.title("🏢 IoT 实时楼宇监测（Kafka → Postgres → Streamlit）")

# 自动刷新（每 2s）
st_autorefresh = st.experimental_rerun if False else None
st.experimental_set_query_params(ts=int(time.time()))  # 防止浏览器缓存
st.sidebar.write("⏱ 自动刷新：每 2 秒")

@st.cache_data(ttl=2)
def load_recent(limit_rows=5000):
    q = text("""
        WITH latest AS (
            SELECT DISTINCT ON (device_id)
                device_id, event_ts, temperature_c, humidity_pct, air_quality_index,
                battery_pct, status, latitude, longitude, building, floor, latency_ms
            FROM iot_readings
            ORDER BY device_id, event_ts DESC
        )
        SELECT * FROM latest;
    """)
    df_latest = pd.read_sql(q, engine)

    q2 = text("""
        SELECT event_ts, building, AVG(temperature_c) AS avg_temp,
               AVG(humidity_pct) AS avg_hum, AVG(latency_ms) AS avg_latency,
               COUNT(*) AS events
        FROM iot_readings
        WHERE event_ts > NOW() - INTERVAL '30 minutes'
        GROUP BY event_ts, building
        ORDER BY event_ts DESC
        LIMIT :limit_rows
    """)
    df_timeseries = pd.read_sql(q2, engine, params={"limit_rows": limit_rows})
    return df_latest, df_timeseries

df_latest, df_ts = load_recent()

# 顶部 KPI
col1, col2, col3, col4 = st.columns(4)
col1.metric("在线设备数", int((df_latest["status"] == "online").sum()))
col2.metric("最近30分钟事件数", int(df_ts["events"].sum()) if not df_ts.empty else 0)
col3.metric("平均延迟(ms)", int(df_ts["avg_latency"].mean()) if not df_ts.empty else 0)
col4.metric("低电量(≤20%)", int((df_latest["battery_pct"] <= 20).sum()))

st.markdown("---")

# 左：时序图；右：地图与状态分布
lc, rc = st.columns([2, 1])

with lc:
    st.subheader("按楼实时平均温湿度（最近 30 分钟）")
    if not df_ts.empty:
        # 转宽表方便绘图
        temp_pivot = df_ts.pivot_table(index="event_ts", columns="building", values="avg_temp", aggfunc="mean")
        hum_pivot = df_ts.pivot_table(index="event_ts", columns="building", values="avg_hum", aggfunc="mean")
        st.line_chart(temp_pivot.sort_index(), height=260)
        st.line_chart(hum_pivot.sort_index(), height=160)
    else:
        st.info("暂无数据，等待流入…")

with rc:
    st.subheader("设备地图（最新位置）")
    if not df_latest.empty and {"latitude","longitude"}.issubset(df_latest.columns):
        st.map(df_latest.rename(columns={"latitude":"lat","longitude":"lon"})[["lat","lon"]], use_container_width=True)
    else:
        st.info("暂无位置数据")

    st.subheader("状态分布")
    st.bar_chart(df_latest["status"].value_counts())

st.markdown("---")
st.caption("数据流：Producer → Kafka → Consumer → PostgreSQL → Streamlit（2 秒自动刷新）")
