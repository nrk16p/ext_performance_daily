# =========================================================
# PERFORMANCE PIPELINE - JENKINS READY (LAST 3 MONTHS)
# =========================================================

import os
import sys
import logging
import requests
import urllib3
import pandas as pd

from io import BytesIO
from datetime import date, datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------
# BASIC SETUP
# ---------------------------------------------------------

sys.stdout.reconfigure(encoding="utf-8")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ---------------------------------------------------------
# CONFIG (FROM ENV)
# ---------------------------------------------------------

BASE_URL = "https://www.mena-atms.com/report/excel/index.excel/type/vehicle.availability"

COOKIE_STRING = os.environ["ATMS_COOKIE"]

MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = quote_plus(os.environ["MYSQL_PASSWORD"])
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DB = os.environ["MYSQL_DB"]

MYSQL_TABLE = "performance_vehicle_daily"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "3000"))

# DEBUG ชั่วคราว: print body ของ response เฉพาะ failure ครั้งแรก
_DEBUG_DONE = False

# FIX: ใช้ str(m) ตรงๆ ไม่ใช่ f"{m:2d}" ซึ่งเติม space นำหน้า (" 1", " 2", ...)
# space ที่ติดมาทำให้ server ไม่รู้จัก fleet_group_id แล้วคืน HTML แทน Excel
fleet_id_list = [str(m) for m in range(1, 9)]

HEADERS = {
    "Accept": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
        "application/octet-stream;q=0.9,*/*;q=0.8"
    ),
    "Content-Type": "application/x-www-form-urlencoded",
    "Origin": "https://www.mena-atms.com",
    "Referer": "https://www.mena-atms.com/report/excel/index.excel/type/vehicle.availability",
    "User-Agent": "Mozilla/5.0",
    "Cookie": COOKIE_STRING,
}

# ---------------------------------------------------------
# TIME WINDOW (LAST 3 MONTHS)
# ---------------------------------------------------------

def get_last_n_months(n=3):
    base = date.today().replace(day=1)
    return [
        (str((base - relativedelta(months=i)).year),
         f"{(base - relativedelta(months=i)).month:02d}")
        for i in range(n)
    ]

# ---------------------------------------------------------
# DOWNLOAD
# ---------------------------------------------------------

def download_report_bytes(fleet_group_id, year, month):
    r = requests.post(
        BASE_URL,
        headers=HEADERS,
        data={
            "fleet_group_id": fleet_group_id,
            "fleet_id": "",
            "year": year,
            "month": month,
            "is_tail": "n",
            "report_type": "vehicle.availability",
            "submit": "พิมพ์",
        },
        timeout=60,
        verify=False,
    )

    r.raise_for_status()

    # FIX: ยึด magic bytes "PK" (zip header ของ .xlsx) เป็นเกณฑ์หลัก
    # เชื่อถือได้กว่าการเช็ค Content-Type เพราะบาง server คืน Excel มาด้วย
    # application/vnd.ms-excel หรือ application/octet-stream
    if not r.content.startswith(b"PK"):
        ct = r.headers.get("Content-Type", "")
        # DEBUG ชั่วคราว: ดู body ของ HTML ที่ server คืนมา (เฉพาะครั้งแรก)
        global _DEBUG_DONE
        if not _DEBUG_DONE:
            _DEBUG_DONE = True
            logging.warning(f"--- DEBUG fleet={fleet_group_id} {year}-{month} ---")
            logging.warning(f"STATUS: {r.status_code}")
            logging.warning(f"REQUEST URL: {r.url}")
            logging.warning(f"RESP HEADERS: {dict(r.headers)}")
            logging.warning(f"BODY[:1500]: {r.text[:1500]}")
        raise RuntimeError(f"Not Excel response (Content-Type={ct})")

    return r.content


def bytes_to_df(content: bytes) -> pd.DataFrame:
    return pd.read_excel(BytesIO(content), header=1, engine="openpyxl")

# ---------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------

STATUS_MAP = {
    "working": ["A", "Aท", "A75", "A50", "A25", "AX"],
    "break": ["B", "BA", "BAQ", "BY"],
    "accident": ["อ"],
    "recruit": ["ว"],
    "leave": ["วล", "วก", "วป", "วข", "วส"],
    "lose": ["X"],
    "temporary": ["วพ"],
}

def map_group_status(s):
    for k, v in STATUS_MAP.items():
        if s in v:
            return k
    return "unknown"


def transform_performance(df):
    day_cols = [c for c in df.columns if str(c).isdigit()]
    id_vars = ["ลูกค้า", "แพล้นท", "ทะเบียนรถ", "fleet_group_id", "year", "month"]

    df = df.melt(
        id_vars=id_vars,
        value_vars=day_cols,
        var_name="day",
        value_name="status",
    ).dropna(subset=["status"])

    df["date"] = pd.to_datetime(
        df["year"] + "-" + df["month"] + "-" + df["day"].astype(str),
        errors="coerce",
    )

    df = df[df["date"] <= pd.Timestamp.today().normalize()]
    df["month_year"] = df["date"].dt.strftime("%m-%y")
    df["group_status"] = df["status"].apply(map_group_status)

    return df

# ---------------------------------------------------------
# MYSQL
# ---------------------------------------------------------

def mysql_engine():
    return create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
        f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}",
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def delete_target_months(engine, months):
    with engine.begin() as conn:
        for y, m in months:
            ym = f"{m}-{y[-2:]}"
            conn.execute(
                text(f"DELETE FROM {MYSQL_TABLE} WHERE month_year = :ym"),
                {"ym": ym},
            )
            logging.info(f"🧹 deleted {ym}")


def upsert_chunked(df, engine):
    sql = f"""
    INSERT INTO {MYSQL_TABLE} (
        fleet_group_id, license_plate, plant, customer,
        status, group_status, date, month_year
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON DUPLICATE KEY UPDATE
        status = VALUES(status),
        group_status = VALUES(group_status)
    """

    with engine.begin() as conn:
        for i in range(0, len(df), CHUNK_SIZE):
            batch = df.iloc[i : i + CHUNK_SIZE]
            data = [
                (
                    r["fleet_group_id"],
                    r["ทะเบียนรถ"],
                    r["แพล้นท"],
                    r["ลูกค้า"],
                    r["status"],
                    r["group_status"],
                    r["date"].date(),
                    r["month_year"],
                )
                for _, r in batch.iterrows()
            ]
            conn.exec_driver_sql(sql, data)
            logging.info(f"⬆️ inserted {i:,}")

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    logging.info("🚀 Jenkins performance job started")

    engine = mysql_engine()
    target_months = get_last_n_months(3)

    logging.info(f"Target months: {target_months}")

    # ดาวน์โหลดให้ครบก่อน แล้วค่อยลบ-แทนที่ เพื่อไม่ให้ข้อมูลเดิมหาย
    # ถ้า run ล้มเหลว (เช่น cookie หมดอายุ) ข้อมูลเดือนเป้าหมายจะยังอยู่
    raw = []

    for year, month in target_months:
        for fleet in fleet_id_list:
            try:
                content = download_report_bytes(fleet, year, month)
                df = bytes_to_df(content)

                df["fleet_group_id"] = fleet.strip()
                df["year"] = year
                df["month"] = month

                raw.append(df)
                logging.info(f"OK fleet={fleet} {year}-{month}")

            except Exception as e:
                logging.warning(f"Skip fleet={fleet} {year}-{month}: {e}")

    if not raw:
        raise RuntimeError("No data downloaded")

    raw_df = pd.concat(raw, ignore_index=True)
    logging.info(f"raw rows: {len(raw_df):,}")

    final_df = transform_performance(raw_df)
    logging.info(f"final rows: {len(final_df):,}")

    # ลบเฉพาะตอนที่ดาวน์โหลดและ transform สำเร็จแล้วเท่านั้น
    delete_target_months(engine, target_months)

    upsert_chunked(final_df, engine)

    logging.info("🎉 Jenkins job finished successfully")

# ---------------------------------------------------------

if __name__ == "__main__":
    main()
