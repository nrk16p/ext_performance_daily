# =========================================================
# PERFORMANCE PIPELINE - JENKINS READY (LAST 3 MONTHS)
# Auth: session login (username/password) - no static cookie
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

ATMS_BASE = "https://www.mena-atms.com"
LOGIN_URL = f"{ATMS_BASE}/account/user/login"
REPORT_URL = f"{ATMS_BASE}/report/excel/index.excel/type/vehicle.availability"

# เก็บเป็น Jenkins Credentials แล้วผูกผ่าน withCredentials (อย่า hardcode / อย่าใช้ env ธรรมดา)
ATMS_USERNAME = os.environ["ATMS_USERNAME"]
ATMS_PASSWORD = os.environ["ATMS_PASSWORD"]

MYSQL_USER = os.environ["MYSQL_USER"]
MYSQL_PASSWORD = quote_plus(os.environ["MYSQL_PASSWORD"])
MYSQL_HOST = os.environ["MYSQL_HOST"]
MYSQL_PORT = os.environ.get("MYSQL_PORT", "3306")
MYSQL_DB = os.environ["MYSQL_DB"]

MYSQL_TABLE = "performance_vehicle_daily"
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "3000"))

# FIX: str(m) ไม่ใช่ f"{m:2d}" (ตัวหลังเติม space นำหน้า ทำให้ server ไม่รู้จัก fleet)
fleet_id_list = [str(m) for m in range(1, 9)]

# headers ทั่วไป (ไม่มี Cookie แล้ว - session จัดการ cookie ให้เอง)
BASE_HEADERS = {
    "Accept": (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,"
        "application/octet-stream;q=0.9,*/*;q=0.8"
    ),
    "Origin": ATMS_BASE,
    "Referer": REPORT_URL,
    "User-Agent": "Mozilla/5.0",
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
# AUTH (SESSION LOGIN)
# ---------------------------------------------------------

def atms_login() -> requests.Session:
    session = requests.Session()
    session.headers.update(BASE_HEADERS)

    resp = session.post(
        LOGIN_URL,
        data={
            "username": ATMS_USERNAME,
            "password": ATMS_PASSWORD,
            "submit": "login",
            "next": "",
        },
        verify=False,
        timeout=30,
        allow_redirects=True,
    )
    resp.raise_for_status()

    # เช็คว่า login สำเร็จจริง: หน้า login ที่ผิดก็คืน 200 ได้ จึงต้องเช็คเพิ่ม
    # หลัง login สำเร็จ server จะตั้ง session cookie ให้ และไม่พาเรากลับมาหน้า login
    if not session.cookies:
        raise RuntimeError("ATMS login failed: no session cookie set (check username/password)")
    if "login" in resp.url:
        raise RuntimeError(f"ATMS login failed: still on login page ({resp.url})")

    logging.info("✅ ATMS login OK")
    return session

# ---------------------------------------------------------
# DOWNLOAD
# ---------------------------------------------------------

def download_report_bytes(session: requests.Session, fleet_group_id, year, month):
    r = session.post(
        REPORT_URL,
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

    # ยึด magic bytes "PK" (zip header ของ .xlsx) เป็นเกณฑ์หลัก
    if not r.content.startswith(b"PK"):
        ct = r.headers.get("Content-Type", "")
        # ถ้าโดน redirect กลับหน้า login = session หลุด/ดาวน์โหลดนานเกินจน session หมดอายุ
        if "login" in r.url:
            raise RuntimeError(f"Session expired - redirected to login ({r.url})")
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

    # login ครั้งเดียว ใช้ session ซ้ำทุก fleet+month
    session = atms_login()

    # ดาวน์โหลดให้ครบก่อน แล้วค่อยลบ-แทนที่ (run ล้มเหลว ข้อมูลเดิมไม่หาย)
    raw = []
    for year, month in target_months:
        for fleet in fleet_id_list:
            try:
                content = download_report_bytes(session, fleet, year, month)
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

    # ลบเฉพาะตอนที่ดาวน์โหลด + transform สำเร็จแล้วเท่านั้น
    delete_target_months(engine, target_months)
    upsert_chunked(final_df, engine)

    logging.info("🎉 Jenkins job finished successfully")

# ---------------------------------------------------------

if __name__ == "__main__":
    main()
