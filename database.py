"""
SQL Server 連線與 Schema 快取模組
- 啟動時讀取所有 VIEW / TABLE 定義並快取為 JSON
- 提供 execute_query() 執行 SQL 並回傳結果
"""
import json
import os
from pathlib import Path
from typing import Any

import pymssql
from dotenv import load_dotenv

load_dotenv()

DB_SERVER   = os.getenv("DB_SERVER",   "163.17.141.61:8000")
DB_NAME     = os.getenv("DB_NAME",     "gemio05")
DB_USER     = os.getenv("DB_USER",     "drcas")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")

SCHEMA_CACHE_PATH = Path(__file__).parent / "schema_cache.json"

# pymssql 用逗號當 port 分隔，需轉成 (host, port)
def _parse_server(server_str: str):
    if "," in server_str:
        host, port = server_str.split(",", 1)
        return host.strip(), int(port.strip())
    return server_str.strip(), 1433


def _get_conn():
    host, port = _parse_server(DB_SERVER)
    return pymssql.connect(
        server=host,
        port=port,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset="UTF-8",
    )


# ── Schema 載入 ─────────────────────────────────────────────────────────────

def load_schema_from_db() -> dict[str, list[dict]]:
    """從 SQL Server 讀取所有 VIEW 與 TABLE 的欄位定義。"""
    sql = """
    SELECT
        t.TABLE_NAME,
        t.TABLE_TYPE,
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.CHARACTER_MAXIMUM_LENGTH,
        c.IS_NULLABLE
    FROM INFORMATION_SCHEMA.TABLES  t
    JOIN INFORMATION_SCHEMA.COLUMNS c
        ON t.TABLE_NAME = c.TABLE_NAME
       AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
    WHERE t.TABLE_SCHEMA = 'dbo'
    ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
    """
    schema: dict[str, dict] = {}
    try:
        with _get_conn() as conn:
            with conn.cursor(as_dict=True) as cur:
                cur.execute(sql)
                for row in cur.fetchall():
                    tname = row["TABLE_NAME"]
                    if tname not in schema:
                        schema[tname] = {
                            "type": row["TABLE_TYPE"],
                            "columns": [],
                        }
                    schema[tname]["columns"].append({
                        "name":     row["COLUMN_NAME"],
                        "type":     row["DATA_TYPE"],
                        "nullable": row["IS_NULLABLE"],
                    })
    except Exception as e:
        print(f"[Schema] 連線失敗：{e}")
    return schema


def get_schema() -> dict:
    """取得 Schema（優先用快取，不存在時從 DB 載入並存檔）。"""
    if SCHEMA_CACHE_PATH.exists():
        with open(SCHEMA_CACHE_PATH, encoding="utf-8") as f:
            return json.load(f)
    schema = load_schema_from_db()
    save_schema_cache(schema)
    return schema


def save_schema_cache(schema: dict):
    with open(SCHEMA_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(schema, f, ensure_ascii=False, indent=2)


def refresh_schema() -> dict:
    """強制從 DB 重新載入 Schema 並更新快取。"""
    schema = load_schema_from_db()
    save_schema_cache(schema)
    return schema


# ── 查詢執行 ─────────────────────────────────────────────────────────────────

def execute_query(sql: str) -> tuple[list[list[Any]], list[str]]:
    """
    執行 SELECT SQL，回傳 (rows, columns)。
    rows:    list of list  (每筆資料)
    columns: list of str   (欄位名稱)
    """
    # 安全性：只允許 SELECT
    clean = sql.strip().lstrip(";").strip()
    if not clean.upper().startswith("SELECT"):
        raise ValueError("只允許執行 SELECT 陳述式")

    with _get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(clean)
            columns = [desc[0] for desc in cur.description]
            rows = [list(r) for r in cur.fetchall()]
    return rows, columns
