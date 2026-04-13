"""
Gemio ERP 自然語言查詢入口
FastAPI + HTMX + Bootstrap
"""
import io
import json
import os

import pandas as pd
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ai_service import nl_to_sql
from database import execute_query, get_schema, refresh_schema

app = FastAPI(title="Gemio ERP NL Query")

# ── Static & Templates ────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(__file__)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


# ── 啟動時載入 Schema ─────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup_event():
    print("[Startup] 載入資料庫 Schema ...")
    schema = get_schema()
    print(f"[Startup] 共載入 {len(schema)} 個資料表/VIEW")


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    schema = get_schema()
    tables = sorted(schema.keys())
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "tables": tables},
    )


class QueryRequest(BaseModel):
    query: str


@app.post("/api/query")
async def api_query(body: QueryRequest):
    """
    主要查詢端點：自然語言 → SQL → 執行 → 回傳結果
    """
    if not body.query.strip():
        raise HTTPException(status_code=400, detail="查詢內容不能為空")

    schema = get_schema()

    # 1. NL → SQL
    try:
        sql = await nl_to_sql(body.query, schema)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI 轉換失敗：{e}")

    # 2. 執行 SQL
    try:
        rows, columns = execute_query(sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SQL 執行失敗：{e}")

    # 3. 計算數字欄位總計
    totals = _calc_totals(rows, columns)

    return {
        "sql":     sql,
        "columns": columns,
        "rows":    rows,
        "totals":  totals,
        "count":   len(rows),
    }


@app.get("/api/export-excel")
async def export_excel(sql: str):
    """將最後一次查詢結果匯出為 Excel。"""
    try:
        rows, columns = execute_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    df = pd.DataFrame(rows, columns=columns)
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="查詢結果")
    output.seek(0)

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=gemio_result.xlsx"},
    )


@app.post("/api/refresh-schema")
async def api_refresh_schema():
    """強制重新載入 Schema。"""
    schema = refresh_schema()
    return {"message": "Schema 已更新", "tables": len(schema)}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _calc_totals(rows, columns) -> dict:
    """計算每個數字欄位的加總。"""
    if not rows:
        return {}
    totals = {}
    for i, col in enumerate(columns):
        try:
            vals = [r[i] for r in rows if r[i] is not None]
            numeric = [v for v in vals if isinstance(v, (int, float))]
            if numeric:
                totals[col] = round(sum(numeric), 4)
        except Exception:
            pass
    return totals
