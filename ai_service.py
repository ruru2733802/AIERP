"""
Claude AI 自然語言 → SQL 轉換服務
- 將相關 Schema 注入 prompt
- 回傳乾淨的 SELECT SQL
"""
import os
import re

import anthropic
from dotenv import load_dotenv

load_dotenv()

_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

MODEL = "claude-sonnet-4-6"
MAX_TOKENS = 1024


def _build_schema_text(schema: dict) -> str:
    """把 Schema dict 轉成文字說明供 prompt 使用。"""
    lines = []
    for tname, info in schema.items():
        cols = ", ".join(
            f"{c['name']}({c['type']})" for c in info["columns"]
        )
        ttype = "VIEW" if "VIEW" in info.get("type", "") else "TABLE"
        lines.append(f"[{ttype}] {tname}: {cols}")
    return "\n".join(lines)


def _extract_sql(text: str) -> str:
    """從 AI 回應中擷取第一段 SQL。"""
    # 嘗試抓 ```sql ... ``` 或 ``` ... ```
    m = re.search(r"```(?:sql)?\s*([\s\S]+?)```", text, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # 沒有 fence，直接取整段
    return text.strip()


async def nl_to_sql(nl_query: str, schema: dict) -> str:
    """
    將自然語言轉為 SQL。
    回傳乾淨的 SQL 字串。
    """
    schema_text = _build_schema_text(schema)

    system_prompt = f"""你是一個專業的 SQL Server 查詢助手，幫助使用者將繁體中文自然語言問題轉換為精確的 T-SQL SELECT 語句。

規則：
1. 只產生 SELECT 陳述式，不得產生 INSERT / UPDATE / DELETE / DROP。
2. 資料庫為 gemio05，使用 dbo schema。
3. 只能使用下方列出的資料表或 VIEW。
4. 欄位名稱請使用 schema 中的原始名稱。
5. 若需要排序，預設用第一欄 ASC。
6. 只回傳 SQL，用 ```sql ``` 包裹，不要解釋。

=== 可用的資料表與欄位 ===
{schema_text}
"""

    message = _client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=system_prompt,
        messages=[{"role": "user", "content": nl_query}],
    )

    raw = message.content[0].text
    return _extract_sql(raw)
