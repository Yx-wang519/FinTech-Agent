# Auto-generated backend from notebook for Streamlit app
from __future__ import annotations


# ==== From notebook cell 3 ====
import os
from pathlib import Path
from dotenv import load_dotenv
import os, json, time, sqlite3, requests, textwrap
import pandas as pd
import yfinance as yf
from dataclasses import dataclass, field
from openai import OpenAI

# project root = current file directory
BASE_DIR = Path(__file__).resolve().parent

# 先尝试从 Streamlit secrets 读取；如果失败，再退回 .env
OPENAI_API_KEY = ""
ALPHAVANTAGE_API_KEY = ""

try:
    import streamlit as st
    OPENAI_API_KEY = st.secrets.get("OPENAI_API_KEY", "")
    ALPHAVANTAGE_API_KEY = st.secrets.get("ALPHAVANTAGE_API_KEY", "")
except Exception:
    pass

if not OPENAI_API_KEY or not ALPHAVANTAGE_API_KEY:
    load_dotenv(BASE_DIR / ".env")
    OPENAI_API_KEY = OPENAI_API_KEY or os.getenv("OPENAI_API_KEY", "")
    ALPHAVANTAGE_API_KEY = ALPHAVANTAGE_API_KEY or os.getenv("ALPHAVANTAGE_API_KEY", "")

MODEL_SMALL  = "gpt-4o-mini"
MODEL_LARGE  = "gpt-4o"
ACTIVE_MODEL = MODEL_SMALL          # switch to MODEL_LARGE for the second run

if not OPENAI_API_KEY:
    raise ValueError("OPENAI_API_KEY is missing. Set it in Streamlit secrets or .env")

if not ALPHAVANTAGE_API_KEY:
    raise ValueError("ALPHAVANTAGE_API_KEY is missing. Set it in Streamlit secrets or .env")
client = OpenAI(api_key=OPENAI_API_KEY)
print(f"✅ Ready  |  active model: {ACTIVE_MODEL}")

# ==== From notebook cell 6 ====
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str(BASE_DIR / "stocks.db")
CSV_PATH = str(BASE_DIR / "sp500_companies.csv")

def create_local_database(csv_path: str = CSV_PATH):
    if not os.path.exists(csv_path):
        raise FileNotFoundError(
            f"'{csv_path}' not found.\n"
            "Download from: https://www.kaggle.com/datasets/andrewmvd/sp-500-stocks"
        )
    df = pd.read_csv(csv_path)
    df.columns = df.columns.str.strip().str.lower()
    df = df.rename(columns={
        "symbol":"ticker", "shortname":"company",
        "sector":"sector",  "industry":"industry",
        "exchange":"exchange", "marketcap":"market_cap_raw"
    })
    def cap_bucket(v):
        try:
            v = float(v)
            return "Large" if v >= 10_000_000_000 else "Mid" if v >= 2_000_000_000 else "Small"
        except: return "Unknown"
    df["market_cap"] = df["market_cap_raw"].apply(cap_bucket)
    df = (df.dropna(subset=["ticker","company"])
            .drop_duplicates(subset=["ticker"])
            [["ticker","company","sector","industry","market_cap","exchange"]])
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("stocks", conn, if_exists="replace", index=False)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ticker ON stocks(ticker)")
    conn.commit()
    n = pd.read_sql_query("SELECT COUNT(*) AS n FROM stocks", conn).iloc[0]["n"]
    print(f"✅ {n} companies loaded into stocks.db")
    print("\nDistinct sector values stored in DB:")
    print(pd.read_sql_query("SELECT DISTINCT sector FROM stocks ORDER BY sector", conn).to_string(index=False))
    conn.close()

if not os.path.exists(DB_PATH):
    create_local_database(CSV_PATH)

# ==== From notebook cell 8 ====
# ── Tool 1 ── Provided ────────────────────────────────────────
def get_price_performance(tickers: list, period: str = "1y") -> dict:
    """
    % price change for a list of tickers over a period.
    Valid periods: '1mo', '3mo', '6mo', 'ytd', '1y'
    Returns: { TICKER: {start_price, end_price, pct_change, period} }
    """
    results = {}
    for ticker in tickers:
        try:
            data = yf.download(ticker, period=period, progress=False, auto_adjust=True)
            if data.empty:
                results[ticker] = {"error": "No data — possibly delisted"}
                continue
            start = float(data["Close"].iloc[0].item())
            end   = float(data["Close"].iloc[-1].item())
            results[ticker] = {
                "start_price": round(start, 2),
                "end_price"  : round(end,   2),
                "pct_change" : round((end - start) / start * 100, 2),
                "period"     : period,
            }
        except Exception as e:
            results[ticker] = {"error": str(e)}
    return results

# ── Tool 2 ── Provided ────────────────────────────────────────
def get_market_status() -> dict:
    """Open / closed status for global stock exchanges."""
    return requests.get(
        f"https://www.alphavantage.co/query?function=MARKET_STATUS"
        f"&apikey={ALPHAVANTAGE_API_KEY}", timeout=10
    ).json()

# ── Tool 3 ── Provided ────────────────────────────────────────
def get_top_gainers_losers() -> dict:
    """Today's top gaining, top losing, and most active tickers."""
    return requests.get(
        f"https://www.alphavantage.co/query?function=TOP_GAINERS_LOSERS"
        f"&apikey={ALPHAVANTAGE_API_KEY}", timeout=10
    ).json()

# ── Tool 4 ── Provided ────────────────────────────────────────
def get_news_sentiment(ticker: str, limit: int = 5) -> dict:
    """
    Latest headlines + Bullish / Bearish / Neutral sentiment for a ticker.
    Returns: { ticker, articles: [{title, source, sentiment, score}] }
    """
    data = requests.get(
        f"https://www.alphavantage.co/query?function=NEWS_SENTIMENT"
        f"&tickers={ticker}&limit={limit}&apikey={ALPHAVANTAGE_API_KEY}", timeout=10
    ).json()
    return {
        "ticker": ticker,
        "articles": [
            {
                "title"    : a.get("title"),
                "source"   : a.get("source"),
                "sentiment": a.get("overall_sentiment_label"),
                "score"    : a.get("overall_sentiment_score"),
            }
            for a in data.get("feed", [])[:limit]
        ],
    }

# ── Tool 5 ── Provided ────────────────────────────────────────
def query_local_db(sql: str) -> dict:
    """
    Run any SQL SELECT on stocks.db.
    Table 'stocks' columns: ticker, company, sector, industry, market_cap, exchange
    market_cap values: 'Large' | 'Mid' | 'Small'
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        df   = pd.read_sql_query(sql, conn)
        conn.close()
        return {"columns": list(df.columns), "rows": df.to_dict(orient="records")}
    except Exception as e:
        return {"error": str(e)}

print("✅ 5 provided tools ready")

# ==== From notebook cell 10 ====
# ── Tool 6 — YOUR IMPLEMENTATION ─────────────────────────────
def get_company_overview(ticker: str) -> dict:
    ticker = ticker.strip().upper()
    try:
        data = requests.get(
            f"https://www.alphavantage.co/query?function=OVERVIEW"
            f"&symbol={ticker}&apikey={ALPHAVANTAGE_API_KEY}",
            timeout=10
        ).json()

        if "Name" not in data or not data.get("Name"):
            return {"error": f"No overview data for {ticker}"}

        return {
            "ticker": ticker,
            "name": data.get("Name", ""),
            "sector": data.get("Sector", ""),
            "pe_ratio": data.get("PERatio", ""),
            "eps": data.get("EPS", ""),
            "market_cap": data.get("MarketCapitalization", ""),
            "52w_high": data.get("52WeekHigh", ""),
            "52w_low": data.get("52WeekLow", ""),
        }
    except Exception:
        return {"error": f"No overview data for {ticker}"}


# ── Tool 7 — YOUR IMPLEMENTATION ─────────────────────────────
def get_tickers_by_sector(sector: str) -> dict:
    sector = sector.strip()

    # handle common aliases so the test can pass even if DB stores "Technology"
    sector_aliases = {
        "information technology": ["Information Technology", "Technology"],
        "technology": ["Technology", "Information Technology"],
        "financials": ["Financial Services"],
        "health care": ["Healthcare"],
        "consumer staples": ["Consumer Defensive"],
        "consumer discretionary": ["Consumer Cyclical"],
        "telecommunication services": ["Communication Services"],
    }

    candidates = sector_aliases.get(sector.lower(), [sector])

    try:
        conn = sqlite3.connect(DB_PATH)

        # 1) exact match on sector column (case-insensitive), including aliases
        placeholders = ",".join(["?"] * len(candidates))
        sql_exact = f"""
            SELECT ticker, company, industry
            FROM stocks
            WHERE LOWER(sector) IN ({placeholders})
            ORDER BY ticker
        """
        df = pd.read_sql_query(sql_exact, conn, params=[c.lower() for c in candidates])

        # 2) if no results, fallback to LIKE on industry column
        if df.empty:
            sql_fallback = """
                SELECT ticker, company, industry
                FROM stocks
                WHERE LOWER(industry) LIKE ?
                ORDER BY ticker
            """
            df = pd.read_sql_query(sql_fallback, conn, params=[f"%{sector.lower()}%"])

        conn.close()

        return {
            "sector": sector,
            "stocks": df.to_dict(orient="records")
        }
    except Exception as e:
        return {
            "sector": sector,
            "stocks": [],
            "error": str(e)
        }


# ==== From notebook cell 12 ====
def _s(name, desc, props, req):
    return {"type":"function","function":{
        "name":name,"description":desc,
        "parameters":{"type":"object","properties":props,"required":req}}}

SCHEMA_TICKERS  = _s("get_tickers_by_sector",
    "Return all stocks in a sector or industry from the local database. "
    "Use broad sector names ('Information Technology', 'Energy') or sub-sectors ('semiconductor', 'insurance').",
    {"sector":{"type":"string","description":"Sector or industry name"}}, ["sector"])

SCHEMA_PRICE    = _s("get_price_performance",
    "Get % price change for a list of tickers over a time period. "
    "Periods: '1mo','3mo','6mo','ytd','1y'.",
    {"tickers":{"type":"array","items":{"type":"string"}},
     "period":{"type":"string","default":"1y"}}, ["tickers"])

SCHEMA_OVERVIEW = _s("get_company_overview",
    "Get fundamentals for one stock: P/E ratio, EPS, market cap, 52-week high and low.",
    {"ticker":{"type":"string","description":"Ticker symbol e.g. 'AAPL'"}}, ["ticker"])

SCHEMA_STATUS   = _s("get_market_status",
    "Check whether global stock exchanges are currently open or closed.", {}, [])

SCHEMA_MOVERS   = _s("get_top_gainers_losers",
    "Get today's top gaining, top losing, and most actively traded stocks.", {}, [])

SCHEMA_NEWS     = _s("get_news_sentiment",
    "Get latest news headlines and Bullish/Bearish/Neutral sentiment scores for a stock.",
    {"ticker":{"type":"string"},"limit":{"type":"integer","default":5}}, ["ticker"])

SCHEMA_SQL      = _s("query_local_db",
    "Run a SQL SELECT on stocks.db. "
    "Table 'stocks': ticker, company, sector, industry, market_cap (Large/Mid/Small), exchange.",
    {"sql":{"type":"string","description":"A valid SQL SELECT statement"}}, ["sql"])

# All 7 schemas in one list — used by single agent
ALL_SCHEMAS = [SCHEMA_TICKERS, SCHEMA_PRICE, SCHEMA_OVERVIEW,
               SCHEMA_STATUS, SCHEMA_MOVERS, SCHEMA_NEWS, SCHEMA_SQL]

# Dispatch map — maps the tool name string the LLM returns → the Python function to call
ALL_TOOL_FUNCTIONS = {
    "get_tickers_by_sector" : get_tickers_by_sector,
    "get_price_performance"  : get_price_performance,
    "get_company_overview"   : get_company_overview,
    "get_market_status"      : get_market_status,
    "get_top_gainers_losers" : get_top_gainers_losers,
    "get_news_sentiment"     : get_news_sentiment,
    "query_local_db"         : query_local_db,
}

print("✅ Schemas ready")
print(f"   Tools available: {list(ALL_TOOL_FUNCTIONS.keys())}")

# ==== From notebook cell 14 ====
@dataclass
class AgentResult:
    agent_name   : str
    answer       : str
    tools_called : list  = field(default_factory=list)   # tool names in order called
    raw_data     : dict  = field(default_factory=dict)   # tool name → raw tool output
    confidence   : float = 0.0                           # set by evaluator / critic
    issues_found : list  = field(default_factory=list)   # set by evaluator / critic
    reasoning    : str   = ""

    def summary(self):
        print(f"\n{'─'*54}")
        print(f"Agent      : {self.agent_name}")
        print(f"Tools used : {', '.join(self.tools_called) or 'none'}")
        print(f"Confidence : {self.confidence:.0%}")
        if self.issues_found:
            print(f"Issues     : {'; '.join(self.issues_found)}")
        print(f"Answer     :\n{textwrap.indent(self.answer[:500], '  ')}")

print("✅ AgentResult defined")

# ==== From notebook cell 15 ====
def run_specialist_agent(
    agent_name   : str,
    system_prompt: str,
    task         : str,
    tool_schemas : list,
    max_iters    : int  = 8,
    verbose      : bool = True,
) -> AgentResult:
    """
    Core agentic loop used by every agent in this project.

    How it works:
      1. Sends system_prompt + task to the LLM
      2. If the LLM requests a tool call → looks up the function in ALL_TOOL_FUNCTIONS,
         executes it, appends the result to the message history, loops back to step 1
      3. When the LLM produces a response with no tool calls → returns an AgentResult

    Parameters
    ----------
    agent_name    : display name for logging
    system_prompt : the agent's persona, rules, and focus area
    task          : the specific question or sub-task for this agent
    tool_schemas  : list of schema dicts this agent is allowed to use
                    (pass [] for no tools — used by baseline)
    max_iters     : hard cap on iterations to prevent infinite loops
    verbose       : print each tool call as it happens
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": task},
    ]

    tools_called = []
    raw_data = {}

    for step in range(max_iters):
        try:
            response = client.chat.completions.create(
                model=ACTIVE_MODEL,
                messages=messages,
                tools=tool_schemas if tool_schemas else None,
                tool_choice="auto" if tool_schemas else None,
            )
        except Exception as e:
            return AgentResult(
                agent_name=agent_name,
                answer=f"LLM call failed: {str(e)}",
                tools_called=tools_called,
                raw_data=raw_data,
                issues_found=["llm_call_failed"],
            )

        msg = response.choices[0].message

        # If the model wants to call tool(s)
        if hasattr(msg, "tool_calls") and msg.tool_calls:
            messages.append(msg)

            for tool_call in msg.tool_calls:
                tool_name = tool_call.function.name

                try:
                    tool_args = json.loads(tool_call.function.arguments or "{}")
                except Exception:
                    tool_args = {}

                if verbose:
                    print(f"[{agent_name}] Step {step+1}: calling {tool_name}({tool_args})")

                tools_called.append(tool_name)

                if tool_name not in ALL_TOOL_FUNCTIONS:
                    tool_result = {"error": f"Unknown tool: {tool_name}"}
                else:
                    try:
                        tool_result = ALL_TOOL_FUNCTIONS[tool_name](**tool_args)
                    except Exception as e:
                        tool_result = {"error": str(e)}

                raw_data[f"{tool_name}_{len(tools_called)}"] = tool_result

                messages.append({
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": json.dumps(tool_result),
                })

            continue

        # Otherwise this is the final answer
        final_answer = msg.content if msg.content else ""
        return AgentResult(
            agent_name=agent_name,
            answer=final_answer,
            tools_called=tools_called,
            raw_data=raw_data,
        )

    return AgentResult(
        agent_name=agent_name,
        answer="Stopped before reaching a final answer.",
        tools_called=tools_called,
        raw_data=raw_data,
        issues_found=["max_iters_reached"],
    )

print("✅ run_specialist_agent ready")

# ==== From notebook cell 18 ====
def run_baseline(question: str, verbose: bool = True) -> AgentResult:
    """
    Single LLM call with no tools.
    This is the control condition / baseline.
    """
    system_prompt = (
        "You are a helpful stock-market assistant answering without any external tools. "
        "Answer as best as you can from general knowledge only. "
        "Do not claim to have checked live data. "
        "If you are uncertain, say so briefly and avoid overconfidence."
    )

    try:
        response = client.chat.completions.create(
            model=ACTIVE_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": question},
            ],
        )
        answer = response.choices[0].message.content or ""

        if verbose:
            print("[Baseline] Single LLM call completed (no tools used)")

        return AgentResult(
            agent_name="Baseline",
            answer=answer,
            tools_called=[],
            raw_data={},
        )

    except Exception as e:
        return AgentResult(
            agent_name="Baseline",
            answer=f"Baseline LLM call failed: {str(e)}",
            tools_called=[],
            raw_data={},
            issues_found=["baseline_llm_call_failed"],
        )


# ==== From notebook cell 20 ====
# ── YOUR SINGLE AGENT IMPLEMENTATION ──────────────────────────

SINGLE_AGENT_PROMPT = """
You are a single stock-analysis agent with access to 7 tools.

Your job:
- Answer stock-market questions accurately using tools when needed.
- You may use multiple tools in sequence.
- Base your final answer on tool outputs, not guesses.

Important rules:
1. Do not fabricate live or company-specific facts.
2. If a tool returns an error or empty result, say that clearly.
3. For sector / industry lookups, use get_tickers_by_sector or query_local_db.
4. For company fundamentals like P/E, EPS, market cap, 52-week high/low, use get_company_overview.
5. For price performance questions, use get_price_performance.
6. For latest news / sentiment, use get_news_sentiment.
7. For market open/closed questions, use get_market_status.
8. For top gainers / losers / most active, use get_top_gainers_losers.
9. Use query_local_db only for SQL SELECT queries on the local stocks database.

Multi-step reasoning guidance:
- If the question asks about a sector's best/worst performers, first get the tickers in that sector, then fetch price data.
- If the question compares multiple stocks, gather all needed data before answering.
- If the question asks for rankings like top 3, compute the ranking from the tool results and explain briefly.

Output style:
- Be concise but clear.
- Include the main conclusion first.
- Mention uncertainty briefly when needed.
"""

def run_single_agent(question: str, verbose: bool = True) -> AgentResult:
    return run_specialist_agent(
        agent_name="Single Agent",
        system_prompt=SINGLE_AGENT_PROMPT,
        task=question,
        tool_schemas=ALL_SCHEMAS,
        max_iters=10,
        verbose=verbose,
    )

# ==== From notebook cell 28 ====
# ── YOUR MULTI-AGENT IMPLEMENTATION ──────────────────────────
#
# Architecture chosen: Sequential Pipeline + Aggregator
# Reason: Many finance questions in this assignment have a natural order:
# first identify tickers / market data, then fetch fundamentals or sentiment,
# then synthesize a final answer. This is simpler and more robust than a full
# orchestrator-critic design, while handling cross-domain questions better than
# a purely parallel design.
#
# Specialist breakdown:
#   Agent 1 — Market Agent: sector/industry lookup + price performance
#   Agent 2 — Fundamentals Agent: company overview / valuation metrics
#   Agent 3 — Sentiment Agent: latest news sentiment
#
# Verification mechanism:
#   The system extracts the top-k ranking from structured tool outputs
#   (raw get_price_performance results) rather than trusting the market
#   agent’s free-form natural language summary. This reduces ranking errors
#   in the final answer and makes the handoff between specialists more reliable.
#
import time
import re

MARKET_TOOLS = [SCHEMA_TICKERS, SCHEMA_PRICE]
FUNDAMENTAL_TOOLS = [SCHEMA_OVERVIEW, SCHEMA_SQL]
SENTIMENT_TOOLS   = [SCHEMA_NEWS]

MARKET_AGENT_PROMPT = """
You are a market-data specialist.

Focus only on:
- sector / industry lookup
- price performance
- ranking stocks by return

Rules:
1. For sector/industry questions, use get_tickers_by_sector first.
2. For return/performance questions, use get_price_performance.
3. For ranking questions, do ONE broad lookup first, then rank from that result.
4. Do not repeatedly re-query small subsets unless the first result is missing.
5. Do not use top gainers/losers or market status unless the user explicitly asks for those.
6. If some tickers fail, ignore failed entries and rank only successful ones.
7. Do not fabricate tickers or numbers.
8. When asked for top-k, clearly identify exactly the requested number of tickers if available.
"""

FUNDAMENTAL_AGENT_PROMPT = """
You are a fundamentals specialist.
Focus only on:
- company overview
- P/E ratio
- EPS
- market cap
- 52-week high / low
- local database lookup when needed

Rules:
1. Use get_company_overview for stock fundamentals.
2. Use query_local_db only for simple SQL SELECT queries on stocks.db.
3. If a tool returns missing/error data, say that clearly.
4. Do not invent financial metrics.
"""

SENTIMENT_AGENT_PROMPT = """
You are a news-sentiment specialist.
Focus only on:
- latest stock news sentiment
- bullish / bearish / neutral summaries

Rules:
1. Use get_news_sentiment for ticker-specific news.
2. Summarize sentiment briefly and clearly.
3. Do not invent headlines if news data is missing.
4. If no news is available, say that clearly.
"""

def run_market_agent(task: str, verbose: bool = True) -> AgentResult:
    return run_specialist_agent(
        agent_name="Market Agent",
        system_prompt=MARKET_AGENT_PROMPT,
        task=task,
        tool_schemas=MARKET_TOOLS,
        max_iters=6,
        verbose=verbose,
    )

def run_fundamental_agent(task: str, verbose: bool = True) -> AgentResult:
    return run_specialist_agent(
        agent_name="Fundamentals Agent",
        system_prompt=FUNDAMENTAL_AGENT_PROMPT,
        task=task,
        tool_schemas=FUNDAMENTAL_TOOLS,
        max_iters=8,
        verbose=verbose,
    )

def run_sentiment_agent(task: str, verbose: bool = True) -> AgentResult:
    return run_specialist_agent(
        agent_name="Sentiment Agent",
        system_prompt=SENTIMENT_AGENT_PROMPT,
        task=task,
        tool_schemas=SENTIMENT_TOOLS,
        max_iters=8,
        verbose=verbose,
    )

def _extract_requested_top_n(question: str, default: int = 3) -> int:
    """
    Extract requested top-k from the question.
    Examples:
      'top 3' -> 3
      'top five' -> 5
      'top ten' -> 10
    """
    q = question.lower()

    m = re.search(r"\btop\s+(\d+)\b", q)
    if m:
        return int(m.group(1))

    word_to_num = {
        "one": 1,
        "two": 2,
        "three": 3,
        "four": 4,
        "five": 5,
        "six": 6,
        "seven": 7,
        "eight": 8,
        "nine": 9,
        "ten": 10,
    }

    for word, num in word_to_num.items():
        if f"top {word}" in q:
            return num

    return default

def _has_top_k_pattern(question: str) -> bool:
    q = question.lower()

    if re.search(r"\btop\s+\d+\b", q):
        return True

    for word in ["one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten"]:
        if f"top {word}" in q:
            return True

    return False

def _extract_top_tickers_from_price_raw(raw_data: dict, top_n: int = 3):
    """
    Recover top-N tickers by pct_change from the MOST COMPLETE get_price_performance
    result, rather than the last one.
    """
    best_price_dict = None
    best_count = -1

    for k, v in raw_data.items():
        if not k.startswith("get_price_performance"):
            continue
        if not isinstance(v, dict):
            continue

        valid_count = 0
        for ticker, info in v.items():
            if isinstance(info, dict) and "pct_change" in info:
                valid_count += 1

        if valid_count > best_count:
            best_count = valid_count
            best_price_dict = v

    if not isinstance(best_price_dict, dict):
        return []

    scored = []
    for ticker, info in best_price_dict.items():
        if isinstance(info, dict) and "pct_change" in info:
            scored.append((ticker, info["pct_change"]))

    scored.sort(key=lambda x: x[1], reverse=True)
    return [t for t, _ in scored[:top_n]]

def _extract_top_ticker_rows_from_price_raw(raw_data: dict, top_n: int = 3):
    """
    Recover top-N rows from the MOST COMPLETE get_price_performance result.
    Returns:
        [{"ticker": "...", "pct_change": ...}, ...]
    """
    best_price_dict = None
    best_count = -1

    for k, v in raw_data.items():
        if not k.startswith("get_price_performance"):
            continue
        if not isinstance(v, dict):
            continue

        valid_rows = []
        for ticker, info in v.items():
            if isinstance(info, dict) and "pct_change" in info:
                valid_rows.append((ticker, info["pct_change"]))

        if len(valid_rows) > best_count:
            best_count = len(valid_rows)
            best_price_dict = v

    if not isinstance(best_price_dict, dict):
        return []

    scored = []
    for ticker, info in best_price_dict.items():
        if isinstance(info, dict) and "pct_change" in info:
            scored.append({
                "ticker": ticker,
                "pct_change": info["pct_change"]
            })

    scored.sort(key=lambda x: x["pct_change"], reverse=True)
    return scored[:top_n]

def _aggregate_answers(question: str, agent_results: list) -> str:
    """
    Merge specialist answers for fallback cases without exposing the full
    augmented prompt / conversation wrapper to the user.
    """
    parts = []
    for r in agent_results:
        if r.answer:
            parts.append(r.answer.strip())
    return "\n\n".join(parts).strip()

def run_multi_agent(question: str, verbose: bool = True, routing_text: str | None = None) -> dict:
    start = time.time()
    agent_results = []
    q = (routing_text or question).lower()

    requested_top_n = _extract_requested_top_n(question, default=3)
    has_top_k = _has_top_k_pattern(question)

    # 1) cross-domain hard question FIRST
    is_cross_domain = (
        has_top_k and
        ("return" in q or "performance" in q or "grew" in q) and
        ("p/e" in q or "pe ratio" in q or "eps" in q or "market cap" in q) and
        ("sentiment" in q or "news" in q)
    )

    if is_cross_domain:
        market_task = (
            "Answer ONLY the market-data part of this question.\n"
            "Steps:\n"
            "1. Find the relevant stock universe.\n"
            "2. Fetch the requested return/performance metric for the full universe once.\n"
            "3. Rank the successful results.\n"
            f"4. Identify exactly the top {requested_top_n} tickers.\n"
            "Do not fetch fundamentals. Do not fetch sentiment. Do not use unrelated tools.\n\n"
            f"Original question: {question}"
        )
        market_res = run_market_agent(market_task, verbose=verbose)
        agent_results.append(market_res)

        top_rows = _extract_top_ticker_rows_from_price_raw(
            market_res.raw_data,
            top_n=requested_top_n
        )
        top_tickers = [row["ticker"] for row in top_rows]

        if top_tickers:
            ticker_str = ", ".join(top_tickers)

            fund_task = (
                f"For tickers {ticker_str}, provide their P/E ratios only. "
                f"If any ticker has missing data, say so clearly."
            )
            sent_task = (
                f"For tickers {ticker_str}, summarize their current news sentiment briefly. "
                f"If any ticker has missing news, say so clearly."
            )

            fund_res = run_fundamental_agent(fund_task, verbose=verbose)
            sent_res = run_sentiment_agent(sent_task, verbose=verbose)

            agent_results.extend([fund_res, sent_res])

            market_summary = f"Top {len(top_rows)} stocks by the requested return metric:\n"
            for i, row in enumerate(top_rows, 1):
                market_summary += f"{i}. {row['ticker']} — {row['pct_change']:.2f}%\n"

            final_answer = (
                f"{market_summary}\n"
                f"{fund_res.answer}\n\n"
                f"{sent_res.answer}"
            )
        else:
            final_answer = market_res.answer

    # 2) fundamentals-only
    elif any(x in q for x in ["p/e", "pe ratio", "eps", "market cap", "52-week", "52 week"]):
        r = run_fundamental_agent(question, verbose=verbose)
        agent_results.append(r)
        final_answer = r.answer

    # 2.5) compare follow-up that likely refers to prior fundamentals context
    elif (
        any(x in q for x in ["compare", "comparison", "how does that compare", "how do they compare"])
        and any(x in question.lower() for x in ["p/e", "pe ratio", "eps", "market cap", "52-week", "52 week"])
    ):
        r = run_fundamental_agent(question, verbose=verbose)
        agent_results.append(r)
        final_answer = r.answer

    # 3) sentiment-only
    elif "sentiment" in q or "news" in q:
        r = run_sentiment_agent(question, verbose=verbose)
        agent_results.append(r)
        final_answer = r.answer

    # 4) market-only
    elif any(x in q for x in ["price performance", "grew", "return", "top gainers", "top losers", "market open", "market closed", "which stock", "which stocks", "sector"]):
        r = run_market_agent(question, verbose=verbose)
        agent_results.append(r)
        final_answer = r.answer

    # 5) fallback
    else:
        r1 = run_market_agent(question, verbose=verbose)
        r2 = run_fundamental_agent(question, verbose=verbose)
        agent_results.extend([r1, r2])
        final_answer = _aggregate_answers(question, agent_results)

    elapsed = time.time() - start

    return {
        "final_answer": final_answer,
        "agent_results": agent_results,
        "elapsed_sec": float(elapsed),
        "architecture": "pipeline",
    }


# ==== Streamlit adapter helpers ====
def set_active_model(model_name: str):
    global ACTIVE_MODEL
    ACTIVE_MODEL = model_name
    return ACTIVE_MODEL

def get_active_model() -> str:
    return ACTIVE_MODEL

def run_agent(
    architecture: str,
    question: str,
    model_name: str | None = None,
    verbose: bool = False,
    routing_text: str | None = None,
):
    if model_name:
        set_active_model(model_name)

    arch = architecture.lower().strip()

    if arch in {'single agent', 'single', 'sa'}:
        return run_single_agent(question, verbose=verbose)

    if arch in {'multi-agent', 'multi agent', 'multi', 'ma'}:
        return run_multi_agent(question, verbose=verbose, routing_text=routing_text)

    raise ValueError(f'Unsupported architecture: {architecture}')
