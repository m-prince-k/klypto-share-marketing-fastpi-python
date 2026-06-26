from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
import traceback
import re
import os
import psycopg2
import psycopg2.extras
import requests
from datetime import datetime, timedelta

app = FastAPI(title="Klypto Strategy Evaluator API")

# ---------------------------------------------------------
# DB + Angel One Config (matches Node.js .env)
# ---------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "43.205.133.183"),
    "user":     os.getenv("DB_USER",     "web_user"),
    "password": os.getenv("DB_PASSWORD", "web_klypto"),
    "dbname":   os.getenv("DB_NAME",     "web_stock_data"),
    "port":     int(os.getenv("DB_PORT", 5432)),
}

ANGEL_CLIENT_CODE = os.getenv("ANGEL_CLIENT_CODE", "AAAP423969")
ANGEL_PASSWORD    = os.getenv("ANGEL_PASSWORD",     "2004")
ANGEL_API_KEY     = os.getenv("ANGEL_API_KEY",      "AsZssQ9i")
ANGEL_TOTP_SECRET = os.getenv("ANGEL_TOTP_SECRET",  "W2VACHHBZMVA6ACTPAJY3TNGFA")

# Node.js webhook base URL (for progress / signal push)
NODE_WEBHOOK = os.getenv("NODE_WEBHOOK_URL", "http://127.0.0.1:4000")


# ---------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------
class CandleData(BaseModel):
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: int

class StrategyPayload(BaseModel):
    symbol: str
    interval: str
    strategy: Optional[str] = "MACD_RSI"
    params: Optional[Dict[str, Any]] = {}
    strategy_code: Optional[str] = None
    historical_data: List[CandleData]

class ScannerRequest(BaseModel):
    strategy_code: str
    use_historical_only: Optional[bool] = True
    userId: str


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)


def validate_strategy_code(code: str):
    """Raise HTTPException if code looks invalid or unsafe."""
    valid_keywords = ['plot_markers', 'markers.append', 'df[', 'df.loc', 'signal', 'plotshape', 'plot(']
    has_valid = any(kw in code for kw in valid_keywords)
    if not has_valid or len(code.strip()) < 20:
        raise HTTPException(
            status_code=400,
            detail=f'Invalid strategy_code. Must contain strategy logic (e.g. plot_markers, df[], signal). Received: "{code[:50]}"'
        )
    forbidden_patterns = [
        r"import\s+(os|sys|subprocess|shlex|socket|urllib|requests)",
        r"from\s+(os|sys|subprocess|shlex|socket|urllib|requests)\s+import",
        r"__import__", r"open\s*\(", r"eval\s*\(", r"exec\s*\(",
        r"globals\s*\(", r"locals\s*\(", r"os\.", r"sys\.", r"subprocess\."
    ]
    for pattern in forbidden_patterns:
        if re.search(pattern, code):
            raise HTTPException(
                status_code=400,
                detail="Security Error: Unsafe code detected! System calls and file operations are not allowed."
            )


def run_strategy_on_df(df: pd.DataFrame, strategy_code: str) -> pd.DataFrame:
    """Execute strategy_code safely using exec() and return the modified df."""
    import numpy as np

    def wma(series, length):
        weights = np.arange(1, length + 1)
        return series.rolling(length).apply(lambda prices: np.dot(prices, weights) / weights.sum(), raw=True)

    local_scope = {
        'df': df, 'pd': pd, 'np': np, 'wma': wma,
        'open': df['open'], 'high': df['high'],
        'low': df['low'], 'close': df['close'], 'volume': df['volume']
    }

    marker_count = [1]

    def plot_markers(data, *args, **kwargs):
        if isinstance(data, list):
            df['text'] = None; df['position'] = None
            df['signal'] = None; df['type'] = None
            for m in data:
                idx = m.get('time')
                row_idx = None
                if isinstance(idx, int) and 0 <= idx < len(df):
                    row_idx = idx
                elif isinstance(idx, str):
                    matches = df.index[df['datetime'] == idx].tolist()
                    if matches:
                        row_idx = matches[0]
                if row_idx is not None:
                    df.at[row_idx, 'text']     = m.get('text')
                    df.at[row_idx, 'position'] = m.get('position')
                    df.at[row_idx, 'signal']   = m.get('signal', m.get('type'))
                    df.at[row_idx, 'type']     = m.get('type',   m.get('signal'))
        else:
            col_name = kwargs.get('title', kwargs.get('name', f"marker_{marker_count[0]}"))
            df[col_name] = data
            marker_count[0] += 1

    def plot(series_or_condition, *args, **kwargs):
        col_name = kwargs.get('title', kwargs.get('name', f"plot_{marker_count[0]}"))
        df[col_name] = series_or_condition
        marker_count[0] += 1

    local_scope['plot_markers'] = plot_markers
    local_scope['plot']         = plot
    local_scope['plotshape']    = plot_markers

    exec(strategy_code, local_scope, local_scope)
    return local_scope['df']


def df_to_clean_records(df: pd.DataFrame) -> list:
    raw = df.to_dict(orient='records')
    result = []
    for row in raw:
        clean = {}
        for k, v in row.items():
            if isinstance(v, float) and v != v:  # NaN check
                clean[k] = None
            else:
                clean[k] = v
        result.append(clean)
    return result


def notify_node(path: str, payload: dict):
    """Fire-and-forget POST to Node.js internal webhook."""
    try:
        requests.post(f"{NODE_WEBHOOK}{path}", json=payload, timeout=3)
    except Exception:
        pass  # Never crash if Node.js is unreachable


def angel_login():
    """Login to Angel One and return the JWT token."""
    try:
        import pyotp
        totp = pyotp.TOTP(ANGEL_TOTP_SECRET).now()
        url = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByPassword"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": ANGEL_API_KEY,
        }
        body = {
            "clientcode": ANGEL_CLIENT_CODE,
            "password":   ANGEL_PASSWORD,
            "totp":       totp,
        }
        resp = requests.post(url, json=body, headers=headers, timeout=10)
        data = resp.json()
        if data.get("status"):
            return data["data"]["jwtToken"]
    except Exception as e:
        print(f"[Scanner] Angel One login failed: {e}")
    return None


def angel_get_candles(jwt_token: str, symboltoken: str, fromdate: str, todate: str) -> list:
    """Fetch 5-minute candles from Angel One REST API."""
    try:
        url = "https://apiconnect.angelone.in/rest/secure/angelbroking/historical/v1/getCandleData"
        headers = {
            "Authorization": f"Bearer {jwt_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "127.0.0.1",
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": ANGEL_API_KEY,
        }
        body = {
            "exchange":    "NSE",
            "symboltoken": symboltoken,
            "interval":    "FIVE_MINUTE",
            "fromdate":    fromdate,
            "todate":      todate,
        }
        resp = requests.post(url, json=body, headers=headers, timeout=15)
        data = resp.json()
        if data.get("status") and data.get("data"):
            return [
                {
                    "datetime": c[0].replace("T", " ")[:19],
                    "open":     float(c[1]),
                    "high":     float(c[2]),
                    "low":      float(c[3]),
                    "close":    float(c[4]),
                    "volume":   int(c[5]),
                }
                for c in data["data"]
            ]
    except Exception as e:
        print(f"[Scanner] Angel One candle fetch error: {e}")
    return []


# ---------------------------------------------------------
# Full Background Scanner Task
# ---------------------------------------------------------
def run_scanner_background(strategy_code: str, use_historical_only: bool, userId: str):
    print(f"\n[Scanner] Background scan started for user: {userId}")

    conn = None
    try:
        conn = get_db_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # 1. Clear old signals for this user
        cur.execute('DELETE FROM strategy_signals WHERE "userId" = %s', (userId,))
        conn.commit()
        print(f"[Scanner] Cleared old signals for user {userId}")

        # 2. Fetch all symbols from historical_candles
        cur.execute("SELECT DISTINCT symbol FROM historical_candles")
        symbols = [row[0] for row in cur.fetchall()]
        total = len(symbols)
        print(f"[Scanner] Scanning {total} symbols...")

        # 3. Angel One login if live data needed
        jwt_token = None
        if not use_historical_only:
            jwt_token = angel_login()
            if jwt_token:
                print("[Scanner] Angel One login successful.")
            else:
                print("[Scanner] Angel One login failed â€” using historical-only mode.")
                use_historical_only = True

        total_signals = 0

        for idx, symbol in enumerate(symbols):
            try:
                # Notify Node.js of progress (for Socket.io progress bar)
                notify_node("/api/strategy/internal/scanner-progress", {
                    "userId":        userId,
                    "processed":     idx + 1,
                    "total":         total,
                    "current_stock": symbol,
                })
                print(f"  - Processing {symbol} ({idx+1}/{total})...")

                # 4. Fetch historical candles from DB
                cur.execute(
                    "SELECT datetime, open, high, low, close, volume "
                    "FROM historical_candles WHERE symbol = %s ORDER BY datetime ASC",
                    (symbol,)
                )
                rows = cur.fetchall()
                historical_data = [
                    {
                        "datetime": str(r["datetime"])[:19].replace("T", " "),
                        "open":     float(r["open"]),
                        "high":     float(r["high"]),
                        "low":      float(r["low"]),
                        "close":    float(r["close"]),
                        "volume":   int(r["volume"]),
                    }
                    for r in rows
                ]

                # 5. Append latest live candles from Angel One (if enabled)
                if not use_historical_only and jwt_token:
                    # Fetch symbol token from DB if available
                    cur.execute(
                        'SELECT token FROM "Stocks" WHERE name = %s LIMIT 1',
                        (symbol,)
                    )
                    token_row = cur.fetchone()
                    if token_row and token_row[0]:
                        now = datetime.now()
                        two_days_ago = now - timedelta(days=2)
                        fmt = "%Y-%m-%d %H:%M"
                        live_candles = angel_get_candles(
                            jwt_token,
                            str(token_row[0]),
                            two_days_ago.strftime(fmt),
                            now.strftime(fmt),
                        )
                        existing_times = {c["datetime"] for c in historical_data}
                        for c in live_candles:
                            if c["datetime"] not in existing_times:
                                historical_data.append(c)

                # 6. Ensure ordered & clean data
                historical_data.sort(key=lambda x: x["datetime"])
                df = pd.DataFrame(historical_data)

                # 7. Run Strategy code if valid
                if len(df) > 0 and strategy_code:
                    df = run_strategy_on_df(df, strategy_code)
                else:
                    df['signal'] = 'HOLD'

                clean_records = df_to_clean_records(df)

                # 8. Extract Signals and save to DB
                signals = [r for r in clean_records if str(r.get('signal') or r.get('type') or '').upper() in ['BUY', 'SELL']]
                
                if signals:
                    total_signals += len(signals)
                    notify_node("/api/strategy/internal/scanner-signal", {
                        "userId": userId,
                        "symbol": symbol,
                        "signalData": signals[-1]
                    })
                    print(f"  - Found {len(signals)} signals for {symbol}")

                for i, sig in enumerate(signals):
                    sig_type = str(sig.get('signal') or sig.get('type')).upper()
                    time_str = str(sig.get('datetime') or sig.get('time') or '')

                    if sig_type == 'BUY' and '09:15' in time_str:
                        trade_type = str(sig.get('text') or sig.get('tradeType') or 'CALL').upper()

                        # Look ahead for the next SELL signal
                        exit_sig  = next(
                            (s for s in signals[i+1:]
                             if str(s.get('type') or s.get('signal') or '').upper() == 'SELL'),
                            None
                        )
                        exit_time = str((exit_sig or {}).get('datetime') or (exit_sig or {}).get('time') or '')

                        try:
                            signal_ts = datetime.fromisoformat(time_str.replace(' ', 'T'))
                        except Exception:
                            signal_ts = datetime.now()

                        indicator_values = {**sig, "exitTime": exit_time, "tradeType": trade_type}

                        # Insert into DB
                        cur.execute("""
                            INSERT INTO strategy_signals
                                (symbol, "userId", "signalType", "indicatorValues", timestamp, "createdAt", "updatedAt")
                            VALUES (%s, %s, %s, %s::jsonb, %s, NOW(), NOW())
                        """, (
                            symbol, userId, sig_type,
                            psycopg2.extras.Json(indicator_values),
                            signal_ts
                        ))
                        conn.commit()
                        print(f"    => Stored: {symbol} | {trade_type} @ {time_str}, exit @ {exit_time}")

            except Exception as e:
                err_msg = str(e)
                print(f"[Scanner] Error processing {symbol}: {err_msg}")
                notify_node("/api/strategy/internal/scanner-error", {
                    "userId": userId,
                    "symbol": symbol,
                    "error":  err_msg,
                })

        # All done
        final_msg = (
            f"Scan Complete. Generated signals for {total_signals} stocks."
            if total_signals > 0
            else "Scan Complete. No BUY/SELL signals generated."
        )
        print(f"\n[Scanner] {final_msg}")
        notify_node("/api/strategy/internal/scanner-complete", {
            "userId":  userId,
            "success": True,
            "message": final_msg,
        })

    except Exception as e:
        err = traceback.format_exc()
        print(f"[Scanner] FATAL error:\n{err}")
        notify_node("/api/strategy/internal/scanner-complete", {
            "userId":  userId,
            "success": False,
            "message": f"Scanner failed: {str(e)}",
        })
    finally:
        if conn:
            conn.close()


# ---------------------------------------------------------
# Endpoints
# ---------------------------------------------------------
@app.get("/")
def read_root():
    return {"status": "FastAPI Strategy Engine is running", "message": "Welcome to Klypto Python Engine!"}


@app.post("/api/evaluate-strategy")
def evaluate_strategy(payload: StrategyPayload):
    """Evaluate a strategy on given candle data â€” used for Dry Run validation."""
    try:
        if not payload.historical_data:
            raise HTTPException(status_code=400, detail="historical_data cannot be empty")

        data_dicts = [candle.model_dump() for candle in payload.historical_data]
        df = pd.DataFrame(data_dicts)
        df['open']   = df['open'].astype(float)
        df['high']   = df['high'].astype(float)
        df['low']    = df['low'].astype(float)
        df['close']  = df['close'].astype(float)
        df['volume'] = df['volume'].astype(int)

        if payload.strategy_code:
            validate_strategy_code(payload.strategy_code)
            try:
                df = run_strategy_on_df(df, payload.strategy_code)
            except Exception as e:
                error_trace = traceback.format_exc()
                print("Error executing custom strategy code:\n", error_trace)
                raise HTTPException(status_code=400, detail=f"Error in strategy code: {str(e)}")
        else:
            df['signal'] = 'HOLD'

        return df_to_clean_records(df)

    except HTTPException as he:
        raise he
    except Exception as e:
        error_trace = traceback.format_exc()
        print("Internal Server Error:\n", error_trace)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.post("/api/start-scanner")
def start_scanner(req: ScannerRequest, background_tasks: BackgroundTasks):
    """
    New endpoint â€” Node.js calls this with strategy_code + userId.
    Validates code, then runs the full scanner in a Python background task.
    Progress / signals / completion are pushed back to Node.js via
    POST /internal/* webhook endpoints.
    """
    validate_strategy_code(req.strategy_code)

    background_tasks.add_task(
        run_scanner_background,
        strategy_code=req.strategy_code,
        use_historical_only=req.use_historical_only,
        userId=req.userId,
    )
    return {"success": True, "message": "Scanner started in Python background"}


@app.post("/api/scan-complete")
def scan_complete():
    print("\n" + "="*50)
    print(" SCAN PROCESS COMPLETED SUCCESSFULLY! ")
    print("="*50 + "\n")
    return {"status": "Scan complete message logged."}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)


