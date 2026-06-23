from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
import traceback

app = FastAPI(title="Klypto Strategy Evaluator API")

# ---------------------------------------------------------
# 1. Solid Data Validation (Pydantic Models)
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


# ---------------------------------------------------------
# 2. Endpoints
# ---------------------------------------------------------
@app.get("/")
def read_root():
    return {"status": "FastAPI Strategy Engine is running", "message": "Welcome to Klypto Python Engine!"}

@app.post("/api/evaluate-strategy")
def evaluate_strategy(payload: StrategyPayload):
    try:
        # Convert incoming historical data to pandas DataFrame
        if not payload.historical_data:
            raise HTTPException(status_code=400, detail="historical_data cannot be empty")
        
        # We can convert list of Pydantic models to list of dicts directly
        data_dicts = [candle.model_dump() for candle in payload.historical_data]
        df = pd.DataFrame(data_dicts)
        
        # Ensure numerical columns are exact floats/ints for Pandas
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(int)

        # ---------------------------------------------------------
        # 3. Dynamic Execution of Custom Strategy Code
        # ---------------------------------------------------------
        if payload.strategy_code:
            # Create a safe local scope for the exec function
            local_scope = {'df': df, 'pd': pd}
            try:
                # The custom code should modify the 'df' directly
                exec(payload.strategy_code, {}, local_scope)
                df = local_scope['df']
            except Exception as e:
                # Catch syntax or runtime errors in the user's python code
                error_trace = traceback.format_exc()
                print("Error executing custom strategy code:\n", error_trace)
                raise HTTPException(status_code=400, detail=f"Error in strategy code: {str(e)}")
        else:
            # Handle Built-in strategies (MACD_RSI, etc.)
            # For now, just add a dummy signal column if strategy_code is missing
            df['signal'] = 'HOLD'

        # Convert back to list of dicts for JSON response
        # replace NaN with None so JSON encoder doesn't complain about Out of Range floats
        df = df.where(pd.notnull(df), None)
        result_data = df.to_dict(orient='records')
        
        return result_data

    except HTTPException as he:
        raise he
    except Exception as e:
        error_trace = traceback.format_exc()
        print("Internal Server Error:\n", error_trace)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    # This block allows running with: python main.py
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)
