@echo off
echo Starting Klypto FastAPI Engine using Uvicorn...
call .\venv\Scripts\activate.bat
uvicorn main:app --reload --port 8000
pause
