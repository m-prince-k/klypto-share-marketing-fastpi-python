#!/bin/bash
echo "Starting Klypto FastAPI Engine using Uvicorn..."
source venv/Scripts/activate
uvicorn main:app --reload --port 8000
