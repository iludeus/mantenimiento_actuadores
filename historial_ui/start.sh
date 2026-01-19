#!/bin/sh
uvicorn app.api:app --host 0.0.0.0 --port 8003 &
streamlit run app/ui.py --server.port 8501 --server.address 0.0.0.0

