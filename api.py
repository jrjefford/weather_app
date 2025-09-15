#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
from contextlib import contextmanager

import db as dbmod
from open_meteo import fetch_current

app = FastAPI(title="Weather API", version="1.0.0")

# Enable CORS (allow all origins for now)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class WeatherReading(BaseModel):
    city: str
    ts: str
    temperature: float | None = None
    humidity: float | None = None


class Stats(BaseModel):
    avg: float | None
    min: float | None
    max: float | None
    count: int


# Per-request SQLite connection using context manager
@contextmanager
def get_conn(db_path: str = "weather.db"):
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        dbmod.init_db(conn)
        yield conn
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/weather/latest", response_model=WeatherReading)
def weather_latest(city: str = Query(..., min_length=1)):
    city_value = city.strip()
    if not city_value:
        raise HTTPException(status_code=422, detail="city must be non-empty")

    with get_conn() as conn:
        latest = dbmod.latest_for_city(conn, city_value)
        if not latest:
            raise HTTPException(status_code=404, detail="No readings for city")

        return WeatherReading(
            city=latest["city"] if isinstance(latest, sqlite3.Row) else latest.get("city"),
            ts=latest["ts"] if isinstance(latest, sqlite3.Row) else latest.get("ts"),
            temperature=latest["temperature"] if isinstance(latest, sqlite3.Row) else latest.get("temperature"),
            humidity=latest["humidity"] if isinstance(latest, sqlite3.Row) else latest.get("humidity"),
        )


@app.get("/weather/stats", response_model=Stats)
def weather_stats(
    city: str = Query(..., min_length=1),
    n: int = Query(24, ge=1, le=1000),
):
    city_value = city.strip()
    if not city_value:
        raise HTTPException(status_code=422, detail="city must be non-empty")

    with get_conn() as conn:
        stats = dbmod.stats_last_n(conn, city_value, n)
        if not stats or stats.get("count", 0) == 0:
            raise HTTPException(status_code=404, detail="No readings for city")
        return Stats(avg=stats.get("avg"), min=stats.get("min"), max=stats.get("max"), count=stats.get("count", 0))


class FetchBody(BaseModel):
    city: str
    lat: float
    lon: float


@app.post("/weather/fetch")
def weather_fetch(body: FetchBody):
    if not body.city or not body.city.strip():
        raise HTTPException(status_code=422, detail="city must be non-empty")

    try:
        reading = fetch_current(body.lat, body.lon, body.city.strip())
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Upstream fetch failed: {e}")

    with get_conn() as conn:
        try:
            inserted = dbmod.insert_reading(conn, reading)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Database insert failed: {e}")

    return {"inserted": bool(inserted), "skipped": (not inserted), "reading": reading}
