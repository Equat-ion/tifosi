import os
import pandas as pd
import json
from glob import glob

# ----------------- CONFIG -----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "../data/raw")
PROC_DIR = os.path.join(BASE_DIR, "../data/processed")

os.makedirs(PROC_DIR, exist_ok=True)

# ----------------- LOAD HELPERS -----------------
def load_jsons(pattern):
    files = glob(os.path.join(RAW_DIR, pattern))
    dfs = []
    for f in files:
        try:
            df = pd.read_json(f)
            df["source_file"] = os.path.basename(f)
            dfs.append(df)
        except Exception as e:
            print(f"Failed to load {f}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# ----------------- BUILD MASTER TABLES -----------------

print("Loading race info...")
race_info = load_jsons("race_info_*.json")

print("Loading laps...")
laps = load_jsons("laps_*.json")

print("Loading pitstops...")
pitstops = load_jsons("pitstops_*.json")

print("Loading qualifying...")
qual = load_jsons("qualifying_*.json")

# ----------------- PROCESSING -----------------
# Race info stays as-is (already clean)
race_info.to_csv(os.path.join(PROC_DIR, "race_info.csv"), index=False)
race_info.to_json(os.path.join(PROC_DIR, "race_info.json"), orient="records")

# Driver results summary from laps: take best lap, avg lap, total laps
if not laps.empty:
    results_summary = (
        laps.groupby(["Driver", "source_file"])
        .agg(
            total_laps=("LapNumber", "max"),
            avg_lap_time=("LapTime", "mean"),
            best_lap_time=("LapTime", "min"),
            finish_position=("Position", "last")
        )
        .reset_index()
    )
    results_summary.to_csv(os.path.join(PROC_DIR, "results_summary.csv"), index=False)
    results_summary.to_json(os.path.join(PROC_DIR, "results_summary.json"), orient="records")

# Pitstops summary per driver
if not pitstops.empty:
    pit_summary = (
        pitstops.groupby(["Driver", "source_file"])
        .agg(
            pit_count=("LapNumber", "count"),
            avg_pit_duration=("Duration", "mean")
        )
        .reset_index()
    )
    pit_summary.to_csv(os.path.join(PROC_DIR, "pitstops_summary.csv"), index=False)
    pit_summary.to_json(os.path.join(PROC_DIR, "pitstops_summary.json"), orient="records")

# Qualifying best times per driver
if not qual.empty:
    qual_summary = (
        qual.groupby(["Driver", "source_file"])
        .agg(
            best_q_time=("LapTime", "min"),
            laps_attempted=("LapNumber", "count")
        )
        .reset_index()
    )
    qual_summary.to_csv(os.path.join(PROC_DIR, "qualifying_summary.csv"), index=False)
    qual_summary.to_json(os.path.join(PROC_DIR, "qualifying_summary.json"), orient="records")

print("Processing complete. Clean data saved in data/processed/")
