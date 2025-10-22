import os
import time
import fastf1
import pandas as pd
from tqdm import tqdm
import numpy as np


# ----------------- CONFIG -----------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "../data/raw")
CACHE_DIR = os.path.join(BASE_DIR, "../data/fastf1_cache")


os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)


fastf1.Cache.enable_cache(CACHE_DIR)


SEASONS = [2019]        # SEASON


SESSIONS = ["Race", "Qualifying"]       # SESSIONS

# ----------------- HELPER FUNCTIONS -----------------

def save_df(df, name, season, race_name, ext="csv"):
    """Save DataFrame as CSV and JSON"""
    safe_race_name = race_name.replace(" ", "_").replace("/", "_")
    csv_path = os.path.join(RAW_DIR, f"{name}_{season}_{safe_race_name}.{ext}")
    json_path = os.path.join(RAW_DIR, f"{name}_{season}_{safe_race_name}.json")
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient='records')
    print(f"Saved {csv_path} and {json_path}")

# ----------------- FETCH DATA -----------------

for season in SEASONS:
    print(f"\nFetching season {season}...")
    try:
        events = fastf1.get_event_schedule(season)
    except Exception as e:
        print(f"Failed to fetch event schedule for {season}: {e}")
        continue

    for idx, race_info in events.iterrows():
        round_name = race_info['EventName']
        print(f"\nFetching {round_name} ({season})...")
        try:
            race_session = fastf1.get_session(season, round_name, "Race")
            race_session.load()
        except Exception as e:
            print(f"Failed to load race session for {round_name}: {e}")
            continue

        # ----------------- Race Info -----------------
        race_df = pd.DataFrame([{
            "season": season,
            "round": race_info.get("RoundNumber", ""),
            "raceName": race_info.get("EventName", ""),
            "circuit": race_info.get("Circuit", ""),
            "date": race_info.get("Date", "")
        }])
        save_df(race_df, "race_info", season, round_name)

        # ----------------- Laps -----------------
        laps = race_session.laps
        save_df(laps, "laps", season, round_name)

        # ----------------- Pitstops -----------------
        if hasattr(race_session, "pit_stops") and race_session.pit_stops is not None:
            pitstops = race_session.pit_stops
            save_df(pitstops, "pitstops", season, round_name)

        # ----------------- Qualifying -----------------
        try:
            qual_session = fastf1.get_session(season, round_name, "Qualifying")
            qual_session.load()
            save_df(qual_session.laps, "qualifying", season, round_name)
        except Exception as e:
            print(f"No qualifying data for {round_name}: {e}")

        # ----------------- Cooldown -----------------
        time.sleep(1 + 2 * np.random.rand())  # random 1-3 seconds delay


    print(f"Finished season {season}")
