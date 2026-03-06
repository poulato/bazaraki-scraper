#!/usr/bin/env python3
"""
Quick way to play with bazaraki_land.csv.
Run:  python explore.py
Or copy the code into a Jupyter notebook and run cell-by-cell.
"""

import pandas as pd
from pathlib import Path

CSV_PATH = Path(__file__).parent / "bazaraki_land.csv"

def load():
    df = pd.read_csv(CSV_PATH)
    # Coerce numeric columns
    for col in ("price_numeric", "listing_area_m2", "parcel_area_m2", "cost_per_sqm",
                "hosting_capacity_mw", "res_total_mw", "available_capacity_mw",
                "lat", "lng", "road_distance_m", "slope_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


if __name__ == "__main__":
    df = load()
    print("Shape:", df.shape)
    print("\nColumns:", list(df.columns))
    print("\n--- Sample stats ---")
    print(df["district"].value_counts().head(10))
    print("\nPrice (€) — min/median/max:")
    print(df["price_numeric"].agg(["min", "median", "max"]))
    print("\nListing area (m²) — min/median/max:")
    print(df["listing_area_m2"].agg(["min", "median", "max"]))
    print("\n--- Example: residential plots in Limassol, 1000–5000 m², with price ---")
    mask = (
        (df["location"].str.contains("Limassol", na=False))
        & (df["listing_type"] == "Residential")
        & (df["listing_area_m2"] >= 1000)
        & (df["listing_area_m2"] <= 5000)
        & (df["price_numeric"].notna())
    )
    sample = df.loc[mask, ["id", "title", "price", "listing_area_m2", "cost_per_sqm", "location"]].head(10)
    print(sample.to_string())
    print("\nLoad in REPL:  from explore import load; df = load()")
