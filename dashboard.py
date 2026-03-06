#!/usr/bin/env python3
"""
Solar Park Land Finder — Streamlit Dashboard
Loads bazaraki_land.csv and helps identify viable plots for a 1 MW solar park.
"""

import pathlib
import pandas as pd
import streamlit as st

CSV_PATH = pathlib.Path(__file__).parent / "bazaraki_land.csv"

st.set_page_config(
    page_title="Solar Park Land Finder",
    page_icon="☀️",
    layout="wide",
)


@st.cache_data
def load_data():
    df = pd.read_csv(CSV_PATH)
    for col in ("price_numeric", "listing_area_m2", "parcel_area_m2", "cost_per_sqm",
                "hosting_capacity_mw", "res_total_mw", "available_capacity_mw",
                "lat", "lng", "road_distance_m", "slope_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "listing_area_m2" in df.columns:
        df["area_m2"] = df["listing_area_m2"].fillna(df.get("parcel_area_m2", pd.Series()))
    else:
        df["area_m2"] = df["parcel_area_m2"]
    return df


SQM_PER_KW = 12  # ~12 m² per kW in Cyprus (panels + spacing + access roads)

df_raw = load_data()

# ── Sidebar: Solar park size ──────────────────────────────────────────────────
st.sidebar.title("Solar Park Size")

unit = st.sidebar.radio("Unit", ["kW", "MW"], index=1, horizontal=True)
if unit == "MW":
    park_size_mw = st.sidebar.slider(
        "Target park size (MW)", min_value=0.1, max_value=2.0, value=1.0, step=0.1
    )
else:
    park_size_kw = st.sidebar.slider(
        "Target park size (kW)", min_value=50, max_value=10000, value=1000, step=50
    )
    park_size_mw = park_size_kw / 1000

required_area = int(park_size_mw * 1000 * SQM_PER_KW)

st.sidebar.markdown(f"""
**Calculated requirements:**
- Grid capacity needed: **{park_size_mw:.1f} MW**
- Land needed: **~{required_area:,} m²** ({required_area / 10_000:.1f} ha)
""")

st.sidebar.divider()

# ── Sidebar: Filters ──────────────────────────────────────────────────────────
st.sidebar.title("Filters")

min_capacity = st.sidebar.slider(
    "Min available grid capacity (MW)",
    min_value=0.0,
    max_value=float(df_raw["available_capacity_mw"].max() or 50),
    value=float(park_size_mw),
    step=0.5,
)

min_area = st.sidebar.slider(
    "Min parcel area (m²)",
    min_value=0,
    max_value=50_000,
    value=min(required_area, 50_000),
    step=500,
)

max_cost = st.sidebar.slider(
    "Max cost per m² (€)",
    min_value=0,
    max_value=int(df_raw["cost_per_sqm"].quantile(0.95) or 1000) + 100,
    value=int(df_raw["cost_per_sqm"].quantile(0.95) or 500),
    step=10,
)

max_road_dist = st.sidebar.slider(
    "Max distance to road (m)",
    min_value=0,
    max_value=500,
    value=200,
    step=25,
)

max_slope = st.sidebar.slider(
    "Max terrain slope (%)",
    min_value=0,
    max_value=30,
    value=15,
    step=1,
)

districts = sorted(df_raw["district"].dropna().unique())
selected_districts = st.sidebar.multiselect(
    "Districts", districts, default=districts
)

zones = sorted(df_raw["planning_zone"].dropna().unique())
selected_zones = st.sidebar.multiselect(
    "Planning zones", zones, default=zones
)

# ── Apply filters ─────────────────────────────────────────────────────────────
df = df_raw.copy()
df = df[df["lat"].notna() & df["lng"].notna()]
df = df[df["available_capacity_mw"].notna() & (df["available_capacity_mw"] >= min_capacity)]
df = df[df["area_m2"].notna() & (df["area_m2"] >= min_area)]
df = df[df["cost_per_sqm"].notna() & (df["cost_per_sqm"] <= max_cost)]
df = df[df["district"].isin(selected_districts)]
df = df[df["planning_zone"].isin(selected_zones)]
if "road_distance_m" in df.columns:
    df = df[df["road_distance_m"].isna() | (df["road_distance_m"] <= max_road_dist)]
if "slope_pct" in df.columns:
    df = df[df["slope_pct"].isna() | (df["slope_pct"] <= max_slope)]
df = df.sort_values("cost_per_sqm", ascending=True).reset_index(drop=True)

# ── Header ────────────────────────────────────────────────────────────────────
park_label = f"{park_size_mw:.1f} MW" if park_size_mw >= 1 else f"{park_size_mw * 1000:.0f} kW"
st.title("☀️ Solar Park Land Finder")
st.caption(f"Plots with at least {park_label} of grid capacity and ~{required_area:,} m² of land.")

# ── Summary metrics ───────────────────────────────────────────────────────────
col1, col2, col3 = st.columns(3)
col1.metric("Matching plots", len(df))
if len(df) > 0:
    col2.metric("Cheapest €/m²", f"€{df['cost_per_sqm'].min():,.2f}")
    col3.metric("Median €/m²", f"€{df['cost_per_sqm'].median():,.2f}")
else:
    col2.metric("Cheapest €/m²", "—")
    col3.metric("Median €/m²", "—")

# ── Map ───────────────────────────────────────────────────────────────────────
st.subheader("Map")

if len(df) > 0:
    map_df = df[["lat", "lng"]].dropna().rename(columns={"lng": "lon"})
    st.map(map_df, use_container_width=True)
else:
    st.info("No plots match the current filters. Try relaxing the criteria.")

# ── Results table ─────────────────────────────────────────────────────────────
st.subheader(f"Ranked Results ({len(df)} plots)")

if len(df) > 0:
    # Map link: open in Google Maps
    df["map_link"] = df.apply(
        lambda r: f"https://www.google.com/maps?q={r['lat']},{r['lng']}" if pd.notna(r.get("lat")) and pd.notna(r.get("lng")) else "",
        axis=1,
    )
    display_cols = [
        "url", "map_link", "location", "price_numeric", "area_m2",
        "cost_per_sqm",
        "road_distance_m", "slope_pct", "slope_class",
        "planning_zone", "substation_en", "available_capacity_mw",
    ]
    for c in display_cols:
        if c not in df.columns:
            df[c] = ""
    display_df = df[display_cols].copy()
    display_df.columns = [
        "Listing", "Map", "Location", "Price (€)", "Area (m²)",
        "€/m²",
        "Road (m)", "Slope %", "Terrain",
        "Zone", "Substation", "Available MW",
    ]

    st.dataframe(
        display_df,
        width="stretch",
        hide_index=True,
        column_config={
            "Listing": st.column_config.LinkColumn("Listing", display_text="Open"),
            "Map": st.column_config.LinkColumn("Map", display_text="Map"),
            "Price (€)": st.column_config.NumberColumn(format="€%d"),
            "Area (m²)": st.column_config.NumberColumn(format="%d m²"),
            "€/m²": st.column_config.NumberColumn(format="€%.2f"),
            "Road (m)": st.column_config.NumberColumn(format="%d m"),
            "Slope %": st.column_config.NumberColumn(format="%.1f%%"),
            "Available MW": st.column_config.NumberColumn(format="%.1f MW"),
        },
    )
else:
    st.warning("No results. Adjust filters in the sidebar.")

# ── Substation breakdown ──────────────────────────────────────────────────────
st.subheader("Substations with Available Capacity")

if len(df) > 0:
    subst = (
        df.groupby("substation_en")
        .agg(
            plots=("id", "count"),
            avg_cost_sqm=("cost_per_sqm", "median"),
            available_mw=("available_capacity_mw", "first"),
            total_mw=("hosting_capacity_mw", "first"),
        )
        .sort_values("available_mw", ascending=False)
        .reset_index()
    )
    subst.columns = ["Substation", "Plots", "Median €/m²", "Available MW", "Total MW"]

    st.dataframe(
        subst,
        width="stretch",
        hide_index=True,
        column_config={
            "Median €/m²": st.column_config.NumberColumn(format="€%.2f"),
            "Available MW": st.column_config.NumberColumn(format="%.1f MW"),
            "Total MW": st.column_config.NumberColumn(format="%.1f MW"),
        },
    )
