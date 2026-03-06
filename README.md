# Bazaraki Land Scraper

Scrapes all land-for-sale listings from [Bazaraki](https://www.bazaraki.com) and enriches each with:

- **EAC Substation** data (name, hosting capacity, available capacity)
- **DLS Ktimatologio** data (parcel number, sheet/plan, block, area, planning zone, district, municipality)

Outputs a CSV file.

## Setup

```bash
pip install -r requirements.txt
```

## Usage

```bash
python scrape.py
```

For testing, edit `MAX_PAGES` at the top of `scrape.py` (e.g. set to `3` to only scrape 3 pages).

Output: `bazaraki_land.csv`

## Exploring the data

**1. Streamlit dashboard (maps + filters)**  
Interactive map and filters for solar park land (area, slope, grid capacity, etc.):

```bash
streamlit run dashboard.py
```

**2. Python / pandas**  
Load the CSV and filter, aggregate, or plot in a script or Jupyter:

```bash
python explore.py          # quick stats and example queries
# or
jupyter notebook           # then New → Notebook, paste from explore.py
```

**3. Spreadsheet**  
Open `bazaraki_land.csv` in Excel, Numbers, or Google Sheets for sorting and ad‑hoc filters.

## Notes

- Runs locally using `curl_cffi` to bypass Cloudflare
- Respects rate limits with delays between requests
- ArcGIS enrichment runs in parallel (8 workers) since those APIs have no rate limits
- ~6,500 listings across ~110 pages — full scrape takes a while due to individual ad fetches
