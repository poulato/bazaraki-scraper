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

## Notes

- Runs locally using `curl_cffi` to bypass Cloudflare
- Respects rate limits with delays between requests
- ArcGIS enrichment runs in parallel (8 workers) since those APIs have no rate limits
- ~6,500 listings across ~110 pages — full scrape takes a while due to individual ad fetches
