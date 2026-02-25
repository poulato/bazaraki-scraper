#!/usr/bin/env python3
"""
Scrape all Bazaraki land-for-sale listings and enrich each with
substation (EAC) and cadastral (DLS Ktimatologio) data.
Outputs a CSV file.
"""

import csv
import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from curl_cffi import requests as cffi_requests

# ── Config ──────────────────────────────────────────────────────────────────
BASE_URL = "https://www.bazaraki.com"
LISTING_PATH = "/real-estate-for-sale/plots-of-land/"
DELAY_BETWEEN_PAGES = 2        # seconds between listing pages
DELAY_BETWEEN_ADS = 0.5        # seconds between individual ad fetches
MAX_PAGES = None                # set to e.g. 3 for testing, None for all
ARCGIS_WORKERS = 8              # parallel ArcGIS requests (they're fast)
OUTPUT_FILE = "bazaraki_land.csv"

# ArcGIS endpoints
POLYGON_URL = "https://services5.arcgis.com/yaIunh7Pa3QmwPBN/arcgis/rest/services/DistrTrSubstThPoly20240528racp/FeatureServer/317/query"
SUBSTATION_URL = "https://services5.arcgis.com/yaIunh7Pa3QmwPBN/arcgis/rest/services/Transmission_Substations_RES_Hosting_WFL1/FeatureServer/0/query"
DLS_BASE = "https://eservices.dls.moi.gov.cy/arcgis/rest/services/National/CadastralMap_EN/MapServer"

BROWSERS = ["chrome", "chrome110", "chrome116", "chrome120", "safari"]


# ── Bazaraki scraping ───────────────────────────────────────────────────────
def fetch_page(url):
    """Fetch a Bazaraki page using curl_cffi with browser impersonation."""
    for browser in BROWSERS:
        try:
            resp = cffi_requests.get(url, impersonate=browser, timeout=20)
            if resp.status_code == 200:
                return resp.text
        except Exception:
            continue
    return None


def parse_listing_page(html):
    """Extract ad info from a listing page: url, title, price, location."""
    ads = []
    for m in re.finditer(
        r'<div\s+class="advert\s[^"]*"\s+data-event-name="advert_click"\s+data-id="(\d+)"',
        html,
    ):
        ad_id = m.group(1)
        block = html[m.start():m.start() + 5000]

        url_m = re.search(r'href="(/adv/' + ad_id + r'[^"]*)"', block)
        price_m = re.search(r'advert__content-price[^>]*>.*?<span>(.*?)</span>', block, re.S)
        title_m = re.search(r'advert__content-title[^>]*>\s*(.+?)\s*</a>', block, re.S)
        place_m = re.search(r'advert__content-place[^>]*>([^<]+)', block)

        ads.append({
            "id": ad_id,
            "url": BASE_URL + url_m.group(1) if url_m else "",
            "title": title_m.group(1).strip() if title_m else "",
            "price": re.sub(r'<[^>]+>', '', price_m.group(1)).strip() if price_m else "",
            "location": place_m.group(1).strip() if place_m else "",
        })
    return ads


def get_total_pages(html):
    """Find the last page number from pagination links."""
    pages = re.findall(r'page=(\d+)', html)
    return max(int(p) for p in pages) if pages else 1


def scrape_all_listings():
    """Crawl all listing pages and return a list of ads."""
    print("Fetching page 1...")
    html = fetch_page(BASE_URL + LISTING_PATH)
    if not html:
        print("ERROR: Could not fetch first listing page.", file=sys.stderr)
        sys.exit(1)

    total_pages = get_total_pages(html)
    if MAX_PAGES:
        total_pages = min(total_pages, MAX_PAGES)
    print(f"Total pages to scrape: {total_pages}")

    all_ads = parse_listing_page(html)
    print(f"  Page 1: {len(all_ads)} ads")

    for page in range(2, total_pages + 1):
        time.sleep(DELAY_BETWEEN_PAGES)
        url = f"{BASE_URL}{LISTING_PATH}?page={page}"
        print(f"Fetching page {page}/{total_pages}...")
        html = fetch_page(url)
        if not html:
            print(f"  WARNING: Failed to fetch page {page}, skipping.")
            continue
        page_ads = parse_listing_page(html)
        print(f"  Page {page}: {len(page_ads)} ads")
        all_ads.extend(page_ads)

    seen = set()
    unique = []
    for ad in all_ads:
        if ad["id"] not in seen:
            seen.add(ad["id"])
            unique.append(ad)
    print(f"\nTotal unique ads: {len(unique)}")
    return unique


def _parse_listing_attributes(html):
    """Extract key-value attributes from a listing page (area, zone, etc.)."""
    attrs = {}
    for m in re.finditer(
        r'class="key-chars">(.*?)</span>.*?class="value-chars"[^>]*>(.*?)</',
        html, re.S,
    ):
        key = re.sub(r'<[^>]+>', '', m.group(1)).strip().rstrip(':').lower()
        val = re.sub(r'<[^>]+>', '', m.group(2)).strip()
        if val:
            attrs[key] = val
    return attrs


def extract_details_from_ad(ad):
    """Fetch an individual ad page and extract coords + listing attributes."""
    result = {"lat": None, "lng": None, "listing_area_m2": None,
              "listing_zone": None, "listing_type": None}
    if not ad["url"]:
        return result
    html = fetch_page(ad["url"])
    if not html:
        return result

    m = re.search(r'data-default-lat="([0-9.]+)"\s+data-default-lng="([0-9.]+)"', html)
    if m:
        result["lat"] = float(m.group(1))
        result["lng"] = float(m.group(2))

    attrs = _parse_listing_attributes(html)

    area_str = attrs.get("plot area", "")
    area_m = re.search(r'([\d.,]+)\s*m', area_str)
    if area_m:
        result["listing_area_m2"] = int(area_m.group(1).replace(".", "").replace(",", ""))

    result["listing_zone"] = attrs.get("planning zone", "")
    result["listing_type"] = attrs.get("plot type", attrs.get("land type", ""))

    return result


# ── ArcGIS enrichment ──────────────────────────────────────────────────────
_substation_cache = None


def get_substation_names():
    global _substation_cache
    if _substation_cache is not None:
        return _substation_cache
    params = {
        "where": "1=1",
        "outFields": "SUBSTATIONNAMEEL,SUBSTATIONNAMEEN,SCADASUBSTSHORTID,"
                      "HostingCapacityNet_MW,REStotal_MW,AvailableCapacity_MW",
        "returnGeometry": "false",
        "f": "json",
        "resultRecordCount": "100",
    }
    resp = requests.get(SUBSTATION_URL, params=params, timeout=15)
    data = resp.json()
    result = {}
    for feat in data.get("features", []):
        a = feat["attributes"]
        if a.get("SCADASUBSTSHORTID"):
            result[a["SCADASUBSTSHORTID"].strip()] = a
    _substation_cache = result
    return result


def find_substation(lat, lng):
    delta = 0.001
    geometry = json.dumps({
        "xmin": lng - delta, "ymin": lat - delta,
        "xmax": lng + delta, "ymax": lat + delta,
        "spatialReference": {"wkid": 4326},
    })
    params = {
        "geometry": geometry,
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "SCADASUBSTSHORTID",
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        resp = requests.get(POLYGON_URL, params=params, timeout=15)
        data = resp.json()
        feats = data.get("features", [])
        return feats[0]["attributes"]["SCADASUBSTSHORTID"] if feats else None
    except Exception:
        return None


def dls_query(layer_id, out_fields, lat, lng):
    params = {
        "geometry": f"{lng},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": out_fields,
        "returnGeometry": "false",
        "f": "json",
    }
    try:
        resp = requests.get(f"{DLS_BASE}/{layer_id}/query", params=params, timeout=15)
        data = resp.json()
        feats = data.get("features", [])
        return feats[0]["attributes"] if feats else {}
    except Exception:
        return {}


def enrich_ad(ad, subst_names):
    """Add substation + DLS data to an ad dict."""
    lat, lng = ad.get("lat"), ad.get("lng")
    if lat is None or lng is None:
        return

    short_id = find_substation(lat, lng)
    if short_id:
        info = subst_names.get(short_id, {})
        ad["substation_el"] = info.get("SUBSTATIONNAMEEL", short_id)
        ad["substation_en"] = info.get("SUBSTATIONNAMEEN", short_id)
        ad["substation_id"] = short_id
        ad["hosting_capacity_mw"] = info.get("HostingCapacityNet_MW", "")
        ad["res_total_mw"] = info.get("REStotal_MW", "")
        ad["available_capacity_mw"] = info.get("AvailableCapacity_MW", "")

    parcel = dls_query(0, "PARCEL_NBR,SHEET,PLAN_NBR,BLCK_CODE,SHAPE.STArea()", lat, lng)
    ad["parcel_number"] = parcel.get("PARCEL_NBR", "")
    ad["sheet"] = parcel.get("SHEET", "")
    ad["plan"] = parcel.get("PLAN_NBR", "")
    ad["block"] = parcel.get("BLCK_CODE", "")
    ad["parcel_area_m2"] = round(parcel["SHAPE.STArea()"]) if parcel.get("SHAPE.STArea()") else ""

    zone = dls_query(12, "PLNZNT_NAME,PLNZNT_DESC", lat, lng)
    ad["planning_zone"] = zone.get("PLNZNT_NAME", "")
    ad["planning_zone_desc"] = zone.get("PLNZNT_DESC", "")

    muni = dls_query(16, "VIL_NM_E", lat, lng)
    ad["municipality"] = muni.get("VIL_NM_E", "")

    district = dls_query(15, "DIST_NM_E", lat, lng)
    ad["district"] = district.get("DIST_NM_E", "")


# ── Road proximity (Overpass / OSM) ──────────────────────────────────────────
import numpy as np

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
ROAD_SEARCH_RADIUS = 500  # metres
ROAD_TYPES = "motorway|trunk|primary|secondary|tertiary|residential"

# Cyprus bounding box (with margin)
CY_BBOX = (34.4, 32.0, 35.8, 34.7)  # south, west, north, east


def _haversine(lat1, lon1, lat2, lon2):
    """Distance in metres between two WGS-84 points."""
    R = 6_371_000
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ / 2) ** 2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _haversine_np(lat1, lon1, lat2_arr, lon2_arr):
    """Vectorised haversine: one point vs arrays. Returns distances in metres."""
    R = 6_371_000
    φ1 = np.radians(lat1)
    φ2 = np.radians(lat2_arr)
    Δφ = np.radians(lat2_arr - lat1)
    Δλ = np.radians(lon2_arr - lon1)
    a = np.sin(Δφ / 2) ** 2 + np.cos(φ1) * np.cos(φ2) * np.sin(Δλ / 2) ** 2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))


_road_cache = None


def _load_cyprus_roads():
    """Download all Cyprus road nodes in one Overpass query. Returns
    (node_lats, node_lngs, node_road_type) as numpy arrays + a list."""
    global _road_cache
    if _road_cache is not None:
        return _road_cache

    s, w, n, e = CY_BBOX
    query = (
        f'[out:json][timeout:120];'
        f'way["highway"~"{ROAD_TYPES}"]({s},{w},{n},{e});'
        f'(._;>;);out body qt;'
    )
    print("  Downloading Cyprus road network from OSM (one-time)...")
    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=180)
    data = resp.json()

    nodes = {}
    way_nodes = {}  # node_id -> highway type
    for el in data.get("elements", []):
        if el["type"] == "node":
            nodes[el["id"]] = (el["lat"], el["lon"])
        elif el["type"] == "way":
            hw = el.get("tags", {}).get("highway", "")
            for nid in el.get("nodes", []):
                if nid not in way_nodes:
                    way_nodes[nid] = hw

    lats, lngs, types = [], [], []
    for nid, hw in way_nodes.items():
        if nid in nodes:
            lat, lon = nodes[nid]
            lats.append(lat)
            lngs.append(lon)
            types.append(hw)

    print(f"  Loaded {len(lats):,} road nodes")
    _road_cache = (np.array(lats), np.array(lngs), types)
    return _road_cache


def compute_road_distances(ads):
    """Batch-compute nearest road distance for all ads with coordinates."""
    to_process = [a for a in ads if a.get("lat") is not None]
    if not to_process:
        return

    node_lats, node_lngs, node_types = _load_cyprus_roads()
    if len(node_lats) == 0:
        return

    for i, ad in enumerate(to_process):
        dists = _haversine_np(ad["lat"], ad["lng"], node_lats, node_lngs)
        idx = np.argmin(dists)
        best_dist = dists[idx]
        if best_dist <= ROAD_SEARCH_RADIUS:
            ad["road_distance_m"] = round(float(best_dist))
            ad["road_type"] = node_types[idx]
        else:
            ad["road_distance_m"] = ""
            ad["road_type"] = ""
        if (i + 1) % 50 == 0 or (i + 1) == len(to_process):
            print(f"  Road distances: {i+1}/{len(to_process)}")


# ── Terrain slope (Open-Meteo elevation API) ────────────────────────────────
OPEN_METEO_ELEV_URL = "https://api.open-meteo.com/v1/elevation"
SLOPE_SAMPLE_OFFSET = 0.001  # ~111m at equator, ~91m at 35°N


def _batch_elevations(coords):
    """Fetch elevations for a list of (lat, lng) tuples. Returns list of floats."""
    elevations = []
    for i in range(0, len(coords), 100):
        chunk = coords[i:i + 100]
        lats = ",".join(f"{c[0]:.6f}" for c in chunk)
        lngs = ",".join(f"{c[1]:.6f}" for c in chunk)
        try:
            resp = requests.get(
                OPEN_METEO_ELEV_URL,
                params={"latitude": lats, "longitude": lngs},
                timeout=15,
            )
            data = resp.json()
            elevations.extend(data.get("elevation", [None] * len(chunk)))
        except Exception:
            elevations.extend([None] * len(chunk))
    return elevations


def compute_slopes(ads):
    """Batch-compute slope for all ads that have coordinates.
    Sets slope_pct and slope_class on each ad."""
    coords = []
    indexed_ads = []
    for ad in ads:
        lat, lng = ad.get("lat"), ad.get("lng")
        if lat is None or lng is None:
            continue
        indexed_ads.append(ad)
        d = SLOPE_SAMPLE_OFFSET
        coords.extend([
            (lat, lng),
            (lat + d, lng),   # N
            (lat - d, lng),   # S
            (lat, lng + d),   # E
            (lat, lng - d),   # W
        ])

    if not coords:
        return

    elevs = _batch_elevations(coords)

    for i, ad in enumerate(indexed_ads):
        base = i * 5
        e_center = elevs[base]
        e_n = elevs[base + 1]
        e_s = elevs[base + 2]
        e_e = elevs[base + 3]
        e_w = elevs[base + 4]

        if any(v is None for v in (e_center, e_n, e_s, e_e, e_w)):
            continue

        lat = ad["lat"]
        h_ns = _haversine(lat - SLOPE_SAMPLE_OFFSET, ad["lng"],
                          lat + SLOPE_SAMPLE_OFFSET, ad["lng"])
        h_ew = _haversine(lat, ad["lng"] - SLOPE_SAMPLE_OFFSET,
                          lat, ad["lng"] + SLOPE_SAMPLE_OFFSET)

        slope_ns = abs(e_n - e_s) / h_ns * 100 if h_ns > 0 else 0
        slope_ew = abs(e_e - e_w) / h_ew * 100 if h_ew > 0 else 0
        max_slope = round(max(slope_ns, slope_ew), 1)

        ad["slope_pct"] = max_slope
        if max_slope < 5:
            ad["slope_class"] = "flat"
        elif max_slope < 15:
            ad["slope_class"] = "moderate"
        else:
            ad["slope_class"] = "steep"


# ── Price parsing & derived metrics ──────────────────────────────────────────
def parse_price_eur(price_str):
    """Parse a Bazaraki price string like '€3.900.000' into a numeric value.
    For ranges like '€185.000  €195.000', returns the first (lower) price."""
    if not price_str:
        return None
    first = price_str.split("€")[1] if "€" in price_str else price_str
    first = first.strip().split("€")[0].split()[0]
    first = first.replace(".", "").replace(",", "").strip()
    try:
        return int(first)
    except ValueError:
        return None


def compute_cost_per_sqm(ad):
    """Set price_numeric and cost_per_sqm on the ad dict.
    Uses listing_area_m2 (from the ad page) as primary, parcel_area_m2 (DLS) as fallback."""
    price = parse_price_eur(ad.get("price", ""))
    ad["price_numeric"] = price if price else ""
    area = ad.get("listing_area_m2") or ad.get("parcel_area_m2")
    if price and area and isinstance(area, (int, float)) and area > 0:
        ad["cost_per_sqm"] = round(price / area, 2)
    else:
        ad["cost_per_sqm"] = ""


# ── CSV output ──────────────────────────────────────────────────────────────
CSV_FIELDS = [
    "id", "url", "title", "price", "price_numeric", "location",
    "listing_area_m2", "listing_zone", "listing_type", "cost_per_sqm",
    "lat", "lng",
    "road_distance_m", "road_type", "slope_pct", "slope_class",
    "district", "municipality",
    "parcel_number", "sheet", "plan", "block", "parcel_area_m2",
    "planning_zone", "planning_zone_desc",
    "substation_el", "substation_en", "substation_id",
    "hosting_capacity_mw", "res_total_mw", "available_capacity_mw",
]


def write_csv(ads, filename):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ads)
    print(f"\nCSV written to {filename} ({len(ads)} rows)")


# ── Main ────────────────────────────────────────────────────────────────────
def main():
    # Step 1: Scrape all listings
    ads = scrape_all_listings()

    # Step 2: Extract coordinates + listing details from each ad page
    print("\nExtracting details from individual ad pages...")
    for i, ad in enumerate(ads):
        details = extract_details_from_ad(ad)
        ad.update(details)
        lat = ad.get("lat")
        area = ad.get("listing_area_m2", "")
        status = f"({lat:.6f}, {ad['lng']:.6f})" if lat else "(no coords)"
        print(f"  [{i+1}/{len(ads)}] {ad['id']} {status} area={area}m²")
        if i < len(ads) - 1:
            time.sleep(DELAY_BETWEEN_ADS)

    with_coords = [a for a in ads if a.get("lat") is not None]
    print(f"\nAds with coordinates: {len(with_coords)}/{len(ads)}")

    # Step 3: Enrich with substation + DLS data
    print("\nFetching substation names...")
    subst_names = get_substation_names()
    print(f"Loaded {len(subst_names)} substations")

    print(f"\nEnriching {len(with_coords)} ads with ArcGIS data...")
    with ThreadPoolExecutor(max_workers=ARCGIS_WORKERS) as pool:
        futures = {pool.submit(enrich_ad, ad, subst_names): ad for ad in with_coords}
        done = 0
        for future in as_completed(futures):
            done += 1
            ad = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"  WARNING: enrichment failed for {ad['id']}: {e}")
            if done % 50 == 0 or done == len(with_coords):
                print(f"  Enriched {done}/{len(with_coords)}")

    # Step 4: Batch terrain slope via Open-Meteo
    print("\nComputing terrain slopes...")
    compute_slopes(with_coords)
    slopes_done = sum(1 for a in with_coords if a.get("slope_pct") is not None)
    print(f"  Slopes computed for {slopes_done}/{len(with_coords)} ads")

    # Step 5: Road proximity (bulk OSM download + local computation)
    print("\nComputing road distances...")
    compute_road_distances(with_coords)

    # Step 6: Compute derived metrics
    for ad in ads:
        compute_cost_per_sqm(ad)

    # Step 7: Write CSV
    write_csv(ads, OUTPUT_FILE)


if __name__ == "__main__":
    main()
