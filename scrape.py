#!/usr/bin/env python3
"""
Scrape all Bazaraki land-for-sale listings and enrich each with
substation (EAC) and cadastral (DLS Ktimatologio) data.
Outputs a CSV file.
"""

import csv
import json
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

    # Step 4: Compute derived metrics
    for ad in ads:
        compute_cost_per_sqm(ad)

    # Step 5: Write CSV
    write_csv(ads, OUTPUT_FILE)


if __name__ == "__main__":
    main()
