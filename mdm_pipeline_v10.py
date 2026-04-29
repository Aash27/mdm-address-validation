#!/usr/bin/env python3
"""
MDM Full Pipeline — v9 Unified Parallel
========================================
STEP 1  Address pre-processing  (clean, translate, normalize)
STEP 2  Address validation      (Azure Maps address search + correction fallbacks)
STEP 3  Filter                  (score=1.0 + VALID  →  company match)
STEP 4  Company match           (Tier1 Fuzzy → Tier2 POI → Tier3 OpenAI web search)

All 4 steps run per-record inside ONE worker node.
10 worker threads process records in parallel.
Records are independent — each thread owns one record end-to-end.

Usage
-----
  pip install httpx pandas tqdm python-dotenv openai rapidfuzz langdetect deep-translator
  python mdm_pipeline_v9.py                         # full run
  python mdm_pipeline_v9.py --test                  # first 10 records only (worker assignment test)
  python mdm_pipeline_v9.py --input my_data.csv     # custom input
"""

import os, re, json, time, logging, csv, argparse, unicodedata
from typing import Optional, Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import threading

import httpx
import pandas as pd
from tqdm import tqdm
from dotenv import load_dotenv
from openai import OpenAI

try:
    from rapidfuzz import fuzz as rfuzz
    HAVE_RAPIDFUZZ = True
except ImportError:
    HAVE_RAPIDFUZZ = False

try:
    from langdetect import detect, LangDetectException
    HAVE_LANGDETECT = True
except ImportError:
    HAVE_LANGDETECT = False

try:
    from deep_translator import GoogleTranslator
    HAVE_TRANSLATOR = True
except ImportError:
    HAVE_TRANSLATOR = False

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ═══════════════════════════════════════════════════════════════════════════════
# ██  CONFIG  — edit here
# ═══════════════════════════════════════════════════════════════════════════════
AZURE_MAPS_KEY = os.getenv("AZURE_MAPS_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

INPUT_CSV       = "100_sample_MDM.csv"      # ← your raw input
OUTPUT_CSV      = "mdm_pipeline_results.csv"

MAX_WORKERS     = 10     # parallel worker threads
ADDR_DELAY      = 0.0    # sleep between address API calls (set 0.2 if hitting rate limits)
COMPANY_DELAY   = 0.0    # sleep between company API calls
BATCH_SIZE      = 10     # print summary banner every N completed records
TEST_MODE_ROWS  = 10     # how many rows to run in --test mode

# Address validation
DELIMITER            = "|#|#"
ADDR_FIELDS          = ["street", "city", "state", "country", "zip"]
ADDR_CONFIDENCE_THR  = 0.6
ADDRESS_URL          = "https://atlas.microsoft.com/search/address/json"

# Company match
FUZZY_URL            = "https://atlas.microsoft.com/search/fuzzy/json"
POI_URL              = "https://atlas.microsoft.com/search/poi/json"
GEOCODE_URL          = "https://atlas.microsoft.com/search/address/json"
MODEL_NAME           = "gpt-4.1-mini"
NAME_SIM_THRESHOLD   = 0.72
ADDR_SIM_THRESHOLD   = 0.55
REGION_THRESHOLDS    = {
    "CN":0.60,"JP":0.60,"KR":0.60,"SA":0.62,"AE":0.62,
    "TR":0.65,"RU":0.65,"IN":0.65,
}

# ═══════════════════════════════════════════════════════════════════════════════
# ██  OUTPUT COLUMNS
# ═══════════════════════════════════════════════════════════════════════════════
OUTPUT_COLUMNS = [
    # identifiers
    "MDM_KEY","SOURCE_NAME","FULL_ADDRESS_RAW",
    # address pre-processing
    "street","street_original","city","state","country","zip",
    "address_for_api","detected_lang","has_non_latin","street_translated",
    "mojibake_rescued","mojibake_fields",
    "pre_flags","skip_api",
    # address validation
    "addr_final_status","addr_correction_strategy",
    "val_score","val_match_type",
    "val_returned_street","val_returned_city","val_returned_state",
    "val_returned_country","val_returned_zip","val_returned_freeform",
    "val_lat","val_lon",
    # filter decision
    "routing_decision",
    "sent_to_company_match",
    # company match
    "match_status","company_exists","tier_used",
    "canonical_name","website",
    "all_known_locations","locations_in_state",
    "best_address","best_street","best_city","best_state","best_zip",
    "mdm_address_occupant","nearest_location_reasoning",
    "address_match","ai_confidence","ai_evidence",
    "fuzzy_match_name","fuzzy_similarity","fuzzy_found_address",
    "poi_match_name","poi_similarity",
    # meta
    "worker_id","pipeline_latency_sec",
]

if not OPENAI_API_KEY: raise ValueError("OPENAI_API_KEY missing from .env")
if not AZURE_MAPS_KEY: raise ValueError("AZURE_MAPS_KEY missing from .env")
oai_client = OpenAI(api_key=OPENAI_API_KEY)

# ═══════════════════════════════════════════════════════════════════════════════
# ██  THREAD-LOCAL HTTP CLIENT  (connection pooling — 2-3x faster Azure calls)
# ═══════════════════════════════════════════════════════════════════════════════
_tl = threading.local()

def http() -> httpx.Client:
    if not hasattr(_tl, "client"):
        _tl.client = httpx.Client(timeout=15, http2=False)
    return _tl.client

def safe_get(url: str, params: dict, retries: int = 3) -> httpx.Response:
    for attempt in range(retries):
        try:
            r = http().get(url, params=params)
            r.raise_for_status()
            return r
        except Exception as e:
            if attempt == retries - 1: raise
            time.sleep(0.5 * (2 ** attempt))

# ═══════════════════════════════════════════════════════════════════════════════
# ██  CACHE  (skip repeat company+address pairs)
# ═══════════════════════════════════════════════════════════════════════════════
_cache: Dict[tuple, Dict] = {}
_cache_lock = threading.Lock()

def cache_get(k):
    with _cache_lock: return _cache.get(k)

def cache_set(k, v):
    with _cache_lock: _cache[k] = v

# Name-first cache — keyed by (cleaned_name.lower(), country.upper())
# High hit rate expected: many MDM records share companies (e.g., Tj Maxx 01607, 01608)
_name_cache: Dict[tuple, Dict] = {}
_name_cache_lock = threading.Lock()

def name_cache_get(k):
    with _name_cache_lock: return _name_cache.get(k)

def name_cache_set(k, v):
    with _name_cache_lock: _name_cache[k] = v

# ═══════════════════════════════════════════════════════════════════════════════
# ██  WRITE QUEUE  (dedicated writer thread — no lock contention)
# ═══════════════════════════════════════════════════════════════════════════════
_wq       = Queue()
_cl       = threading.Lock()    # counter lock
_SENTINEL = object()

ABBREV_MAP = {
    r"\bSt\b":"Street", r"\bBlvd\b":"Boulevard", r"\bRd\b":"Road",
    r"\bAve?\b":"Avenue", r"\bDr\b":"Drive", r"\bLn\b":"Lane",
    r"\bPkwy\b":"Parkway", r"\bCt\b":"Court", r"\bPl\b":"Place",
    r"\bHwy\b":"Highway", r"\bSq\b":"Square", r"\bFwy\b":"Freeway",
    r"\bAv\.\b":"Avenida", r"\bC/\b":"Calle",
}

# ═══════════════════════════════════════════════════════════════════════════════
# ██  HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def norm(v) -> str:
    return re.sub(r"\s+", " ", str(v or "").strip())

def safe_float(x):
    try:
        s = str(x or "").strip().lower()
        return None if s in ("","nan","none","null") else float(x)
    except: return None

def token_sim(a: str, b: str) -> float:
    a, b = norm(a).lower(), norm(b).lower()
    if not a or not b: return 0.0
    if a == b: return 1.0
    if HAVE_RAPIDFUZZ:
        return rfuzz.token_set_ratio(a, b) / 100.0
    if a in b or b in a: return 0.92
    at = set(re.sub(r"[^\w\s]","",a).split())
    bt = set(re.sub(r"[^\w\s]","",b).split())
    if not at or not bt: return 0.0
    if at.issubset(bt) or bt.issubset(at): return 0.92
    return len(at & bt) / max(len(at), len(bt))

def get_threshold(country: str) -> float:
    return REGION_THRESHOLDS.get(country.upper(), NAME_SIM_THRESHOLD)

def strip_legal(n: str) -> str:
    return re.sub(
        r"\b(llc|inc|corp|ltd|lp|llp|s\.a\.|s\.l\.|gmbh|bv|nv|plc|pty|co\.|company|"
        r"limited|incorporated|corporation|ltda|sas|srl|spa|ag|kg|s\.a\.s\.|s\.r\.l\.|s\.p\.a\.)\b\.?",
        "", n, flags=re.IGNORECASE).strip(" ,.")

def clean_company(n: str) -> str:
    n = re.sub(r"\b(run rate|recap|actuals|budget|forecast|ytd|mtd|qtd|variance|plan|"
               r"baseline|accrual|reforecast|outlook|headcount)\b", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s+[A-Z]{1,4}\d{3,}\b", "", n, flags=re.IGNORECASE)
    n = re.sub(r"\s+\d{4,}\b", "", n)
    n = re.sub(r"\b(accounts payable|accounts receivable|a/p|a/r|corporate office|"
               r"head office|main office|c/o|attn|procurement|purchasing)\b", "", n, flags=re.IGNORECASE)
    n = re.sub(r"[-–—/]\s*$", "", n)
    n = re.sub(r"\s{2,}", " ", n).strip(" ,.-")
    return n or n

def has_non_latin(t: str) -> bool:
    for ch in (t or ""):
        nm = unicodedata.name(ch, "")
        if any(s in nm for s in ["CJK","ARABIC","CYRILLIC","HEBREW","THAI","HANGUL","HIRAGANA","KATAKANA","DEVANAGARI"]):
            return True
    return False

def detect_lang(t: str) -> str:
    if not HAVE_LANGDETECT or not t or len(t.strip()) < 3: return "en"
    try: return detect(t)
    except: return "en"

def translate_en(t: str) -> str:
    if not HAVE_TRANSLATOR or not t or len(t.strip()) < 3: return t
    try: return GoogleTranslator(source="auto", target="en").translate(t) or t
    except: return t

def normalize_abbrevs(t: str) -> str:
    for pat, rep in ABBREV_MAP.items():
        t = re.sub(pat, rep, t, flags=re.IGNORECASE)
    return t

def clean_field(t) -> str:
    if not isinstance(t, str): return ""
    t = unicodedata.normalize("NFC", t)
    t = re.sub(r"[\x00-\x1f\x7f]", " ", t)
    return re.sub(r"\s+", " ", t).strip()

# Canonical mojibake: a UTF-8 multi-byte lead (U+00C2..U+00EF) decoded as
# Latin-1 leaves a "lead char" followed by a continuation byte (U+0080..U+00BF).
_MOJIBAKE_PAIR_RE = re.compile(r"[Â-ï][-¿]")

def _rescue_mojibake(s: str):
    """Detect and reverse UTF-8 → Latin-1 / cp1252 encoding corruption.
    Returns (possibly_fixed_string, was_rescued).

    Detection: count canonical 'lead+continuation' mojibake pairs. If the
    rescue eliminates all/most of them (and produces valid UTF-8), accept.
    Works for Latin-diacritic (Málaga, Lübecker) AND non-Latin script
    (Chinese, Cyrillic, Arabic, etc.)."""
    if not s or not isinstance(s, str):
        return s, False
    pair_count = len(_MOJIBAKE_PAIR_RE.findall(s))
    if pair_count == 0:
        return s, False
    for src_enc in ("latin-1", "cp1252"):
        try:
            rescued = s.encode(src_enc, errors="strict").decode("utf-8", errors="strict")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        new_pair_count = len(_MOJIBAKE_PAIR_RE.findall(rescued))
        if new_pair_count < pair_count:
            return rescued, True
    return s, False

def _is_garbage_street(s: str) -> bool:
    """Detect streets that are unusable for geocoding (codes, internal docs, single
    generic words, etc.). City/state/country are kept regardless — only the street
    is dropped when this fires."""
    s = (s or "").strip()
    if not s: return True
    if re.search(r"\b(VAT|EORI|TIN|EIN|GST|TVA|RFC|CUIT|NIT)\s*[A-Z0-9]+", s, re.I): return True
    if re.fullmatch(r"[A-Z0-9\-_/.\s]{1,15}", s): return True
    if s.lower() in {"st","street","road","rd","ave","avenue","main","na","n/a","--","-","none","null"}: return True
    if re.match(r"^(goods receipt|loading dock|warehouse|gate|building|piso|nave|unit|suite|room)\s*\d*\b", s, re.I): return True
    if re.fullmatch(r"[\s\-_/.,]+", s): return True
    return False

def _is_generic_name(name: str) -> bool:
    """Reject company names too generic to web-search productively."""
    n = (name or "").strip().lower()
    if not n or len(n) <= 2: return True
    if n in {"company","corp","corporation","inc","ltd","limited","llc","sa","gmbh",
             "the company","na","n/a","unknown","tbd"}: return True
    if re.fullmatch(r"[\d\s\-_/.,]+", n): return True
    return False

# ═══════════════════════════════════════════════════════════════════════════════
# ██  STEP 1 — ADDRESS PRE-PROCESSING
# ═══════════════════════════════════════════════════════════════════════════════
def preprocess_address(row: pd.Series) -> dict:
    raw   = row.get("FULL_ADDRESS", "")
    parts = [clean_field(p) for p in (raw.split(DELIMITER) if isinstance(raw, str) else [])]
    comp  = {f: (parts[i] if i < len(parts) else "") for i, f in enumerate(ADDR_FIELDS)}

    # Rescue mojibake (UTF-8 mis-decoded as Latin-1/cp1252) before any further
    # processing. Applied per-field so a corrupted street doesn't poison the rest.
    rescued_fields = []
    for f in ADDR_FIELDS:
        fixed, was_rescued = _rescue_mojibake(comp[f])
        if was_rescued:
            comp[f] = fixed
            rescued_fields.append(f)
    mojibake_rescued = bool(rescued_fields)

    sl        = detect_lang(comp["street"])
    nl        = has_non_latin(comp["street"])
    orig      = ""
    translated= False

    if sl != "en" or nl:
        orig             = comp["street"]
        comp["street"]   = translate_en(comp["street"])
        translated       = True

    comp["street"] = normalize_abbrevs(comp["street"])

    addr_for_api = ", ".join(p for p in [
        comp["street"], comp["city"], comp["state"], comp["zip"], comp["country"]
    ] if p)

    flags = []
    if not comp["street"]:  flags.append("MISSING_STREET")
    if not comp["city"]:    flags.append("MISSING_CITY")
    if not comp["country"]: flags.append("MISSING_COUNTRY")
    if comp["street"] and re.fullmatch(r"[A-Z0-9\-_]{1,10}", comp["street"]):
        flags.append("POSSIBLE_PLACEHOLDER_STREET")
    if not comp["zip"] and comp["country"] in ["US","CA","GB","DE","FR"]:
        flags.append("MISSING_ZIP")
    if comp["street"] and _is_garbage_street(comp["street"]):
        flags.append("GARBAGE_STREET")
    if mojibake_rescued:
        flags.append("MOJIBAKE_RESCUED")

    skip = "MISSING_STREET" in flags and "MISSING_CITY" in flags

    return {
        "MDM_KEY":          row.get("MDM_KEY", ""),
        "SOURCE_NAME":      clean_field(str(row.get("SOURCE_NAME", ""))),
        "FULL_ADDRESS_RAW": raw,
        "street":           comp["street"],
        "street_original":  orig,
        "city":             comp["city"],
        "state":            comp["state"],
        "country":          comp["country"],
        "zip":              comp["zip"],
        "address_for_api":  addr_for_api,
        "detected_lang":    sl,
        "has_non_latin":    nl,
        "street_translated":translated,
        "mojibake_rescued": mojibake_rescued,
        "mojibake_fields":  ",".join(rescued_fields),
        "pre_flags":        "|".join(flags) if flags else "OK",
        "skip_api":         skip,
    }

# ═══════════════════════════════════════════════════════════════════════════════
# ██  STEP 2 — ADDRESS VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════
def _addr_search(query: str, country: str) -> dict:
    params = {
        "subscription-key": AZURE_MAPS_KEY, "api-version": "1.0",
        "query": query, "limit": 1, "language": "en-US",
    }
    if country and len(country) == 2:
        params["countrySet"] = country.upper()
    try:
        r   = safe_get(ADDRESS_URL, params)
        res = r.json().get("results", [])
        if not res: return {"raw_status": "NO_RESULT", "score": 0.0}
        top  = res[0]
        sc   = top.get("score", 0.0)
        mt   = top.get("type", "")
        addr = top.get("address", {})
        pos  = top.get("position", {})
        if sc >= ADDR_CONFIDENCE_THR and mt in ["Point Address","Address Range","Street"]:
            rs = "VALID"
        elif sc >= ADDR_CONFIDENCE_THR:
            rs = "VALID_LOW_PRECISION"
        elif sc >= 0.3:
            rs = "LOW_CONFIDENCE"
        else:
            rs = "INVALID"
        return {
            "raw_status":        rs, "score": round(sc,4), "match_type": mt,
            "returned_street":   addr.get("streetNameAndNumber", addr.get("streetName","")),
            "returned_city":     addr.get("municipality",""),
            "returned_state":    addr.get("countrySubdivision",""),
            "returned_country":  addr.get("countryCode",""),
            "returned_zip":      addr.get("postalCode",""),
            "returned_freeform": addr.get("freeformAddress",""),
            "lat": pos.get("lat",""), "lon": pos.get("lon",""),
        }
    except Exception as e:
        return {"raw_status": "API_ERROR", "error": str(e), "score": 0.0}

def validate_address(rec: dict) -> dict:
    if rec["skip_api"]:
        return {
            "addr_final_status":"SKIPPED_NO_ADDRESS","addr_correction_strategy":"N/A",
            "val_score":0.0,"val_match_type":"",
            "val_returned_street":"","val_returned_city":"","val_returned_state":"",
            "val_returned_country":"","val_returned_zip":"","val_returned_freeform":"",
            "val_lat":"","val_lon":"",
        }

    result = _addr_search(rec["address_for_api"], rec["country"])
    if result.get("raw_status") not in ("VALID","VALID_LOW_PRECISION"):
        # fallback strategies
        for strategy, query in [
            ("DROP_STREET",    ", ".join(p for p in [rec["city"],rec["state"],rec["zip"],rec["country"]] if p)),
            ("DROP_STATE_ZIP", ", ".join(p for p in [rec["street"],rec["city"],rec["country"]] if p)),
            ("CITY_COUNTRY",   ", ".join(p for p in [rec["city"],rec["country"]] if p)),
        ]:
            if not query.strip(): continue
            r2 = _addr_search(query, rec["country"])
            r2["correction_strategy"] = strategy
            if r2.get("raw_status") in ("VALID","VALID_LOW_PRECISION"):
                result = r2
                break
        else:
            result["correction_strategy"] = result.get("correction_strategy","ALL_FALLBACKS_FAILED")
            result["raw_status"] = result.get("raw_status","MANUAL_REVIEW_NEEDED")

    if ADDR_DELAY: time.sleep(ADDR_DELAY)

    return {
        "addr_final_status":      result.get("raw_status",""),
        "addr_correction_strategy": result.get("correction_strategy","PRIMARY"),
        "val_score":              result.get("score",0.0),
        "val_match_type":         result.get("match_type",""),
        "val_returned_street":    result.get("returned_street",""),
        "val_returned_city":      result.get("returned_city",""),
        "val_returned_state":     result.get("returned_state",""),
        "val_returned_country":   result.get("returned_country",""),
        "val_returned_zip":       result.get("returned_zip",""),
        "val_returned_freeform":  result.get("returned_freeform",""),
        "val_lat":                result.get("lat",""),
        "val_lon":                result.get("lon",""),
    }

# ═══════════════════════════════════════════════════════════════════════════════
# ██  STEP 3 — ROUTE  (FULL_MATCH | NAME_FIRST | SKIP)
# ═══════════════════════════════════════════════════════════════════════════════
ADDR_GATE_SCORE = 0.85   # business floor — do not lower without approval

def route(rec: dict, addr_result: dict) -> str:
    """Three-way decision after address validation:
      FULL_MATCH  — address passed 0.85 gate as-is, run Tier 1→2→3 with full address
      NAME_FIRST  — address unreliable but name+geo are usable, search by name
      SKIP        — neither address nor name usable
    """
    score    = float(addr_result.get("val_score", 0.0) or 0.0)
    status   = str(addr_result.get("addr_final_status", "")).strip().upper()
    strategy = addr_result.get("addr_correction_strategy", "PRIMARY")
    flags    = rec.get("pre_flags", "")

    # Strong address: passes gate AND street wasn't fallback-dropped AND not garbage
    if (score >= ADDR_GATE_SCORE
        and status in ("VALID", "VALID_LOW_PRECISION")
        and strategy == "PRIMARY"
        and "GARBAGE_STREET" not in flags):
        return "FULL_MATCH"

    # Address is weak/dropped — try name-first if we have a usable name + some geo
    cleaned  = clean_company(rec.get("SOURCE_NAME", ""))
    has_name = bool(cleaned) and not _is_generic_name(cleaned)
    has_geo  = bool(rec.get("city")) or bool(rec.get("country"))

    if has_name and has_geo:
        return "NAME_FIRST"
    return "SKIP"

def should_send_to_company_match(addr_result: dict) -> bool:
    """Kept for backwards-compatibility; prefer route() in new code."""
    score = float(addr_result.get("val_score", 0.0) or 0.0)
    status = str(addr_result.get("addr_final_status", "")).strip().upper()
    return score >= ADDR_GATE_SCORE and status in ("VALID", "VALID_LOW_PRECISION")

# ═══════════════════════════════════════════════════════════════════════════════
# ██  STEP 4 — COMPANY MATCH  (Tier 1 → Tier 2 → Tier 3)
# ═══════════════════════════════════════════════════════════════════════════════
def _geocode(freeform: str, country: str):
    params = {"subscription-key":AZURE_MAPS_KEY,"api-version":"1.0",
              "query":freeform,"limit":1,"language":"en-US"}
    if country and len(country)==2: params["countrySet"]=country.upper()
    try:
        r = safe_get(GEOCODE_URL, params)
        res = r.json().get("results",[])
        if res:
            pos = res[0].get("position",{})
            return pos.get("lat"), pos.get("lon")
    except: pass
    return None, None

def _fuzzy(company, addr, lat, lon, thr) -> dict:
    freeform = addr.get("freeform","")
    country  = addr.get("country","")
    base     = strip_legal(company)
    all_res  = []
    queries  = [
        f"{company} {addr.get('street','')} {addr.get('city','')}",
        f"{company} {addr.get('city','')} {addr.get('state','')}",
        f"{base} {addr.get('city','')} {addr.get('state','')}",
        f"{base} {country}",
    ]
    for q in queries:
        for radius in ([500,2000,20000] if lat and lon else [None]):
            params = {"subscription-key":AZURE_MAPS_KEY,"api-version":"1.0",
                      "query":q,"limit":5,"language":"en-US",
                      "idxSet":"POI,PAD,Addr","maxFuzzyLevel":2}
            if lat and lon and radius: params.update({"lat":lat,"lon":lon,"radius":radius})
            if country and len(country)==2: params["countrySet"]=country.upper()
            try:
                r = safe_get(FUZZY_URL, params)
                res = r.json().get("results",[])
                all_res.extend(res)
                if res: break
            except: pass
    if not all_res: return {"status":"NOT_FOUND"}
    best, bsim = None, -1
    for r in all_res:
        cname = r.get("poi",{}).get("name","") or r.get("address",{}).get("freeformAddress","")
        s = token_sim(company, cname)
        if s > bsim: bsim, best = s, r
    poi = best.get("poi",{}); addr2 = best.get("address",{}); pos = best.get("position",{})
    fa  = norm(addr2.get("freeformAddress",""))
    asim = token_sim(freeform, fa)
    verd = "CORRECT" if asim >= ADDR_SIM_THRESHOLD else ("PARTIAL" if asim >= 0.30 else "WRONG")
    return {"status":"FOUND","name":norm(poi.get("name","")),"name_sim":round(bsim,4),
            "address":fa,"addr_sim":round(asim,4),"addr_verdict":verd,
            "phone":norm(poi.get("phone","")),"url":norm(poi.get("url","")),
            "lat":pos.get("lat",""),"lon":pos.get("lon","")}

def _poi(company, addr, lat, lon, thr) -> dict:
    country = addr.get("country","")
    base    = strip_legal(company)
    all_res = []
    for query in [company, base]:
        for radius in ([500,5000,50000] if lat and lon else [None]):
            params = {"subscription-key":AZURE_MAPS_KEY,"api-version":"1.0",
                      "query":query,"limit":5,"language":"en-US","idxSet":"POI"}
            if lat and lon and radius: params.update({"lat":lat,"lon":lon,"radius":radius})
            if country and len(country)==2: params["countrySet"]=country.upper()
            try:
                r = safe_get(POI_URL, params)
                res = r.json().get("results",[])
                all_res.extend(res)
                if res: break
            except: pass
    if not all_res: return {"status":"NOT_FOUND"}
    best, bsim = None, -1
    for r in all_res:
        s = token_sim(company, r.get("poi",{}).get("name",""))
        if s > bsim: bsim, best = s, r
    return {"status":"FOUND","name":norm(best.get("poi",{}).get("name","")),"name_sim":round(bsim,4),
            "address":norm(best.get("address",{}).get("freeformAddress","")),
            "phone":norm(best.get("poi",{}).get("phone","")),"url":norm(best.get("poi",{}).get("url",""))}

def _occupant(lat, lon, country) -> str:
    if lat is None or lon is None: return ""
    try:
        params = {"subscription-key":AZURE_MAPS_KEY,"api-version":"1.0",
                  "query":"business","limit":3,"language":"en-US",
                  "lat":lat,"lon":lon,"radius":100}
        if country and len(country)==2: params["countrySet"]=country.upper()
        r   = safe_get(POI_URL, params)
        res = r.json().get("results",[])
        nms = [norm(x.get("poi",{}).get("name","")) for x in res if x.get("poi",{}).get("name","")]
        return " | ".join(nms[:3])
    except: return ""

SYSTEM_PROMPT = """You are a senior Master Data Management (MDM) analyst with expertise in:
- Global company name disambiguation across 50+ languages and scripts
- Address validation for multilingual, transliterated, and non-Latin records
- Firmographic data enrichment from authoritative web sources

LANGUAGE & SCRIPT RULES:
1. Company names may be in ANY language (Arabic, Chinese, Russian, Japanese, Korean, Hindi, etc.)
2. Transliterated names (e.g. "Al Faisaliah" = الفيصلية) count as matches
3. Legal suffixes vary by country — match them using local conventions
4. For non-English records, ALWAYS search both the original AND the English translation

VERIFICATION — Chain-of-Thought:
Step 1: Identify the language/script of the company name
Step 2: Generate 3-5 search variants (original, translated, transliterated, abbreviated)
Step 3: Search for each variant + city/country to find official presence
Step 4: Cross-reference found address against MDM address
Step 5: Apply the 3-case decision rule

3-CASE DECISION RULE:
CASE 1 — Company EXISTS at MDM address → company_exists=true, address_match=CORRECT
CASE 2 — Company NOT at MDM address but address is occupied → fill mdm_address_occupant
CASE 3 — Company exists elsewhere → list ALL locations, apply nearest-location rule

ANTI-HALLUCINATION RULES (STRICT):
- Never invent addresses, websites, or phone numbers
- If uncertain, set confidence=LOW
- Only report what you found in actual search results
"""

OUTPUT_SCHEMA = {
    "type":"json_schema","name":"company_verification","schema":{
        "type":"object",
        "properties":{
            "company_exists":             {"type":["boolean","null"]},
            "canonical_name":             {"type":["string","null"]},
            "website":                    {"type":["string","null"]},
            "all_locations":              {"type":"string"},
            "locations_in_state":         {"type":"string"},
            "best_address":               {"type":["string","null"]},
            "real_street":                {"type":["string","null"]},
            "real_city":                  {"type":["string","null"]},
            "real_state":                 {"type":["string","null"]},
            "real_zip":                   {"type":["string","null"]},
            "mdm_address_occupant":       {"type":["string","null"]},
            "nearest_location_reasoning": {"type":["string","null"]},
            "address_match":              {"type":"string","enum":["CORRECT","PARTIAL","WRONG","UNKNOWN"]},
            "confidence":                 {"type":"string","enum":["HIGH","MEDIUM","LOW"]},
            "evidence":                   {"type":"string"},
        },
        "required":["company_exists","canonical_name","website","all_locations",
                    "locations_in_state","best_address","real_street","real_city",
                    "real_state","real_zip","mdm_address_occupant",
                    "nearest_location_reasoning","address_match","confidence","evidence"],
        "additionalProperties":False
    }
}

def _openai_verify(company, cleaned, addr, lat, lon, azure_ev) -> dict:
    prompt = f"""
<mdm_record>
  <original_name>{company}</original_name>
  <cleaned_name>{cleaned}</cleaned_name>
  <street>{addr.get("street","")}</street>
  <city>{addr.get("city","")}</city>
  <state>{addr.get("state","")}</state>
  <zip>{addr.get("zip","")}</zip>
  <country>{addr.get("country","")}</country>
  <full_address>{addr.get("freeform","")}</full_address>
  <coordinates>lat={lat}, lon={lon}</coordinates>
</mdm_record>
<azure_pre_search>{json.dumps(azure_ev, indent=2)}</azure_pre_search>
<execute_these_searches_in_order>
SEARCH 1: "{cleaned}" + "{addr.get("city","")}" "{addr.get("country","")}"
SEARCH 2: "{cleaned}" official website OR headquarters
SEARCH 3: "{addr.get("street","")}" "{addr.get("city","")}" — what business is at this address?
SEARCH 4: If not found — try translated/transliterated name variants
NEAREST LOCATION RULE: same state as "{addr.get("state","")}" → always pick first.
OUTPUT: Fill all JSON fields. Cite sources in evidence.
</execute_these_searches_in_order>"""
    try:
        rsp = oai_client.responses.create(
            model=MODEL_NAME,
            input=[{"role":"system","content":SYSTEM_PROMPT},{"role":"user","content":prompt}],
            tools=[{"type":"web_search_preview"}],
            tool_choice="required",
            text={"format":OUTPUT_SCHEMA},
        )
        return json.loads(rsp.output_text)
    except Exception as e:
        logging.error(f"OpenAI failed [{company}]: {e}")
        return {"company_exists":None,"canonical_name":None,"website":None,
                "all_locations":"","locations_in_state":"","best_address":None,
                "real_street":None,"real_city":None,"real_state":None,"real_zip":None,
                "mdm_address_occupant":None,"nearest_location_reasoning":None,
                "address_match":"UNKNOWN","confidence":"LOW","evidence":f"OpenAI error: {e}"}

def _empty_company_result(status: str = "", evidence: str = "") -> dict:
    """All company-match output keys, populated empty. Used by skip paths and as
    the base for both match_company and match_company_name_first."""
    return {
        "match_status": status, "company_exists": "", "tier_used": "",
        "canonical_name": "", "website": "", "all_known_locations": "",
        "locations_in_state": "", "best_address": "", "best_street": "",
        "best_city": "", "best_state": "", "best_zip": "", "mdm_address_occupant": "",
        "nearest_location_reasoning": "", "address_match": "",
        "ai_confidence": "", "ai_evidence": evidence,
        "fuzzy_match_name": "", "fuzzy_similarity": "", "fuzzy_found_address": "",
        "poi_match_name": "", "poi_similarity": "",
    }

def _ai_to_result_fields(ai: dict, occupant: str, tier: str) -> dict:
    """Translate OpenAI verify response into the company-match output schema."""
    exists = ai.get("company_exists")
    match  = ai.get("address_match", "UNKNOWN")
    if   exists is True  and match == "CORRECT":            status = "COMPANY_FOUND"
    elif exists is True  and match in ("PARTIAL","WRONG"):  status = "COMPANY_FOUND_DIFF_ADDR"
    elif exists is False:                                   status = "COMPANY_NOT_FOUND"
    else:                                                   status = "UNVERIFIED"
    return {
        "match_status":               status,
        "company_exists":             exists,
        "tier_used":                  tier,
        "canonical_name":             ai.get("canonical_name","") or "",
        "website":                    ai.get("website","") or "",
        "all_known_locations":        ai.get("all_locations","") or "",
        "locations_in_state":         ai.get("locations_in_state","") or "",
        "best_address":               ai.get("best_address","") or "",
        "best_street":                ai.get("real_street","") or "",
        "best_city":                  ai.get("real_city","") or "",
        "best_state":                 ai.get("real_state","") or "",
        "best_zip":                   ai.get("real_zip","") or "",
        "mdm_address_occupant":       (ai.get("mdm_address_occupant","") or "") or occupant,
        "nearest_location_reasoning": ai.get("nearest_location_reasoning","") or "",
        "address_match":              match,
        "ai_confidence":              ai.get("confidence","") or "",
        "ai_evidence":                ai.get("evidence","") or "",
    }

def match_company(rec: dict, addr_result: dict) -> dict:
    """Run Tier1 → Tier2 → Tier3 company match on a pre-validated record."""
    company  = norm(rec.get("SOURCE_NAME",""))
    cleaned  = clean_company(company)
    country  = rec.get("country","")
    freeform = addr_result.get("val_returned_freeform","") or rec.get("address_for_api","")
    thr      = get_threshold(country)

    addr = {
        "street":  rec.get("street",""),
        "city":    rec.get("city",""),
        "state":   rec.get("state",""),
        "country": country,
        "zip":     rec.get("zip",""),
        "freeform":freeform,
    }

    ck = (cleaned.lower(), freeform.lower())
    cached = cache_get(ck)
    if cached:
        r = dict(cached)
        r["match_status"] = r.get("match_status","") + "_CACHED"
        return r

    lat = safe_float(addr_result.get("val_lat")) 
    lon = safe_float(addr_result.get("val_lon"))
    if lat is None or lon is None:
        lat, lon = _geocode(freeform, country)

    base = _empty_company_result()

    # Tier 1
    fz = _fuzzy(company, addr, lat, lon, thr)
    base["fuzzy_match_name"]    = fz.get("name","")
    base["fuzzy_similarity"]    = fz.get("name_sim","")
    base["fuzzy_found_address"] = fz.get("address","")
    if fz.get("status")=="FOUND" and fz.get("name_sim",0)>=thr and fz.get("addr_verdict")=="CORRECT":
        base.update({"match_status":"COMPANY_FOUND","company_exists":True,"tier_used":"TIER1_FUZZY",
                     "canonical_name":fz["name"],"best_address":fz["address"],
                     "address_match":"CORRECT","ai_confidence":"HIGH"})
        cache_set(ck, base); return base

    # Tier 2
    po = _poi(company, addr, lat, lon, thr)
    base["poi_match_name"]  = po.get("name","")
    base["poi_similarity"]  = po.get("name_sim","")
    if po.get("status")=="FOUND" and po.get("name_sim",0)>=thr:
        base.update({"match_status":"COMPANY_FOUND","company_exists":True,"tier_used":"TIER2_POI",
                     "canonical_name":po["name"],"best_address":po.get("address",""),
                     "address_match":"PARTIAL","ai_confidence":"MEDIUM"})
        cache_set(ck, base); return base

    # Tier 3 — skip if no address AND no Azure signal
    if fz.get("name_sim",0) < 0.5 and po.get("name_sim",0) < 0.5 and not freeform:
        base.update({"match_status":"COMPANY_NOT_FOUND","company_exists":False,
                     "tier_used":"SKIPPED_NO_ADDR","ai_confidence":"LOW",
                     "ai_evidence":"No address + no Azure match — OpenAI skipped"})
        cache_set(ck, base); return base

    occ = _occupant(lat, lon, country)
    ai  = _openai_verify(company, cleaned, addr, lat, lon,
                         {"fuzzy":fz,"poi":po,"occupant":occ,"coords":{"lat":lat,"lon":lon}})
    base.update(_ai_to_result_fields(ai, occ, "TIER3_AI"))
    cache_set(ck, base)
    if COMPANY_DELAY: time.sleep(COMPANY_DELAY)
    return base

# ═══════════════════════════════════════════════════════════════════════════════
# ██  STEP 4b — NAME-FIRST MATCH (when address fails the 0.85 gate)
# ═══════════════════════════════════════════════════════════════════════════════
def match_company_name_first(rec: dict) -> dict:
    """Address is unusable (low confidence or garbage street). Search by company
    name + whatever geographic info we have (city/state/country/zip), and let
    OpenAI find the canonical address. Skips Azure POI/Fuzzy entirely."""
    company = norm(rec.get("SOURCE_NAME", ""))
    cleaned = clean_company(company)
    country = rec.get("country", "")
    city    = rec.get("city", "")
    state   = rec.get("state", "")
    zipc    = rec.get("zip", "")
    flags   = rec.get("pre_flags", "")

    if not cleaned or _is_generic_name(cleaned):
        return _empty_company_result(
            status="SKIPPED_NO_USABLE_NAME",
            evidence="Cleaned name empty or too generic for web search",
        )

    # Cache by (cleaned_name, country) — same company in same country reuses lookup
    nk = (cleaned.lower(), country.upper())
    cached = name_cache_get(nk)
    if cached:
        r = dict(cached)
        r["match_status"] = r.get("match_status", "") + "_NAME_CACHED"
        return r

    # Drop ONLY the street if garbage; keep city/state/country/zip
    street = "" if "GARBAGE_STREET" in flags else rec.get("street", "")
    parts    = [p for p in [street, city, state, zipc, country] if p]
    freeform = ", ".join(parts)

    addr = {
        "street":   street,
        "city":     city,
        "state":    state,
        "country":  country,
        "zip":      zipc,
        "freeform": freeform,
    }

    azure_ev = {
        "mode":   "name_first",
        "note":   "Original street was unusable (low Azure confidence or GARBAGE_STREET).",
        "kept":   {"city": city, "state": state, "country": country, "zip": zipc},
        "dropped": {"street_was_garbage": "GARBAGE_STREET" in flags},
    }
    ai = _openai_verify(company, cleaned, addr, None, None, azure_ev)

    result = _empty_company_result()
    result.update(_ai_to_result_fields(ai, "", "TIER3_NAME_FIRST"))
    name_cache_set(nk, result)
    if COMPANY_DELAY: time.sleep(COMPANY_DELAY)
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# ██  WORKER NODE  — all 4 steps for ONE record
# ═══════════════════════════════════════════════════════════════════════════════
def process_record(row: pd.Series) -> dict:
    t0        = time.time()
    worker_id = threading.current_thread().name   # e.g. "mdm_0", "mdm_1" ...

    # ── Step 1: pre-process ──────────────────────────────────────────────────
    rec = preprocess_address(row)

    # ── Step 2: validate address ─────────────────────────────────────────────
    addr_res = validate_address(rec)

    # ── Step 3: route ────────────────────────────────────────────────────────
    decision = route(rec, addr_res)
    send     = (decision == "FULL_MATCH")

    # ── Step 4: company match (path depends on routing decision) ─────────────
    if decision == "FULL_MATCH":
        comp_res = match_company(rec, addr_res)
    elif decision == "NAME_FIRST":
        comp_res = match_company_name_first(rec)
    else:  # SKIP
        comp_res = _empty_company_result(
            status="SKIPPED_NO_USABLE_DATA",
            evidence=f"Address below {ADDR_GATE_SCORE} confidence and name unusable for web search",
        )

    elapsed = round(time.time() - t0, 2)

    # ── Merge everything into one flat record ─────────────────────────────────
    out = {
        "MDM_KEY":          rec["MDM_KEY"],
        "SOURCE_NAME":      rec["SOURCE_NAME"],
        "FULL_ADDRESS_RAW": rec["FULL_ADDRESS_RAW"],
        # step 1
        "street":           rec["street"],
        "street_original":  rec["street_original"],
        "city":             rec["city"],
        "state":            rec["state"],
        "country":          rec["country"],
        "zip":              rec["zip"],
        "address_for_api":  rec["address_for_api"],
        "detected_lang":    rec["detected_lang"],
        "has_non_latin":    rec["has_non_latin"],
        "street_translated":rec["street_translated"],
        "mojibake_rescued": rec["mojibake_rescued"],
        "mojibake_fields":  rec["mojibake_fields"],
        "pre_flags":        rec["pre_flags"],
        "skip_api":         rec["skip_api"],
        # step 2
        **{f"val_{k}" if not k.startswith("val_") and not k.startswith("addr_") else k: v
           for k, v in addr_res.items()},
        # step 3
        "routing_decision":      decision,
        "sent_to_company_match": send,
        # step 4
        "match_status":               comp_res["match_status"],
        "company_exists":             comp_res["company_exists"],
        "tier_used":                  comp_res["tier_used"],
        "canonical_name":             comp_res["canonical_name"],
        "website":                    comp_res["website"],
        "all_known_locations":        comp_res["all_known_locations"],
        "locations_in_state":         comp_res["locations_in_state"],
        "best_address":               comp_res["best_address"],
        "best_street":                comp_res["best_street"],
        "best_city":                  comp_res["best_city"],
        "best_state":                 comp_res["best_state"],
        "best_zip":                   comp_res["best_zip"],
        "mdm_address_occupant":       comp_res["mdm_address_occupant"],
        "nearest_location_reasoning": comp_res["nearest_location_reasoning"],
        "address_match":              comp_res["address_match"],
        "ai_confidence":              comp_res["ai_confidence"],
        "ai_evidence":                comp_res["ai_evidence"],
        "fuzzy_match_name":           comp_res["fuzzy_match_name"],
        "fuzzy_similarity":           comp_res["fuzzy_similarity"],
        "fuzzy_found_address":        comp_res["fuzzy_found_address"],
        "poi_match_name":             comp_res["poi_match_name"],
        "poi_similarity":             comp_res["poi_similarity"],
        # meta
        "worker_id":              worker_id,
        "pipeline_latency_sec":   elapsed,
    }

    # ── Worker-visible log line (shows thread assignment) ────────────────────
    print(f"  [{worker_id}] MDM={rec['MDM_KEY']:<12} | "
          f"addr={addr_res.get('addr_final_status','?'):<22} | "
          f"route={decision:<11} | "
          f"company={comp_res['match_status']:<30} | "
          f"{elapsed}s")

    return out

# ═══════════════════════════════════════════════════════════════════════════════
# ██  WRITER THREAD
# ═══════════════════════════════════════════════════════════════════════════════
def _writer(output_csv, open_mode, status_counts, routing_counts,
            processed_box, batch_box, in_batch_box, total, pbar):
    fh = open(output_csv, mode=open_mode, newline="", encoding="utf-8-sig")
    wr = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS, extrasaction="ignore")
    if open_mode == "w":
        wr.writeheader(); fh.flush()

    while True:
        try: item = _wq.get(timeout=2)
        except Empty: continue
        if item is _SENTINEL: break
        wr.writerow(item); fh.flush()

        with _cl:
            s = item.get("match_status","")
            status_counts[s] = status_counts.get(s,0) + 1
            r = item.get("routing_decision","")
            routing_counts[r] = routing_counts.get(r,0) + 1
            processed_box[0] += 1
            in_batch_box[0]  += 1
            if in_batch_box[0] >= BATCH_SIZE:
                batch_box[0] += 1
                _banner(batch_box[0], status_counts, routing_counts, processed_box[0], total)
                in_batch_box[0] = 0
        pbar.update(1)
    fh.close()

def _banner(bn, sc, rc, done, total):
    found  = sum(v for k,v in sc.items() if "COMPANY_FOUND" in k and "DIFF" not in k and "CACHED" not in k)
    diff   = sum(v for k,v in sc.items() if "DIFF_ADDR" in k)
    nf     = sum(v for k,v in sc.items() if "NOT_FOUND" in k)
    skip   = sum(v for k,v in sc.items() if "SKIPPED" in k)
    cached = sum(v for k,v in sc.items() if "CACHED" in k)
    full_m = rc.get("FULL_MATCH", 0)
    name_f = rc.get("NAME_FIRST", 0)
    skip_r = rc.get("SKIP", 0)
    pct    = 100*done/total if total else 0
    print("\n" + "▓"*70)
    print(f"  BATCH {bn} | {done}/{total} ({pct:.1f}%)  cache_hits={cached}")
    print("▓"*70)
    print(f"  ✅ COMPANY_FOUND           : {found}")
    print(f"  🔄 COMPANY_FOUND_DIFF_ADDR : {diff}")
    print(f"  ❌ COMPANY_NOT_FOUND       : {nf}")
    print(f"  ⏭  SKIPPED                : {skip}")
    print(f"  ⚡ CACHED                  : {cached}")
    print(f"  ─── ROUTING ─────────────────")
    print(f"  🛣  FULL_MATCH              : {full_m}")
    print(f"  🎯 NAME_FIRST              : {name_f}")
    print(f"  🚫 SKIP                    : {skip_r}")
    print("▓"*70 + "\n")

# ═══════════════════════════════════════════════════════════════════════════════
# ██  MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def run(input_csv: str, output_csv: str, test_mode: bool = False):
    df    = pd.read_csv(input_csv, encoding="utf-8-sig")
    total = len(df)
    n_workers = MAX_WORKERS

    if test_mode:
        df        = df.head(TEST_MODE_ROWS)
        total     = len(df)
        n_workers = min(MAX_WORKERS, total)   # don't spin more threads than records
        print(f"\n{'='*70}")
        print(f"  🧪 TEST MODE — {total} records × {n_workers} workers")
        print(f"  Each worker will log its thread name beside every record.")
        print(f"  You should see up to {n_workers} unique worker IDs below.")
        print(f"{'='*70}\n")
    else:
        print(f"\n{'='*70}")
        print(f"  🚀 FULL RUN — {total} records × {n_workers} workers")
        print(f"  rapidfuzz : {'✅' if HAVE_RAPIDFUZZ else '⚠️  not installed'}")
        print(f"  langdetect: {'✅' if HAVE_LANGDETECT else '⚠️  not installed'}")
        print(f"  translator: {'✅' if HAVE_TRANSLATOR else '⚠️  not installed'}")
        print(f"{'='*70}\n")

    # Resume
    already_done: set = set()
    file_exists = os.path.isfile(output_csv) and not test_mode
    if file_exists:
        try:
            done_df      = pd.read_csv(output_csv, encoding="utf-8-sig", usecols=["MDM_KEY"])
            already_done = set(str(v) for v in done_df["MDM_KEY"].dropna())
            logging.info(f"Resume — {len(already_done)} already done, skipping.")
        except: file_exists = False

    open_mode = "a" if file_exists and already_done else "w"
    rows = [row for _, row in df.iterrows()
            if str(row.get("MDM_KEY","")) not in already_done]
    logging.info(f"Records to process: {len(rows)}")

    sc = {}
    rc = {}
    pb = [len(already_done)]
    bb = [0]; ib = [0]
    pbar = tqdm(total=total, initial=len(already_done), desc="Pipeline", unit="rec")

    wt = threading.Thread(
        target=_writer,
        args=(output_csv, open_mode, sc, rc, pb, bb, ib, total, pbar),
        daemon=True, name="csv-writer"
    )
    wt.start()

    with ThreadPoolExecutor(max_workers=n_workers, thread_name_prefix="mdm") as ex:
        futs = {ex.submit(process_record, row): row for row in rows}
        for fut in as_completed(futs):
            try:    _wq.put(fut.result())
            except Exception as e:
                logging.error(f"Worker crash: {e}")
                pbar.update(1)

    _wq.put(_SENTINEL)
    wt.join()
    pbar.close()

    if ib[0] > 0:
        bb[0] += 1
        _banner(bb[0], sc, rc, pb[0], total)

    print(f"\n✅  Output → {output_csv}")
    print(f"   Cache size      : {len(_cache)} addr-keyed pairs")
    print(f"   Name-cache size : {len(_name_cache)} (name, country) pairs")
    for k, v in sorted(sc.items(), key=lambda x:-x[1]):
        print(f"   {k:<45} {v:>5}  ({100*v/pb[0]:.1f}%)" if pb[0] else f"   {k}: {v}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MDM Full Pipeline v9")
    parser.add_argument("--test",   action="store_true", help=f"Run first {TEST_MODE_ROWS} records only")
    parser.add_argument("--input",  default=INPUT_CSV,   help="Input CSV path")
    parser.add_argument("--output", default=OUTPUT_CSV,  help="Output CSV path")
    args = parser.parse_args()

    run(input_csv=args.input, output_csv=args.output, test_mode=args.test)
