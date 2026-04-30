import os
import json
import csv
import zipfile
import time
import re
import requests
import xml.etree.ElementTree as ET
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
from datetime import datetime, UTC
from urllib.parse import urlparse, parse_qs  # for REST pagination of drafts
from pathlib import Path



# -------------------- Config & setup --------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")
WHOAMI = os.getenv("WHOAMI")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")

if not API_KEY or not WHOAMI or not SHOPIFY_TOKEN or not SHOPIFY_STORE:
    raise ValueError("Missing API_KEY, WHOAMI, SHOPIFY_TOKEN, or SHOPIFY_STORE in .env")

SHOPIFY_BASE = f"https://{SHOPIFY_STORE.strip('/')}" if not SHOPIFY_STORE.startswith("http") else SHOPIFY_STORE.rstrip("/")
SHOPIFY_API_VERSION = "2024-10"
HTTP_TIMEOUT = 25

SERVER_LIMIT = 50
RETRY_COUNT = 3
RETRY_SLEEP_SECS = 2

LOG_TIMINGS = True
PO_WHITELIST = None  # or set([...]) for testing

REST_PAGE_LIMIT = 250
MAX_PAGES_PER_STATUS = 100
REST_PAGE_SLEEP = 0.03
REST_DEDUPE_ENABLED = True

EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

mt_headers = {"x-api-key": API_KEY, "Content-Type": "application/json", "Accept": "application/json"}
shopify_rest_headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

# -------------------- Discount settings --------------------
DISCOUNT_UNIT_PRICE = 5.00

# Include SKUs that should end up at $5 when they appear as assortment children or direct lines.
# (kept as-is for DIRECT LINES; assortments now infer price from the parent)
DISCOUNT_SKUS: set[str] = {
    "165005", "165006", "165007", "165008", "165009", "165012", "165014",
    "LL-16-3148", "LL-16-3149", "LL-16-3150", "LL-16-3151", "LL-16-3152", "LL-16-3153",
    "LL-16-3162", "LL-16-3200", "LL-16-3201", "LL-16-3202", "LL-16-3203", "LL-16-3204",
    "LL-16-3205", "LL-16-3206", "LL-16-3207", "LL-16-3222", "LL-16-3223", "LL-16-3224",
    "LL-16-3225", "LL-16-3243", "LL-16-3244", "LL-16-3245", "LL-16-3246", "LL-16-3247",
    "LL-16-3248", "LL-16-3249", "LL-16-3157", "LL-16-3158",
}

# -------------------- Utilities --------------------
def _t(label: str, start_ts: float | None):
    if LOG_TIMINGS and start_ts is not None:
        elapsed = (time.time() - start_ts) * 1000
        print(f"  · {label} took {elapsed:.0f} ms")

def request_with_retries(method, url, **kwargs):
    last = None
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.request(method, url, headers=mt_headers, timeout=120, **kwargs)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            resp = getattr(e, "response", None)
            if resp is not None:
                try:
                    print(f"[HTTP {resp.status_code}] {url}\n{resp.text[:400]}\n")
                except Exception:
                    pass
            if attempt < RETRY_COUNT:
                time.sleep(RETRY_SLEEP_SECS * attempt)
    raise last

def shopify_graphql(query: str, variables: dict | None = None):
    url = f"{SHOPIFY_BASE}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
    ts = time.time() if LOG_TIMINGS else None
    r = requests.post(
        url,
        headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"},
        json={"query": query, "variables": variables or {}},
        timeout=HTTP_TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    ext = data.get("extensions", {}) if isinstance(data, dict) else {}
    if ext.get("cost") and LOG_TIMINGS:
        actual = ext["cost"].get("actualQueryCost")
        throttle = ext["cost"].get("throttleStatus", {})
        print(f"  · GQL cost={actual}, throttle remaining={throttle.get('currentlyAvailable','?')}")
    _t("GraphQL call", ts)
    return data

_norm_hash_space = re.compile(r"[#\s]")
def norm_po(po: str | None) -> str:
    return _norm_hash_space.sub("", str(po or "").strip()).upper()

# normalize SKU types (int/float/string) to a canonical string
def norm_sku(v) -> str:
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v).strip()
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if v.is_integer():
            return str(int(v))
        return str(v).strip()
    return str(v).strip()

def parse_price(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s:
        return None
    cleaned = re.sub(r"[^0-9.\-]", "", s.replace(",", ""))
    if cleaned in ("", "-", ".", "-."):
        return None
    try:
        return float(Decimal(cleaned))
    except (InvalidOperation, ValueError):
        return None

def _money_round(val: float) -> float:
    return float(Decimal(str(val)).quantize(Decimal("0.01")))

def _nearly_equal(a: float | None, b: float | None, tol: float = 0.0001) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) <= tol

def export_rows_to_csv(rows):
    if not rows:
        return None
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(EXPORT_DIR, f"mt_to_shopify_exports_{ts}.csv")
    fields = ["mt_recordID", "shopify_draft_order_id", "poNumber", "company", "buyer_email", "line_item_count", "created_at"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path

def to_gid(kind: str, numeric_id: int | str) -> str:
    return f"gid://shopify/{kind}/{numeric_id}"

def _to_yyyy_mm_dd(val: str | None) -> str | None:
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        pass
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(1)), int(m.group(2))).date().isoformat()
        except Exception:
            pass
    return s[:10]

# sanitize phone for CompanyAddressInput (omit if invalid)
_digits = re.compile(r"\D+")
def normalize_phone_e164_us(phone: str | None) -> str | None:
    """
    Returns +1XXXXXXXXXX for valid-ish US 10-digit numbers.
    If blank/invalid -> None (caller should omit phone).
    """
    if not phone:
        return None
    raw = str(phone).strip()
    if not raw:
        return None
    raw = re.split(r"(ext\.?|x|#)", raw, flags=re.IGNORECASE)[0].strip()
    digits = _digits.sub("", raw)

    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"+1{digits}"

# -------------------- Email helpers (NEW) --------------------
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _split_email(email: str):
    if not email:
        return None, None
    e = email.strip()
    if "@" not in e:
        return e, None
    local, domain = e.rsplit("@", 1)
    return local, (domain.lower().strip() if domain else None)

def _is_basic_email(email: str) -> bool:
    # Basic sanity check only (no DNS lookups)
    if not email:
        return False
    return bool(_EMAIL_RE.match(email.strip()))

def _levenshtein(a: str, b: str) -> int:
    # Small, fast Levenshtein for short strings (domains)
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        cur = [i]
        for j, cb in enumerate(b, start=1):
            ins = cur[j - 1] + 1
            dele = prev[j] + 1
            sub = prev[j - 1] + (ca != cb)
            cur.append(min(ins, dele, sub))
        prev = cur
    return prev[-1]

def reconcile_emails(bill_email: str | None, ship_email: str | None) -> str | None:
    """
    If both are present and domains differ by a tiny typo (edit distance <= 2),
    rewrite ship_email to use bill_email domain. Also:
      - if ship_email malformed but bill_email valid => use bill_email
      - otherwise leave ship_email unchanged
    """
    ship_email = (ship_email or "").strip() or None
    bill_email = (bill_email or "").strip() or None

    if not ship_email:
        return ship_email
    if not bill_email:
        return ship_email

    bill_ok = _is_basic_email(bill_email)
    ship_ok = _is_basic_email(ship_email)

    if ship_ok and not bill_ok:
        return ship_email
    if bill_ok and not ship_ok:
        return bill_email

    bill_local, bill_domain = _split_email(bill_email)
    ship_local, ship_domain = _split_email(ship_email)

    if not bill_domain or not ship_domain:
        return ship_email
    if bill_domain == ship_domain:
        return ship_email

    if _levenshtein(bill_domain, ship_domain) <= 2:
        if ship_local:
            return f"{ship_local}@{bill_domain}"
        return bill_email

    return ship_email

# -------------------- Assortment expansion --------------------
ASSORTMENT_XLSX_PATH = os.getenv("ASSORTMENT_XLSX_PATH", "assortment-expansion.xlsx")

_XLSX_NS = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_XLSX_REL_NS = {"rel": "http://schemas.openxmlformats.org/package/2006/relationships"}

def _resolve_assortment_xlsx_path(path_value: str) -> str:
    path = Path(path_value)
    if path.is_absolute():
        return str(path)
    here = Path(__file__).resolve().parent
    return str(here / path)

def _xlsx_col_to_index(cell_ref: str) -> int:
    letters = re.sub(r"[^A-Z]", "", str(cell_ref or "").upper())
    idx = 0
    for ch in letters:
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1

def _xlsx_cell_value(cell, shared_strings: list[str]):
    cell_type = cell.attrib.get("t")
    if cell_type == "inlineStr":
        text_node = cell.find("main:is/main:t", _XLSX_NS)
        return text_node.text if text_node is not None else ""

    value_node = cell.find("main:v", _XLSX_NS)
    if value_node is None:
        return ""
    raw = value_node.text or ""

    if cell_type == "s":
        try:
            return shared_strings[int(raw)]
        except Exception:
            return raw

    return raw

def _load_first_sheet_rows(xlsx_path: str) -> list[list[str]]:
    with zipfile.ZipFile(xlsx_path) as zf:
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in zf.namelist():
            shared_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in shared_root.findall("main:si", _XLSX_NS):
                parts = [t.text or "" for t in si.findall(".//main:t", _XLSX_NS)]
                shared_strings.append("".join(parts))

        workbook_root = ET.fromstring(zf.read("xl/workbook.xml"))
        first_sheet = workbook_root.find("main:sheets/main:sheet", _XLSX_NS)
        if first_sheet is None:
            return []

        rel_id = first_sheet.attrib.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        rel_root = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))

        target = None
        for rel in rel_root.findall("rel:Relationship", _XLSX_REL_NS):
            if rel.attrib.get("Id") == rel_id:
                target = rel.attrib.get("Target")
                break

        if not target:
            return []

        target = target.lstrip("/")
        if not target.startswith("xl/"):
            target = f"xl/{target}"

        sheet_root = ET.fromstring(zf.read(target))
        rows: list[list[str]] = []
        for row in sheet_root.findall(".//main:sheetData/main:row", _XLSX_NS):
            values: list[str] = []
            for cell in row.findall("main:c", _XLSX_NS):
                idx = _xlsx_col_to_index(cell.attrib.get("r", "A1"))
                while len(values) <= idx:
                    values.append("")
                values[idx] = _xlsx_cell_value(cell, shared_strings)
            rows.append(values)
        return rows

def _header_key(value) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")

def load_assortment_map_from_xlsx(path_value: str = ASSORTMENT_XLSX_PATH) -> dict[str, list[tuple[str, int, float]]]:
    xlsx_path = _resolve_assortment_xlsx_path(path_value)
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(
            f"Assortment expansion file not found: {xlsx_path}. "
            "Add assortment-expansion.xlsx to the repo root or set ASSORTMENT_XLSX_PATH."
        )

    required = {"parent_sku", "child_sku", "child_qty_per_parent", "fallback_unit_price"}
    aliases = {
        "parent": "parent_sku",
        "parent_item_number": "parent_sku",
        "assortment_sku": "parent_sku",
        "sku": "parent_sku",
        "child": "child_sku",
        "child_item_number": "child_sku",
        "child_qty": "child_qty_per_parent",
        "quantity": "child_qty_per_parent",
        "qty": "child_qty_per_parent",
        "per_parent_qty": "child_qty_per_parent",
        "fallback_price": "fallback_unit_price",
        "unit_price": "fallback_unit_price",
        "price": "fallback_unit_price",
    }

    rows = _load_first_sheet_rows(xlsx_path)
    header_idx = None
    col_map: dict[str, int] = {}

    for i, row in enumerate(rows):
        normalized = [aliases.get(_header_key(v), _header_key(v)) for v in row]
        candidate = {name: idx for idx, name in enumerate(normalized) if name}
        if required.issubset(candidate.keys()):
            header_idx = i
            col_map = candidate
            break

    if header_idx is None:
        raise ValueError(
            "Could not find required assortment columns in Excel: "
            "parent_sku, child_sku, child_qty_per_parent, fallback_unit_price"
        )

    mapping: dict[str, list[tuple[str, int, float]]] = {}
    for row in rows[header_idx + 1:]:
        def get_col(name: str):
            idx = col_map[name]
            return row[idx] if idx < len(row) else ""

        parent_sku = norm_sku(get_col("parent_sku"))
        child_sku = norm_sku(get_col("child_sku"))
        if not parent_sku or not child_sku:
            continue

        qty_raw = get_col("child_qty_per_parent")
        try:
            child_qty = int(float(str(qty_raw).strip()))
        except Exception:
            raise ValueError(f"Invalid child_qty_per_parent for {parent_sku} -> {child_sku}: {qty_raw!r}")

        fallback_price = parse_price(get_col("fallback_unit_price"))
        if fallback_price is None:
            raise ValueError(f"Invalid fallback_unit_price for {parent_sku} -> {child_sku}")

        mapping.setdefault(parent_sku, []).append((child_sku, child_qty, _money_round(fallback_price)))

    if not mapping:
        raise ValueError("No assortment expansion rows were loaded from Excel.")

    total_children = sum(len(children) for children in mapping.values())
    print(f"Loaded {len(mapping)} assortment parent SKUs / {total_children} child rows from {xlsx_path}")
    return mapping

ASSORTMENT_MAP: dict[str, list[tuple[str, int, float]]] = load_assortment_map_from_xlsx()


def is_assortment_parent(sku: str | None) -> bool:
    return (sku or "") in ASSORTMENT_MAP

def expand_assortment_children(parent_sku: str, parent_qty: int) -> list[tuple[str, int, float]]:
    base = ASSORTMENT_MAP.get(parent_sku, [])
    q = int(parent_qty or 0)
    return [(child, per * q, fallback) for (child, per, fallback) in base]

# -------------------- Catalog helpers --------------------
_variant_by_sku_cache: dict[str, str] = {}
_price_by_sku_cache: dict[str, float] = {}

def find_variant_by_sku(sku: str) -> str | None:
    sku = norm_sku(sku)
    if not sku:
        return None
    if sku in _variant_by_sku_cache:
        return _variant_by_sku_cache[sku]
    q = """query($q: String!) { productVariants(first: 1, query: $q) { nodes { id sku } } }"""
    try:
        data = shopify_graphql(q, {"q": f"sku:{sku}"})
        nodes = (((data.get("data", {}) or {}).get("productVariants", {}) or {}).get("nodes", []) or [])
        vid = (nodes[0] or {}).get("id") if nodes else None
        if vid:
            _variant_by_sku_cache[sku] = vid
        return vid
    except Exception:
        return None

def get_shopify_price_by_sku(sku: str) -> float | None:
    """
    Return Shopify catalog variant price for SKU.
    """
    sku = norm_sku(sku)
    if not sku:
        return None
    if sku in _price_by_sku_cache:
        return _price_by_sku_cache[sku]

    q = """
    query($q: String!) {
      productVariants(first: 1, query: $q) {
        nodes { id sku price }
      }
    }"""
    try:
        data = shopify_graphql(q, {"q": f"sku:{sku}"})
        nodes = (((data.get("data", {}) or {}).get("productVariants", {}) or {}).get("nodes", []) or [])
        if not nodes:
            return None
        n0 = nodes[0] or {}
        if n0.get("id") and sku not in _variant_by_sku_cache:
            _variant_by_sku_cache[sku] = n0["id"]
        p = parse_price(n0.get("price"))
        if p is not None:
            _price_by_sku_cache[sku] = p
        return p
    except Exception:
        return None

# -------------------- Country/State normalizers --------------------
US_STATE_ABBR = {
    "alabama": "AL","alaska": "AK","arizona": "AZ","arkansas": "AR","california": "CA","colorado": "CO","connecticut": "CT",
    "delaware": "DE","district of columbia": "DC","florida": "FL","georgia": "GA","hawaii": "HI","idaho": "ID","illinois": "IL",
    "indiana": "IN","iowa": "IA","kansas": "KS","kentucky": "KY","louisiana": "LA","maine": "ME","maryland": "MD",
    "massachusetts": "MA","michigan": "MI","minnesota": "MN","mississippi": "MS","missouri": "MO","montana": "MT",
    "nebraska": "NE","nevada": "NV","new hampshire": "NH","new jersey": "NJ","new mexico": "NM","new york": "NY",
    "north carolina": "NC","north dakota": "ND","ohio": "OH","oklahoma": "OK","oregon": "OR","pennsylvania": "PA",
    "rhode island": "RI","south carolina": "SC","south dakota": "SD","tennessee": "TN","texas": "TX","utah": "UT",
    "vermont": "VT","virginia": "VA","washington": "WA","west virginia": "WV","wisconsin": "WI","wyoming": "WY",
}

def normalize_country(v: str | None) -> str | None:
    if not v:
        return None
    v = v.strip()
    if len(v) == 2:
        return v.upper()
    if v.lower().startswith("united states") or v.upper() in {"US", "USA"}:
        return "US"
    return v

def normalize_zone(country_code: str | None, state: str | None) -> str | None:
    if not state:
        return None
    cc = (country_code or "").upper()
    s = str(state).strip()
    if len(s) == 2:
        return s.upper()
    if cc == "US":
        return US_STATE_ABBR.get(s.lower()) or s
    return s

def _fix_countries(order: dict):
    original_ship = order.get("shipToCountry")
    if original_ship != "US":
        order["shipToCountry"] = "US"
        print(f"  · Forced shipToCountry '{original_ship}' → 'US' for PO {order.get('poNumber')}")

    bt = (order.get("billToCountry") or "").strip()
    if bt:
        bt_norm = normalize_country(bt)
        if (bt_norm or "").upper() == "US" and bt != "US":
            order["billToCountry"] = "US"
            print(f"  · Normalized billToCountry '{bt}' → 'US' for PO {order.get('poNumber')}")
        elif len(bt) == 2 and bt != bt.upper():
            order["billToCountry"] = bt.upper()

# -------------------- B2B helpers --------------------
_company_id_cache: dict[str, str] = {}
_location_id_cache: dict[tuple[str, str], str] = {}

def find_company_by_name(name: str) -> str | None:
    if not name:
        return None
    if name in _company_id_cache:
        return _company_id_cache[name]
    q = """query($q: String!) { companies(first: 10, query: $q) { edges { node { id name } } } }"""
    data = shopify_graphql(q, {"q": name})
    edges = (data.get("data", {}) or {}).get("companies", {}).get("edges", [])
    for e in edges:
        node = e.get("node", {})
        if (node.get("name", "").strip().lower() == name.strip().lower()):
            cid = node.get("id")
            if cid:
                _company_id_cache[name] = cid
                return cid
    return None

def create_company(name: str) -> str:
    m = """mutation($input: CompanyCreateInput!) { companyCreate(input: $input) { company { id name } userErrors { field message } } }"""
    data = shopify_graphql(m, {"input": {"company": {"name": name}}})
    errs = (data.get("data", {}) or {}).get("companyCreate", {}).get("userErrors", [])
    if errs:
        raise RuntimeError(f"companyCreate error: {errs}")
    company_id = (data.get("data", {}) or {}).get("companyCreate", {}).get("company", {}).get("id")
    if not company_id:
        raise RuntimeError("companyCreate returned no company id")
    return company_id

def ensure_company(name: str) -> str | None:
    if not name:
        return None
    cid = _company_id_cache.get(name)
    if cid:
        return cid
    cid = find_company_by_name(name) or create_company(name)
    if cid:
        _company_id_cache[name] = cid
    return cid

def to_company_address_input(order: dict, kind: str) -> dict:
    if kind == "billing":
        return {
            "address1": order.get("billToAddress1"),
            "address2": order.get("billToAddress2"),
            "city": order.get("billToCity"),
            "zip": order.get("billToZip"),
            "countryCode": normalize_country(order.get("billToCountry")) or "US",
            "zoneCode": normalize_zone(normalize_country(order.get("billToCountry")) or "US", order.get("billToState")),
            "recipient": f"{order.get('buyerFirstName') or ''} {order.get('buyerLastName') or ''}".strip() or None,
        }
    else:
        addr = {
            "address1": order.get("shipToAddress1"),
            "address2": order.get("shipToAddress2"),
            "city": order.get("shipToCity"),
            "zip": order.get("shipToZip"),
            "countryCode": normalize_country(order.get("shipToCountry")) or "US",
            "zoneCode": normalize_zone(normalize_country(order.get("shipToCountry")) or "US", order.get("shipToState")),
            "recipient": f"{order.get('buyerFirstName') or ''} {order.get('buyerLastName') or ''}".strip() or None,
        }
        p = normalize_phone_e164_us(order.get("shipToPhone"))
        if p:
            addr["phone"] = p
        return addr

def ensure_company_location(company_id: str, name: str, order: dict) -> str | None:
    key = (company_id, name or "Default")
    if key in _location_id_cache:
        return _location_id_cache[key]

    q = """
    query($id: ID!) {
      company(id: $id) {
        name
        locations(first: 50) {
          nodes {
            id
            name
            shippingAddress { address1 }
            billingAddress { address1 }
          }
        }
      }
    }"""
    data = shopify_graphql(q, {"id": company_id})
    company_node = ((data.get("data", {}) or {}).get("company", {}) or {})
    company_name = company_node.get("name") or ""
    nodes = (((company_node.get("locations", {}) or {}).get("nodes") or []))

    desired_name = name or "Default"

    for n in nodes:
        if (n.get("name") or "").strip() == desired_name.strip():
            lid = n.get("id")
            _location_id_cache[key] = lid
            try:
                ship_has = bool((n.get("shippingAddress") or {}).get("address1"))
                bill_has = bool((n.get("billingAddress") or {}).get("address1"))
                m = """mutation($locationId: ID!, $address: CompanyAddressInput!, $types: [CompanyAddressType!]!) {
                        companyLocationAssignAddress(locationId: $locationId, address: $address, addressTypes: $types) {
                          userErrors { field message } } }"""
                if not ship_has:
                    shopify_graphql(m, {"locationId": lid, "address": to_company_address_input(order,"shipping"), "types": ["SHIPPING"]})
                if not bill_has and order.get("billToAddress1"):
                    shopify_graphql(m, {"locationId": lid, "address": to_company_address_input(order,"billing"), "types": ["BILLING"]})
            except Exception:
                pass
            return lid

    if len(nodes) == 1:
        n = nodes[0]
        lid = n.get("id")
        current_name = (n.get("name") or "").strip()
        ship_has = bool((n.get("shippingAddress") or {}).get("address1"))
        bill_has = bool((n.get("billingAddress") or {}).get("address1"))
        is_generic_name = current_name.lower() in {company_name.strip().lower(), "default"}

        if is_generic_name and (not ship_has and not bill_has):
            try:
                m = """mutation($locationId: ID!, $address: CompanyAddressInput!, $types: [CompanyAddressType!]!) {
                        companyLocationAssignAddress(locationId: $locationId, address: $address, addressTypes: $types) {
                          userErrors { field message } } }"""
                shopify_graphql(m, {"locationId": lid, "address": to_company_address_input(order,"shipping"), "types": ["SHIPPING"]})
                if order.get("billToAddress1"):
                    shopify_graphql(m, {"locationId": lid, "address": to_company_address_input(order,"billing"), "types": ["BILLING"]})
            except Exception:
                pass

            try:
                mu = """
                mutation($id: ID!, $input: CompanyLocationUpdateInput!) {
                  companyLocationUpdate(id: $id, input: $input) {
                    companyLocation { id name }
                    userErrors { field message }
                  }
                }"""
                shopify_graphql(mu, {"id": lid, "input": {"name": desired_name}})
            except Exception:
                pass

            _location_id_cache[key] = lid
            return lid

    m = """mutation($companyId: ID!, $input: CompanyLocationInput!) {
      companyLocationCreate(companyId: $companyId, input: $input) {
        companyLocation { id name } userErrors { field message } } }"""
    loc_input = {
        "name": desired_name,
        "shippingAddress": to_company_address_input(order, "shipping"),
        "billingSameAsShipping": not bool(order.get("billToAddress1")),
    }
    if order.get("billToAddress1"):
        loc_input["billingAddress"] = to_company_address_input(order, "billing")

    data2 = shopify_graphql(m, {"companyId": company_id, "input": loc_input})
    errs = (((data2.get("data", {}) or {}).get("companyLocationCreate", {}) or {}).get("userErrors", []) or [])
    if errs:
        print(f"(non-blocking) companyLocationCreate errors: {errs}")
        return None
    lid = (((data2.get("data", {}) or {}).get("companyLocationCreate", {}) or {}).get("companyLocation", {}) or {}).get("id")
    if lid:
        _location_id_cache[key] = lid
    return lid

# ---- contacts (match the *same* customer) ----
def iterate_company_contacts(company_id: str):
    q = """
    query($id: ID!, $after: String) {
      company(id: $id) {
        contacts(first: 100, after: $after) {
          pageInfo { hasNextPage endCursor }
          nodes { id customer { id } }
        }
      }
    }"""
    after = None
    while True:
        d = shopify_graphql(q, {"id": company_id, "after": after})
        contacts = ((((d.get("data", {}) or {}).get("company", {}) or {}).get("contacts", {}) or {}))
        for n in (contacts.get("nodes") or []):
            yield (n.get("id"), (n.get("customer") or {}).get("id"))
        pi = contacts.get("pageInfo") or {}
        if pi.get("hasNextPage"):
            after = pi.get("endCursor")
        else:
            break

def get_or_create_matching_contact(company_id: str, customer_id_numeric: int | str) -> str | None:
    target_gid = to_gid("Customer", customer_id_numeric)
    for cid, cust_gid in iterate_company_contacts(company_id):
        if cust_gid == target_gid:
            return cid

    m = """
    mutation($companyId: ID!, $customerId: ID!) {
      companyAssignCustomerAsContact(companyId: $companyId, customerId: $customerId) {
        companyContact { id }
        userErrors { field message }
      }
    }"""
    out = shopify_graphql(m, {"companyId": company_id, "customerId": target_gid})
    payload = (out.get("data", {}) or {}).get("companyAssignCustomerAsContact", {}) or {}
    contact = (payload.get("companyContact") or {})
    if contact.get("id"):
        return contact["id"]

    errs = payload.get("userErrors", []) or []
    if any("already associated" in (e.get("message", "").lower()) for e in errs):
        for cid, cust_gid in iterate_company_contacts(company_id):
            if cust_gid == target_gid:
                return cid

    print(f"(non-blocking) companyAssignCustomerAsContact errors: {errs}")
    return None

# --- Contact Role helpers ---
_role_id_cache: dict[tuple[str, str], str] = {}

def get_company_role_id(company_id: str, role_name: str = "Ordering only") -> str | None:
    key = (company_id, role_name)
    if key in _role_id_cache:
        return _role_id_cache[key]
    q = """
    query($id: ID!) {
      company(id: $id) {
        contactRoles(first: 20) { nodes { id name } }
      }
    }"""
    d = shopify_graphql(q, {"id": company_id})
    nodes = ((((d.get("data", {}) or {}).get("company", {}) or {})
              .get("contactRoles", {}) or {}).get("nodes", []) or [])
    for n in nodes:
        if (n.get("name") or "").strip().lower() == role_name.strip().lower():
            _role_id_cache[key] = n.get("id")
            return n.get("id")
    return None

def grant_ordering_permission(company_contact_id: str, company_location_id: str, company_id: str):
    role_id = get_company_role_id(company_id, "Ordering only")
    if not (role_id and company_contact_id and company_location_id):
        print("(non-blocking) Skipping permission assignment — missing role/contact/location")
        return
    m = """
    mutation($companyContactId: ID!, $companyContactRoleId: ID!, $companyLocationId: ID!) {
      companyContactAssignRole(
        companyContactId: $companyContactId,
        companyContactRoleId: $companyContactRoleId,
        companyLocationId: $companyLocationId
      ) {
        companyContactRoleAssignment { id }
        userErrors { field message }
      }
    }"""
    out = shopify_graphql(m, {
        "companyContactId": company_contact_id,
        "companyContactRoleId": role_id,
        "companyLocationId": company_location_id,
    })
    errs = (((out.get("data", {}) or {}).get("companyContactAssignRole", {}) or {})
            .get("userErrors", []) or [])
    if errs and not any("already" in (e.get("message","").lower()) for e in errs):
        print(f"(non-blocking) companyContactAssignRole errors: {errs}")

# -------------------- MT fetch (POST + offset/limit) --------------------
def fetch_all_mt_orders() -> list[dict]:
    base = f"https://publicapi.markettime.com/mtpublic/api/v1/{WHOAMI}"
    url = f"{base}/orders/get"
    offset, limit = 0, SERVER_LIMIT
    total_reported = None
    seen, all_orders = set(), []

    while True:
        params = {"offset": offset, "limit": limit}
        resp = request_with_retries("POST", url, params=params, json=[])
        payload = resp.json() or {}
        batch = payload.get("response") or []

        try:
            total_reported = int(payload.get("total")) if payload.get("total") is not None else total_reported
        except Exception:
            pass

        if not batch:
            print(f"[orders/get] offset={offset} limit={limit} -> 0 records, stopping.")
            break

        new = 0
        for o in batch:
            key = o.get("recordID") or (o.get("poNumber"), o.get("retailerID"), o.get("orderDate"))
            if key not in seen:
                seen.add(key)
                all_orders.append(o)
                new += 1

        print(f"[orders/get] offset={offset} limit={limit} -> fetched={len(batch)} new={new} "
              f"total_so_far={len(all_orders)} (api_total={total_reported})")

        offset += limit
        if total_reported is not None and len(all_orders) >= total_reported:
            print(f"[orders/get] Reached API total={total_reported}, stopping.")
            break

    if all_orders:
        dates = [o.get("orderDate") for o in all_orders if o.get("orderDate")]
        if dates:
            print(f"MT total returned: {len(all_orders)} rows. Date span: {min(dates)} .. {max(dates)}")
        else:
            print(f"MT total returned: {len(all_orders)} rows.")
    return all_orders

# -------------------- PO de-dupe (HARDENED) --------------------
def _draft_exists_graphql(po: str, mt_record_id: str | None) -> bool:
    try:
        for pattern in (f'po_number:"{po}"', f"po_number:{po}"):
            q = """
            query($q: String!) {
              draftOrders(first: 1, query: $q) {
                edges { node { id poNumber tags } }
              }
            }"""
            res = shopify_graphql(q, {"q": pattern})
            edges = (((res.get("data", {}) or {}).get("draftOrders", {}) or {}).get("edges", []) or [])
            for e in edges:
                node = e.get("node", {}) or {}
                if norm_po(node.get("poNumber")) == norm_po(po):
                    print("  · Found existing draft by GraphQL PO search")
                    return True
                if mt_record_id:
                    tags = node.get("tags") or []
                    tag_blob = ",".join(tags) if isinstance(tags, list) else str(tags)
                    if f"mt_recordID:{mt_record_id}" in tag_blob:
                        print("  · Found existing draft by GraphQL tag search (mt_recordID)")
                        return True
        if mt_record_id:
            q2 = """
            query($q: String!) {
              draftOrders(first: 1, query: $q) {
                edges { node { id } }
              }
            }"""
            res2 = shopify_graphql(q2, {"q": f'tag:"mt_recordID:{mt_record_id}"'})
            if (((res2.get("data", {}) or {}).get("draftOrders", {}) or {}).get("edges", [])):
                print("  · Found existing draft by GraphQL tag query")
                return True
    except Exception:
        pass
    ret
