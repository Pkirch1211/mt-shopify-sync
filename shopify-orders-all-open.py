import os
import json
import csv
import time
import re
import requests
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
from datetime import datetime, UTC
from urllib.parse import urlparse, parse_qs  # for REST pagination of drafts

"""
Shopify <> MarketTime sync — hardened (POST + offset/limit paging)

Key changes in this version:
- Fix duplicate empty location: reuse Shopify's auto-created blank company location
  (assign addresses and attempt rename) instead of creating a second one.
- Still creates a new location only when a matching one already exists with
  a different name, or when the default stub can’t be repurposed safely.

Additional change (2025-08-19):
- Add Shopify Draft Order tags for MarketTime IDs needed by Flow write-back:
  - mt_recordID:<id>
  - mt_repGroupID:<id>
  - mt_retailerID:<id>

Additional change (2025-09-09):
- Map MarketTime -> Shopify Draft metafields:
  - b2b.ship_date  = order.shipDate (normalized to YYYY-MM-DD)
  - b2b.bill_to_email = order.billToEmail

Additional change (2025-09-13):
- **STRONG de-dupe** before creating a draft:
  1) Orders by po_number (GraphQL)
  2) Draft Orders by po_number / tag (GraphQL)
  3) Draft Orders paginated via REST (open + invoice_sent) with Link headers
"""

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

# MT tenant specifics
SERVER_LIMIT = 50          # confirmed cap for your tenant
RETRY_COUNT = 3
RETRY_SLEEP_SECS = 2

# Logging
LOG_TIMINGS = True

# Optional PO whitelist; set to set([...]) or None
PO_WHITELIST = None

# Shopify draft de-dupe scan controls
REST_PAGE_LIMIT = 250
MAX_PAGES_PER_STATUS = 100
REST_PAGE_SLEEP = 0.03

# Paths
EXPORT_DIR = "exports"
os.makedirs(EXPORT_DIR, exist_ok=True)

# Headers
mt_headers = {"x-api-key": API_KEY, "Content-Type": "application/json", "Accept": "application/json"}
shopify_rest_headers = {"X-Shopify-Access-Token": SHOPIFY_TOKEN, "Content-Type": "application/json"}

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

def _rest_get(path: str, params: dict | None = None) -> requests.Response:
    url = f"{SHOPIFY_BASE}/admin/api/{SHOPIFY_API_VERSION}/{path}"
    r = requests.get(url, headers=shopify_rest_headers, params=params or {}, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    return r

_norm_hash_space = re.compile(r"[#\s]")
def norm_po(po: str | None) -> str:
    return _norm_hash_space.sub("", str(po or "").strip()).upper()

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
    """Coerce common date strings (ISO, ISO+time, MM/DD/YYYY) to YYYY-MM-DD for metafields."""
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

# -------------------- Catalog helper --------------------
_variant_by_sku_cache: dict[str, str] = {}
def find_variant_by_sku(sku: str) -> str | None:
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
        return {
            "address1": order.get("shipToAddress1"),
            "address2": order.get("shipToAddress2"),
            "city": order.get("shipToCity"),
            "zip": order.get("shipToZip"),
            "countryCode": normalize_country(order.get("shipToCountry")) or "US",
            "zoneCode": normalize_zone(normalize_country(order.get("shipToCountry")) or "US", order.get("shipToState")),
            "phone": order.get("shipToPhone"),
            "recipient": f"{order.get('buyerFirstName') or ''} {order.get('buyerLastName') or ''}".strip() or None,
        }

def ensure_company_location(company_id: str, name: str, order: dict) -> str | None:
    """
    Find or create the company location.
    If the company only has a single, blank auto-created location, reuse it:
    assign addresses and try to rename to the desired 'name' (non-blocking)
    """
    key = (company_id, name or "Default")
    if key in _location_id_cache:
        return _location_id_cache[key]

    # 1) fetch existing locations incl. addresses
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

    # 1a) exact name match -> ensure addresses and return
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

    # 1b) single, blank, generic location -> REUSE it (assign + rename)
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

    # 2) create a fresh location with desired name
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
    """Yield (contactId, customerGID) across ALL contacts (paginated)."""
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

# --- Contact Role helpers (grant "Ordering only" on the location) ---
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
    if errs and not any("already" in (e.get("message","\n").lower()) for e in errs):
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
    """
    Fast, targeted GraphQL checks for draft orders by PO number and by tag.
    Tries both quoted and unquoted search to be robust with '#'.
    """
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
            # Search by explicit tag key:value (quoted)
            res2 = shopify_graphql(q2, {"q": f'tag:"mt_recordID:{mt_record_id}"'})
            if (((res2.get("data", {}) or {}).get("draftOrders", {}) or {}).get("edges", [])):
                print("  · Found existing draft by GraphQL tag query")
                return True
    except Exception:
        pass
    return False

def _rest_draft_exists(target_po_norm: str, mt_record_id: str | None) -> bool:
    """
    Fully paginate draft orders (open + invoice_sent) via REST using Link headers.
    Stop if we find a matching PO or mt_recordID tag, or if we hit page cap.
    """
    base = f"{SHOPIFY_BASE}/admin/api/{SHOPIFY_API_VERSION}/draft_orders.json"
    fields = "id,po_number,tags,created_at"
    for status in ("open", "invoice_sent"):
        params = {"status": status, "limit": REST_PAGE_LIMIT, "fields": fields}
        url = base
        pages = 0
        while True:
            pages += 1
            try:
                r = requests.get(url, headers=shopify_rest_headers, params=params, timeout=HTTP_TIMEOUT)
                r.raise_for_status()
            except Exception as e:
                print(f"(non-blocking) REST draft scan failed (status={status}, page={pages}): {e}")
                break

            for d in (r.json().get("draft_orders") or []):
                po_shop = norm_po(d.get("po_number"))
                if po_shop and po_shop == target_po_norm:
                    print(f"  · Found existing draft via REST (status={status}, page={pages})")
                    return True
                if mt_record_id and f"mt_recordID:{mt_record_id}" in (d.get("tags") or ""):
                    print(f"  · Found existing draft by REST tag (status={status}, page={pages})")
                    return True

            link = r.headers.get("Link") or ""
            if 'rel="next"' not in link:
                break

            try:
                next_part = [p for p in link.split(",") if 'rel="next"' in p][0]
                next_url = next_part[next_part.find("<")+1 : next_part.find(">")]
                pu = urlparse(next_url)
                url = f"{pu.scheme}://{pu.netloc}{pu.path}"
                q = parse_qs(pu.query)
                params = {"status": status, "limit": REST_PAGE_LIMIT, "fields": fields, "page_info": q.get("page_info", [""])[0]}
            except Exception:
                break

            if pages >= MAX_PAGES_PER_STATUS:
                print(f"(non-blocking) REST draft scan reached page cap ({MAX_PAGES_PER_STATUS}) for status={status}")
                break

            time.sleep(REST_PAGE_SLEEP)
    return False

def po_exists_in_shopify(po: str, mt_record_id: str | None = None) -> bool:
    """
    True if a Shopify Order or any Draft already carries this PO (normalized),
    or if we find a draft tagged with mt_recordID:<id>.
    """
    target = norm_po(po)

    # Orders via GraphQL (fast)
    try:
        q = """
        query($q: String!) {
          orders(first: 1, query: $q) {
            edges { node { id poNumber } }
          }
        }"""
        res = shopify_graphql(q, {"q": f"po_number:{po} -status:cancelled"})
        edges = (((res.get("data", {}) or {}).get("orders", {}) or {}).get("edges", []) or [])
        for e in edges:
            n = e.get("node", {}) or {}
            if norm_po(n.get("poNumber")) == target:
                print("  · Found existing Shopify ORDER by PO")
                return True
    except Exception:
        pass

    # Drafts via GraphQL (fast targeted query)
    if _draft_exists_graphql(po, mt_record_id):
        return True

    # Drafts via REST (full pagination fallback)
    return _rest_draft_exists(target, mt_record_id)

# -------------------- Draft creation --------------------
def to_mailing_address(order: dict, kind: str) -> dict:
    if kind == "billing":
        return {
            "firstName": order.get("buyerFirstName"),
            "lastName": order.get("buyerLastName"),
            "company": order.get("billToName"),
            "address1": order.get("billToAddress1"),
            "address2": order.get("billToAddress2"),
            "city": order.get("billToCity"),
            "province": order.get("billToState"),
            "zip": order.get("billToZip"),
            "country": order.get("billToCountry") or "US",
            "phone": order.get("billToPhone"),
        }
    else:
        return {
            "firstName": order.get("buyerFirstName"),
            "lastName": order.get("buyerLastName"),
            "company": order.get("shipToName"),
            "address1": order.get("shipToAddress1"),
            "address2": order.get("shipToAddress2"),
            "city": order.get("shipToCity"),
            "province": order.get("shipToState"),
            "zip": order.get("shipToZip"),
            "country": order.get("shipToCountry") or "US",
            "phone": order.get("shipToPhone"),
        }

def create_draft_order_graphql(order: dict, customer_id_numeric: int | str | None,
                               company_id: str | None, company_contact_id: str | None,
                               company_location_id: str | None) -> str:
    note_parts = []
    if order.get("poNumber"):
        note_parts.append(f"PO: {order['poNumber']}")
    if order.get("specialInstructions"):
        note_parts.append(order["specialInstructions"])
    if order.get("shippingMethod"):
        note_parts.append(f"Shipping: {order['shippingMethod']}")

    line_items = []
    for item in order.get("details", []) or []:
        sku = item.get("itemNumber")
        qty = int(item.get("quantity") or 0)
        parsed_price = parse_price(item.get("unitPrice"))

        variant_id = find_variant_by_sku(sku)
        if variant_id:
            li = {"variantId": variant_id, "quantity": qty}
            if parsed_price is not None:
                li["originalUnitPrice"] = parsed_price
        else:
            li = {"title": item.get("name") or sku, "sku": sku, "quantity": qty, "originalUnitPrice": parsed_price or 0}
        line_items.append(li)

    # --- Build tags including MT IDs for Flow write-back
    tags = [
        "markettime",
        f"mt_recordID:{order.get('recordID')}",
    ]
    rep_group_id = order.get("repGroupID")
    if rep_group_id:
        tags.append(f"mt_repGroupID:{rep_group_id}")
    retailer_id = order.get("retailerID")
    if retailer_id:
        tags.append(f"mt_retailerID:{retailer_id}")
    if order.get("manufacturerID"):
        tags.append(f"mt_manufacturerID:{order['manufacturerID']}")

    # --- B2B metafields from MarketTime -> Shopify draft order
    ship_date_val = _to_yyyy_mm_dd(order.get("shipDate"))
    bill_to_email_val = (order.get("billToEmail") or "").strip() or None
    po_num_val = (order.get("poNumber") or "").strip() or None

    metafields = []
    if ship_date_val:
        metafields.append({"namespace": "b2b", "key": "ship_date", "value": ship_date_val})
    if bill_to_email_val:
        metafields.append({"namespace": "b2b", "key": "bill_to_email", "value": bill_to_email_val})
    if po_num_val:
        metafields.append({"namespace": "b2b", "key": "po_number", "value": po_num_val})

    input_obj = {
        "lineItems": line_items,
        "note": " | ".join([p for p in note_parts if p]),
        "tags": tags,
        "billingAddress": to_mailing_address(order, "billing"),
        "shippingAddress": to_mailing_address(order, "shipping"),
        "poNumber": order.get("poNumber"),
        "email": order.get("shipToEmail") or order.get("billToEmail") or None,
    }

    if metafields:
        input_obj["metafields"] = metafields

    if company_id and company_contact_id and company_location_id:
        input_obj["purchasingEntity"] = {
            "purchasingCompany": {
                "companyId": company_id,
                "companyContactId": company_contact_id,
                "companyLocationId": company_location_id,
            }
        }
    elif customer_id_numeric:
        input_obj["purchasingEntity"] = {"customerId": to_gid("Customer", customer_id_numeric)}

    m = """
    mutation($input: DraftOrderInput!) {
      draftOrderCreate(input: $input) {
        draftOrder { id name poNumber purchasingEntity { __typename } }
        userErrors { field message }
      }
    }"""
    out = shopify_graphql(m, {"input": input_obj})
    payload = (out.get("data", {}) or {}).get("draftOrderCreate", {}) or {}
    errs = payload.get("userErrors", []) or []
    if errs:
        raise RuntimeError(f"draftOrderCreate userErrors: {errs}")
    draft = payload.get("draftOrder", {}) or {}
    draft_id = draft.get("id")
    if not draft_id:
        raise RuntimeError("draftOrderCreate returned no draft id")
    return draft_id

# -------------------- Customer search helpers --------------------
def _lc(s: str | None) -> str:
    return (s or "").strip().lower()

def find_customer_by_email(email: str) -> dict | None:
    url = f"{SHOPIFY_BASE}/admin/api/{SHOPIFY_API_VERSION}/customers/search.json"
    r = requests.get(url, headers=shopify_rest_headers, params={"query": f"email:{email}"}, timeout=HTTP_TIMEOUT)
    if r.ok:
        for c in r.json().get("customers", []):
            if _lc(c.get("email")) == _lc(email):
                return c
    return None

def find_customer_by_name_company(first: str | None, last: str | None, company: str | None) -> dict | None:
    terms = []
    if first:
        terms.append(f"first_name:{first}")
    if last:
        terms.append(f"last_name:{last}")
    if company:
        terms.append(f"company:'{company}'")
    if not terms:
        return None
    q = " ".join(terms)
    url = f"{SHOPIFY_BASE}/admin/api/{SHOPIFY_API_VERSION}/customers/search.json"
    r = requests.get(url, headers=shopify_rest_headers, params={"query": q}, timeout=HTTP_TIMEOUT)
    if r.ok:
        for c in r.json().get("customers", []):
            if _lc(c.get("first_name")) == _lc(first) and _lc(c.get("last_name")) == _lc(last) and _lc(c.get("company")) == _lc(company):
                return c
    return None

# -------------------- Fetch MarketTime orders + process --------------------
orders_all = fetch_all_mt_orders()
open_orders = [o for o in orders_all if str(o.get("manufacturerOrderStatus", "")).upper() == "OPEN"]

if PO_WHITELIST:
    open_orders = [o for o in open_orders if str(o.get("poNumber")) in PO_WHITELIST]
    print(f"PO whitelist active: {sorted(PO_WHITELIST)} -> {len(open_orders)} open orders in scope")

if not open_orders:
    print("No matching OPEN orders found.")
    raise SystemExit(0)

print(f"OPEN orders found: {len(open_orders)} | POs: {[o.get('poNumber') for o in open_orders]}")

# -------------------- Main loop --------------------
_seen_pos: set[str] = set()
exported_rows = []

def _default_ship_country_us(order: dict):
    val = (order.get("shipToCountry") or "").strip()
    if not val:
        order["shipToCountry"] = "US"
        print(f"  · Defaulted shipToCountry -> 'US' for PO {order.get('poNumber')}")

for order in open_orders:
    record_id = order.get("recordID")
    po_number = order.get("poNumber")
    po_norm = norm_po(po_number)
    billToName = order.get("billToName")
    shipToName = order.get("shipToName") or billToName or "Default"
    buyer_email = order.get("shipToEmail") or order.get("billToEmail") or None
    first_name = order.get("buyerFirstName")
    last_name = order.get("buyerLastName")

    _default_ship_country_us(order)

    if po_norm in _seen_pos:
        print(f"Skip record {record_id}: PO {po_number} already handled in this run.")
        continue

    customer = find_customer_by_email(buyer_email) if buyer_email else None
    if not customer:
        customer = find_customer_by_name_company(first_name, last_name, billToName)

    if customer:
        customer_id = customer["id"]
        print(f"↺ Reusing customer {customer.get('first_name','')} {customer.get('last_name','')} (ID: {customer_id})")
    else:
        def _clean_addr(d: dict) -> dict:
            return {k: v for k, v in d.items() if v not in (None, "", [])}

        addresses = []
        bill_addr = _clean_addr({
            "address1": order.get("billToAddress1"),
            "address2": order.get("billToAddress2"),
            "city": order.get("billToCity"),
            "province": order.get("billToState"),
            "zip": order.get("billToZip"),
            "country": order.get("billToCountry") or "US",
            "phone": order.get("billToPhone"),
            "company": billToName,
            "default": True,
        })
        if bill_addr.get("address1"):
            addresses.append(bill_addr)

        ship_addr = _clean_addr({
            "address1": order.get("shipToAddress1"),
            "address2": order.get("shipToAddress2"),
            "city": order.get("shipToCity"),
            "province": order.get("shipToState"),
            "zip": order.get("shipToZip"),
            "country": order.get("shipToCountry") or "US",
            "phone": order.get("shipToPhone"),
            "company": order.get("shipToName"),
        })
        if ship_addr.get("address1"):
            addresses.append(ship_addr)

        customer_body = {"first_name": first_name, "last_name": last_name, "tags": "markettime", "company": billToName}
        if buyer_email and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", buyer_email):
            customer_body["email"] = buyer_email
        if addresses:
            customer_body["addresses"] = addresses

        create_url = f"{SHOPIFY_BASE}/admin/api/{SHOPIFY_API_VERSION}/customers.json"
        cr = requests.post(create_url, headers=shopify_rest_headers, json={"customer": customer_body}, timeout=HTTP_TIMEOUT)

        if cr.status_code == 422:
            try:
                err_json = cr.json()
            except Exception:
                err_json = {"raw": cr.text}
            print(f"✖ Customer create 422 for PO {po_number}: {err_json}")
            reason = json.dumps(err_json).lower()
            retry_body = dict(customer_body)
            changed = False
            if "email" in reason and "email" in retry_body:
                retry_body.pop("email", None); changed = True
            if "phone" in reason and "phone" in retry_body:
                retry_body.pop("phone", None); changed = True
            if changed:
                cr = requests.post(create_url, headers=shopify_rest_headers, json={"customer": retry_body}, timeout=HTTP_TIMEOUT)

        if cr.status_code == 422:
            try:
                err_json = cr.json()
            except Exception:
                err_json = {"raw": cr.text}
            print(f"Skip record {record_id} (PO {po_number}): cannot create customer — {err_json}")
            continue

        cr.raise_for_status()
        customer_id = cr.json()["customer"]["id"]
        print(f"✅ Created customer: {billToName or ''} ({first_name or ''} {last_name or ''}) ID: {customer_id}")

    company_id = ensure_company(billToName) if billToName else None
    company_location_id = None
    company_contact_id = None
    if company_id:
        print("→ Ensuring company location…")
        company_location_id = ensure_company_location(company_id, shipToName, order)

        if customer_id and company_location_id:
            print("→ Resolving matching company contact…")
            company_contact_id = get_or_create_matching_contact(company_id, customer_id)

            if company_contact_id:
                print("→ Granting ordering permission (Ordering only)…")
                grant_ordering_permission(company_contact_id, company_location_id, company_id)

    ts_po = time.time() if LOG_TIMINGS else None
    print(f"→ Checking if PO exists in Shopify: {po_number}")
    if po_number and po_exists_in_shopify(po_number, str(record_id) if record_id else None):
        _t("PO dedupe total", ts_po)
        print(f"Skip record {record_id}: PO {po_number} already exists in Shopify.")
        _seen_pos.add(po_norm)
        continue

    print("→ Creating Shopify draft order (GraphQL)…")
    ts_create = time.time() if LOG_TIMINGS else None
    try:
        draft_id = create_draft_order_graphql(order, customer_id, company_id, company_contact_id, company_location_id)
    except Exception as e:
        print(f"✖ draftOrderCreate failed: {e}")
        continue
    _t("draftOrderCreate", ts_create)
    print(f"✅ Draft order created for PO {po_number} → ID: {draft_id}")

    exported_rows.append({
        "mt_recordID": record_id,
        "shopify_draft_order_id": draft_id,
        "poNumber": po_number,
        "company": billToName,
        "buyer_email": buyer_email or "unknown@example.com",
        "line_item_count": len(order.get("details", []) or []),
        "created_at": datetime.now(UTC).isoformat(),
    })

    _seen_pos.add(po_norm)
    time.sleep(0.15)

# -------------------- CSV --------------------
csv_path = export_rows_to_csv(exported_rows)
print(f"Processed OPEN orders: {len(open_orders)} | Created draft orders: {len(exported_rows)}")
print(f"CSV exported: {csv_path}" if csv_path else "No new orders were exported; CSV not created.")

