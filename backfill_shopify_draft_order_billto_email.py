#!/usr/bin/env python3
"""
Backfill Shopify MarketTime draft orders so invoice email uses bill_to_email.

Date-filtered version:
- Defaults to draft orders created in the last 60 days
- Use --days-back N, --created-after YYYY-MM-DD, and/or --created-before YYYY-MM-DD
- Filters by DraftOrder.createdAt client-side after fetching MarketTime drafts

What it does:
- Finds Shopify draft orders tagged "markettime"
- Reads draft metafield b2b.bill_to_email
- If that email is valid and differs from the draft order email, updates DraftOrderInput.email
- Writes an audit CSV

Optional:
- Set UPDATE_CUSTOMER_EMAIL=true to also update the attached Shopify Customer email when it
  exactly matches the old draft email. Default is false because customer records can be shared
  across multiple orders/locations.

Environment:
  SHOPIFY_TOKEN=shpat_...
  SHOPIFY_STORE=your-store.myshopify.com
  SHOPIFY_API_VERSION=2024-10  # optional
  DRY_RUN=true                 # default true
  DAYS_BACK=60                 # optional
  CREATED_AFTER=YYYY-MM-DD     # optional
  CREATED_BEFORE=YYYY-MM-DD    # optional
  UPDATE_CUSTOMER_EMAIL=false  # default false
"""

import os
import csv
import re
import time
import json
import argparse
import requests
from datetime import datetime, date, time as dt_time, UTC, timedelta
from dotenv import load_dotenv

load_dotenv()

SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")

DEFAULT_DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
DEFAULT_UPDATE_CUSTOMER_EMAIL = os.getenv("UPDATE_CUSTOMER_EMAIL", "false").lower() == "true"
DEFAULT_DAYS_BACK = int(os.getenv("DAYS_BACK", "60"))
DEFAULT_CREATED_AFTER = os.getenv("CREATED_AFTER") or ""
DEFAULT_CREATED_BEFORE = os.getenv("CREATED_BEFORE") or ""

if not SHOPIFY_TOKEN or not SHOPIFY_STORE:
    raise ValueError("Missing SHOPIFY_TOKEN or SHOPIFY_STORE")

SHOPIFY_BASE = (
    f"https://{SHOPIFY_STORE.strip('/')}"
    if not SHOPIFY_STORE.startswith("http")
    else SHOPIFY_STORE.rstrip("/")
)

GRAPHQL_URL = f"{SHOPIFY_BASE}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_TOKEN,
    "Content-Type": "application/json",
}

EMAIL_RE = re.compile(
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?"
    r"(?:\.[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)+$"
)
CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]+")


def parse_date_to_utc_start(value: str | None) -> datetime | None:
    if not value:
        return None
    d = date.fromisoformat(value.strip())
    return datetime.combine(d, dt_time.min, tzinfo=UTC)


def parse_date_to_utc_exclusive_end(value: str | None) -> datetime | None:
    if not value:
        return None
    d = date.fromisoformat(value.strip())
    return datetime.combine(d + timedelta(days=1), dt_time.min, tzinfo=UTC)


def parse_shopify_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def gql(query: str, variables: dict | None = None) -> dict:
    r = requests.post(
        GRAPHQL_URL,
        headers=HEADERS,
        json={"query": query, "variables": variables or {}},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("errors"):
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data


def clean_email(value) -> str | None:
    if value is None:
        return None

    s = str(value).strip().strip('"').strip("'")
    if not s:
        return None

    s = s.replace("mailto:", "")
    m = re.search(r"<([^<>]+@[^<>]+)>", s)
    if m:
        s = m.group(1)

    s = CONTROL_CHARS_RE.sub(" ", s)
    s = s.replace(",", " ").replace(";", " ")

    tokens = [t.strip().strip('"').strip("'") for t in s.split() if "@" in t]
    if not tokens and "@" in s:
        tokens = [s.strip()]
    if not tokens:
        return None

    e = tokens[0].rstrip(".,;:")
    if "@" not in e:
        return None

    local, domain = e.rsplit("@", 1)
    local = local.strip()
    domain = domain.strip().lower()

    if not local or not domain:
        return None

    return f"{local}@{domain}"


def is_valid_email(value) -> bool:
    e = clean_email(value)
    return bool(e and EMAIL_RE.match(e))


def fetch_markettime_drafts():
    query = """
    query($after: String) {
      draftOrders(first: 100, after: $after, query: "tag:markettime") {
        pageInfo {
          hasNextPage
          endCursor
        }
        nodes {
          id
          name
          poNumber
          email
          status
          createdAt
          invoiceSentAt
          completedAt
          tags
          customer {
            id
            email
            firstName
            lastName
          }
          metafield(namespace: "b2b", key: "bill_to_email") {
            value
          }
        }
      }
    }
    """

    after = None

    while True:
        data = gql(query, {"after": after})
        payload = data["data"]["draftOrders"]

        for node in payload["nodes"]:
            yield node

        if payload["pageInfo"]["hasNextPage"]:
            after = payload["pageInfo"]["endCursor"]
            time.sleep(0.2)
        else:
            break


def update_draft_email(draft_id: str, new_email: str):
    mutation = """
    mutation($id: ID!, $input: DraftOrderInput!) {
      draftOrderUpdate(id: $id, input: $input) {
        draftOrder {
          id
          name
          email
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    data = gql(mutation, {
        "id": draft_id,
        "input": {
            "email": new_email,
        },
    })

    payload = data["data"]["draftOrderUpdate"]
    if payload.get("userErrors"):
        raise RuntimeError(json.dumps(payload["userErrors"], indent=2))

    return payload["draftOrder"]


def update_customer_email(customer_id: str, new_email: str):
    mutation = """
    mutation($input: CustomerInput!) {
      customerUpdate(input: $input) {
        customer {
          id
          email
        }
        userErrors {
          field
          message
        }
      }
    }
    """

    data = gql(mutation, {
        "input": {
            "id": customer_id,
            "email": new_email,
        },
    })

    payload = data["data"]["customerUpdate"]
    if payload.get("userErrors"):
        raise RuntimeError(json.dumps(payload["userErrors"], indent=2))

    return payload["customer"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", default=DEFAULT_DRY_RUN)
    parser.add_argument("--live", action="store_true", help="Actually update Shopify. Overrides --dry-run/env.")
    parser.add_argument("--update-customer-email", action="store_true", default=DEFAULT_UPDATE_CUSTOMER_EMAIL)
    parser.add_argument("--include-completed", action="store_true", help="Also attempt completed draft orders. Usually leave off.")
    parser.add_argument("--po", help="Limit to one PO number for testing.")
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK, help="Default 60. Ignored if --created-after is provided.")
    parser.add_argument("--created-after", default=DEFAULT_CREATED_AFTER, help="Inclusive UTC date filter, YYYY-MM-DD.")
    parser.add_argument("--created-before", default=DEFAULT_CREATED_BEFORE, help="Inclusive UTC date filter, YYYY-MM-DD.")
    args = parser.parse_args()

    dry_run = False if args.live else args.dry_run
    update_customers = args.update_customer_email

    if args.created_after:
        created_after_dt = parse_date_to_utc_start(args.created_after)
        created_after_label = args.created_after
    else:
        created_after_dt = datetime.now(UTC) - timedelta(days=args.days_back)
        created_after_label = f"last_{args.days_back}_days"

    created_before_dt = parse_date_to_utc_exclusive_end(args.created_before) if args.created_before else None

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = f"draft_order_billto_email_backfill_{timestamp}.csv"

    rows = []

    print(f"SHOPIFY_STORE={SHOPIFY_STORE}")
    print(f"SHOPIFY_API_VERSION={SHOPIFY_API_VERSION}")
    print(f"DRY_RUN={dry_run}")
    print(f"UPDATE_CUSTOMER_EMAIL={update_customers}")
    print(f"INCLUDE_COMPLETED={args.include_completed}")
    print(f"CREATED_AFTER={created_after_dt.isoformat()} ({created_after_label})")
    print(f"CREATED_BEFORE={created_before_dt.isoformat() if created_before_dt else 'none'}")
    print("")

    scanned = 0
    skipped_outside_window = 0

    for draft in fetch_markettime_drafts():
        scanned += 1
        draft_id = draft["id"]
        name = draft.get("name")
        po = draft.get("poNumber")
        created_at = draft.get("createdAt")
        created_at_dt = parse_shopify_dt(created_at)

        if args.po and str(po or "").strip() != args.po.strip():
            continue

        if created_at_dt and created_at_dt < created_after_dt:
            skipped_outside_window += 1
            continue

        if created_before_dt and created_at_dt and created_at_dt >= created_before_dt:
            skipped_outside_window += 1
            continue

        current_draft_email = clean_email(draft.get("email"))
        bill_to_email = clean_email((draft.get("metafield") or {}).get("value"))
        status = draft.get("status")
        invoice_sent_at = draft.get("invoiceSentAt")
        completed_at = draft.get("completedAt")
        customer = draft.get("customer") or {}
        customer_id = customer.get("id")
        customer_email = clean_email(customer.get("email"))

        draft_action = "skip"
        customer_action = "skip"
        error = ""

        try:
            if completed_at and not args.include_completed:
                draft_action = "skip_completed"
            elif not bill_to_email:
                draft_action = "skip_no_bill_to_email_metafield"
            elif not is_valid_email(bill_to_email):
                draft_action = "skip_invalid_bill_to_email"
            elif current_draft_email and current_draft_email.lower() == bill_to_email.lower():
                draft_action = "skip_draft_already_correct"
            else:
                draft_action = "would_update_draft" if dry_run else "updated_draft"
                if not dry_run:
                    update_draft_email(draft_id, bill_to_email)

            # Optional customer cleanup. Only update if the customer email is exactly the same
            # as the old wrong draft email. This avoids changing a customer whose email has already
            # been manually corrected or is intentionally different.
            if not update_customers:
                customer_action = "skip_customer_update_disabled"
            elif not customer_id:
                customer_action = "skip_no_customer"
            elif not bill_to_email or not is_valid_email(bill_to_email):
                customer_action = "skip_no_valid_bill_to_email"
            elif customer_email and customer_email.lower() == bill_to_email.lower():
                customer_action = "skip_customer_already_correct"
            elif customer_email and current_draft_email and customer_email.lower() == current_draft_email.lower():
                customer_action = "would_update_customer" if dry_run else "updated_customer"
                if not dry_run:
                    update_customer_email(customer_id, bill_to_email)
            else:
                customer_action = "skip_customer_email_does_not_match_old_draft_email"

        except Exception as e:
            error = str(e)
            if draft_action.startswith("would") or draft_action.startswith("updated"):
                draft_action = "error"
            else:
                customer_action = "error"

        print(
            f"{name} PO={po} createdAt={created_at} status={status} "
            f"invoice_sent_at={invoice_sent_at} "
            f"draft={current_draft_email!r} bill_to={bill_to_email!r} "
            f"customer={customer_email!r} -> {draft_action}; {customer_action}"
        )
        if error:
            print(f"  ERROR: {error}")

        rows.append({
            "draft_action": draft_action,
            "customer_action": customer_action,
            "draft_name": name,
            "po_number": po,
            "draft_id": draft_id,
            "created_at": created_at,
            "status": status,
            "invoice_sent_at": invoice_sent_at,
            "completed_at": completed_at,
            "old_draft_email": current_draft_email,
            "bill_to_email": bill_to_email,
            "customer_id": customer_id,
            "old_customer_email": customer_email,
            "error": error,
        })

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "draft_action",
            "customer_action",
            "draft_name",
            "po_number",
            "draft_id",
            "created_at",
            "status",
            "invoice_sent_at",
            "completed_at",
            "old_draft_email",
            "bill_to_email",
            "customer_id",
            "old_customer_email",
            "error",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("")
    print(f"Scanned MarketTime drafts: {scanned}")
    print(f"Skipped outside date window: {skipped_outside_window}")
    print(f"Rows in audit CSV: {len(rows)}")
    print(f"Audit CSV written: {out_path}")


if __name__ == "__main__":
    main()
