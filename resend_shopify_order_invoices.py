#!/usr/bin/env python3
"""
Resend Shopify order invoice emails for an explicit list of live Shopify order names.

Use case:
- You corrected live Order.email values after invoices were originally sent to the wrong address.
- You now want Shopify to send a fresh invoice email to the corrected Order.email.

Input:
- A text file with one Shopify order name per line, for example:
    #19447
    #19451
    #19814

Defaults:
- DRY_RUN=true
- Skips paid/voided/refunded orders unless --include-paid is used
- Sends to the CURRENT Shopify Order.email
- Does not update customer records

Environment:
  SHOPIFY_TOKEN=shpat_...
  SHOPIFY_STORE=your-store.myshopify.com
  SHOPIFY_API_VERSION=2024-10
  DRY_RUN=true

Example:
  python resend_shopify_order_invoices.py --orders-file order_invoice_resend_list.txt
  python resend_shopify_order_invoices.py --orders-file order_invoice_resend_list.txt --live
  python resend_shopify_order_invoices.py --order-name "#24720" --live
"""

import os
import csv
import re
import json
import time
import argparse
import requests
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN")
SHOPIFY_STORE = os.getenv("SHOPIFY_STORE")
SHOPIFY_API_VERSION = os.getenv("SHOPIFY_API_VERSION", "2024-10")
DEFAULT_DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

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

DEFAULT_MESSAGE = (
    "Hello — we are resending this invoice because the original invoice may have been sent "
    "to an outdated contact email. Please use this updated invoice link for payment. "
    "Thank you!"
)


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


def clean_order_name(value: str) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if not s.startswith("#"):
        s = f"#{s}"
    return s


def is_valid_email(value: str | None) -> bool:
    return bool(value and EMAIL_RE.match(value.strip()))


def load_order_names(path: str | None, single_order_name: str | None) -> list[str]:
    names = []

    if path:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw or raw.startswith("//"):
                    continue
                # Accept lines like "#19447" or "19447"
                # Ignore accidental commas/spaces after the order number.
                raw = raw.split()[0].strip().rstrip(",")
                name = clean_order_name(raw)
                if name:
                    names.append(name)

    if single_order_name:
        names.append(clean_order_name(single_order_name))

    # preserve order, de-dupe
    seen = set()
    out = []
    for n in names:
        if n and n not in seen:
            seen.add(n)
            out.append(n)

    return out


def get_order_by_name(order_name: str) -> dict | None:
    query = """
    query($query: String!) {
      orders(first: 2, query: $query) {
        nodes {
          id
          name
          email
          displayFinancialStatus
          displayFulfillmentStatus
          createdAt
          customer {
            id
            email
          }
          currentTotalPriceSet {
            shopMoney {
              amount
              currencyCode
            }
          }
          totalOutstandingSet {
            shopMoney {
              amount
              currencyCode
            }
          }
        }
      }
    }
    """

    # name:#12345 is the most precise Shopify Admin search for order name.
    data = gql(query, {"query": f"name:{order_name}"})
    nodes = data["data"]["orders"]["nodes"]

    exact = [o for o in nodes if o.get("name") == order_name]
    if exact:
        return exact[0]
    if len(nodes) == 1:
        return nodes[0]
    return None


def send_order_invoice(order_id: str, to_email: str, subject: str | None, custom_message: str | None):
    mutation = """
    mutation OrderInvoiceSend($orderId: ID!, $email: EmailInput) {
      orderInvoiceSend(id: $orderId, email: $email) {
        order {
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

    email_input = {"to": to_email}
    if subject:
        email_input["subject"] = subject
    if custom_message:
        email_input["customMessage"] = custom_message

    data = gql(mutation, {"orderId": order_id, "email": email_input})
    payload = data["data"]["orderInvoiceSend"]
    if payload.get("userErrors"):
        raise RuntimeError(json.dumps(payload["userErrors"], indent=2))
    return payload["order"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders-file", default="order_invoice_resend_list.txt")
    parser.add_argument("--order-name", help="Limit to one Shopify order name, for example #24720.")
    parser.add_argument("--dry-run", action="store_true", default=DEFAULT_DRY_RUN)
    parser.add_argument("--live", action="store_true", help="Actually send emails. Overrides --dry-run/env.")
    parser.add_argument("--include-paid", action="store_true", help="Also send invoices to orders marked paid/refunded/voided.")
    parser.add_argument("--subject-prefix", default="Invoice", help='Default subject is "{prefix} {order_name}".')
    parser.add_argument("--message", default=DEFAULT_MESSAGE)
    parser.add_argument("--sleep", type=float, default=0.5, help="Seconds to sleep between sends.")
    args = parser.parse_args()

    dry_run = False if args.live else args.dry_run
    order_names = load_order_names(args.orders_file, args.order_name)

    if not order_names:
        raise ValueError("No order names found. Provide --order-name or an orders file.")

    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    out_path = f"resend_order_invoices_audit_{timestamp}.csv"

    print(f"SHOPIFY_STORE={SHOPIFY_STORE}")
    print(f"SHOPIFY_API_VERSION={SHOPIFY_API_VERSION}")
    print(f"DRY_RUN={dry_run}")
    print(f"ORDERS={len(order_names)}")
    print(f"INCLUDE_PAID={args.include_paid}")
    print("")

    rows = []

    skip_financial_statuses = {"PAID", "REFUNDED", "VOIDED"}

    for order_name in order_names:
        action = "skip"
        error = ""
        order = None

        try:
            order = get_order_by_name(order_name)
            if not order:
                action = "skip_order_not_found"
                print(f"{order_name} -> {action}")
            else:
                email = (order.get("email") or "").strip()
                financial_status = order.get("displayFinancialStatus")
                fulfillment_status = order.get("displayFulfillmentStatus")
                outstanding = (
                    ((order.get("totalOutstandingSet") or {}).get("shopMoney") or {}).get("amount")
                )

                if not is_valid_email(email):
                    action = "skip_invalid_order_email"
                elif (financial_status in skip_financial_statuses) and not args.include_paid:
                    action = "skip_financial_status"
                else:
                    action = "would_send_invoice" if dry_run else "sent_invoice"
                    if not dry_run:
                        subject = f"{args.subject_prefix} {order.get('name') or order_name}"
                        send_order_invoice(order["id"], email, subject, args.message)
                        time.sleep(args.sleep)

                print(
                    f"{order_name} email={email!r} financial={financial_status} "
                    f"fulfillment={fulfillment_status} outstanding={outstanding} -> {action}"
                )

        except Exception as e:
            error = str(e)
            action = "error"
            print(f"{order_name} -> ERROR: {error}")

        rows.append({
            "action": action,
            "order_name": order_name,
            "order_id": order.get("id") if order else "",
            "email": order.get("email") if order else "",
            "customer_id": ((order.get("customer") or {}).get("id") if order else ""),
            "customer_email": ((order.get("customer") or {}).get("email") if order else ""),
            "financial_status": order.get("displayFinancialStatus") if order else "",
            "fulfillment_status": order.get("displayFulfillmentStatus") if order else "",
            "total_price": (((order.get("currentTotalPriceSet") or {}).get("shopMoney") or {}).get("amount") if order else ""),
            "total_outstanding": (((order.get("totalOutstandingSet") or {}).get("shopMoney") or {}).get("amount") if order else ""),
            "error": error,
        })

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "action",
            "order_name",
            "order_id",
            "email",
            "customer_id",
            "customer_email",
            "financial_status",
            "fulfillment_status",
            "total_price",
            "total_outstanding",
            "error",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print("")
    print(f"Audit CSV written: {out_path}")


if __name__ == "__main__":
    main()
