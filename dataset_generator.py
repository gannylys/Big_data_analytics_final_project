#!/usr/bin/env python3
"""
8GB-friendly synthetic dataset generator for AUCA Jan 2026 project.

Generates:
- users.json
- categories.json
- products.json
- transactions.json
- sessions_0.json, sessions_1.json, ...

Matches the sample schema shown in the project PDF:
Users: user_id, geo_data, registration_date, last_active
Categories: category_id, name, subcategories[{subcategory_id,name,profit_margin}]
Products: product_id, name, category_id, subcategory_id, base_price, current_stock, is_active, price_history, creation_date
Sessions: session_id, user_id, start_time, end_time, duration_seconds, geo_data(+ip_address),
          device_profile, viewed_products, page_views[{timestamp,page_type,product_id,category_id,view_duration}],
          cart_contents{prod:{quantity,price}}, conversion_status, referrer
Transactions: transaction_id, session_id (nullable), user_id, timestamp, items[{product_id,quantity,unit_price,subtotal}],
              subtotal, discount, total, payment_method, status

Key 8GB design choices:
- Sessions and transactions are STREAM-WRITTEN (no giant lists in RAM).
- Sessions are written in CHUNKS into sessions_N.json (JSON arrays).
- Transactions are streamed into one JSON array file (transactions.json) safely.
- Products/users/categories are kept in memory (small enough at 8GB-safe defaults).
"""

import argparse
import json
import os
import random
import string
import datetime as dt
from typing import Dict, List, Optional, Tuple

from faker import Faker


# ----------------------------
# Helpers
# ----------------------------
def iso(ts: dt.datetime) -> str:
    # Ensure naive ISO format like the PDF samples
    return ts.replace(microsecond=0).isoformat()


def rand_hex(n: int) -> str:
    return "".join(random.choice("0123456789abcdef") for _ in range(n))


def write_json_array(path: str, items_iter, progress_every: int = 0, label: str = "") -> int:
    """
    Stream-write a JSON array to disk without holding all items in memory.
    Returns count written.
    """
    count = 0
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write("[\n")
        first = True
        for item in items_iter:
            if not first:
                f.write(",\n")
            first = False
            f.write(json.dumps(item, ensure_ascii=False))
            count += 1
            if progress_every and count % progress_every == 0:
                print(f"[{label}] wrote {count:,} records -> {path}")
        f.write("\n]\n")
    return count


def weighted_choice(choices: List[Tuple[str, float]]) -> str:
    r = random.random()
    total = 0.0
    for v, w in choices:
        total += w
        if r <= total:
            return v
    return choices[-1][0]


# ----------------------------
# Inventory manager (light)
# ----------------------------
class Inventory:
    def __init__(self, products: List[dict]):
        # Keep only what we need
        self.products: Dict[str, dict] = {p["product_id"]: p for p in products}

    def in_stock(self, pid: str, qty: int) -> bool:
        p = self.products.get(pid)
        return bool(p) and p["current_stock"] >= qty and p["is_active"] is True

    def reserve(self, pid: str, qty: int) -> bool:
        if not self.in_stock(pid, qty):
            return False
        p = self.products[pid]
        p["current_stock"] -= qty
        if p["current_stock"] <= 0:
            p["current_stock"] = 0
            p["is_active"] = False
        return True


# ----------------------------
# Generators for entities
# ----------------------------
def gen_categories(fake: Faker, num_categories: int, subcats_per_cat: Tuple[int, int]) -> List[dict]:
    categories = []
    for cat_id in range(num_categories):
        cat = {
            "category_id": f"cat_{cat_id:03d}",
            "name": fake.company(),
            "subcategories": []
        }
        n_sub = random.randint(subcats_per_cat[0], subcats_per_cat[1])
        for sub_id in range(n_sub):
            cat["subcategories"].append({
                "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
                "name": fake.bs().title(),
                "profit_margin": round(random.uniform(0.10, 0.40), 2)
            })
        categories.append(cat)
    return categories


def gen_products(fake: Faker, categories: List[dict], num_products: int, timespan_days: int) -> List[dict]:
    products = []
    creation_start = dt.datetime.now() - dt.timedelta(days=timespan_days * 2)

    for prod_id in range(num_products):
        cat = random.choice(categories)
        sub = random.choice(cat["subcategories"])

        base_price0 = round(random.uniform(5, 500), 2)
        price_history = []

        # 1st price
        initial_date = fake.date_time_between(
            start_date=creation_start,
            end_date=creation_start + dt.timedelta(days=max(1, timespan_days // 3))
        )
        price_history.append({"price": base_price0, "date": iso(initial_date)})

        # 0-2 additional changes
        n_changes = random.randint(0, 2)
        last_price = base_price0
        last_date = initial_date
        for _ in range(n_changes):
            change_date = fake.date_time_between(start_date=last_date, end_date="now")
            new_price = round(last_price * random.uniform(0.8, 1.2), 2)
            price_history.append({"price": new_price, "date": iso(change_date)})
            last_price = new_price
            last_date = change_date

        price_history.sort(key=lambda x: x["date"])
        current_price = price_history[-1]["price"]

        # Keep some out-of-stock + inactive products to match PDF sample
        stock = random.randint(10, 1000)
        is_active = random.choices([True, False], weights=[0.95, 0.05])[0]
        if not is_active and random.random() < 0.7:
            stock = 0

        products.append({
            "product_id": f"prod_{prod_id:05d}",
            "name": fake.catch_phrase().title(),
            "category_id": cat["category_id"],
            "subcategory_id": sub["subcategory_id"],
            "base_price": current_price,
            "current_stock": stock,
            "is_active": bool(is_active and stock > 0),
            "price_history": price_history,
            "creation_date": price_history[0]["date"]
        })

    return products


def gen_users(fake: Faker, num_users: int, timespan_days: int) -> List[dict]:
    users = []
    for user_id in range(num_users):
        reg_date = fake.date_time_between(
            start_date=f"-{timespan_days * 3}d",
            end_date=f"-{timespan_days}d"
        )
        users.append({
            "user_id": f"user_{user_id:06d}",
            "geo_data": {
                "city": fake.city(),
                "state": fake.state_abbr(),
                "country": fake.country_code()
            },
            "registration_date": iso(reg_date),
            "last_active": iso(fake.date_time_between(start_date=reg_date, end_date="now"))
        })
    return users


# ----------------------------
# Session/Transaction synthesis
# ----------------------------
PAGE_TYPES = ["home", "search", "category_listing", "product_detail", "cart", "checkout"]

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
OS_TYPES = ["iOS", "Android", "Windows", "macOS", "Linux"]
BROWSERS = ["Chrome", "Safari", "Firefox", "Edge"]

REFERRERS = [
    ("direct", 0.35),
    ("search_engine", 0.35),
    ("social_media", 0.20),
    ("email_campaign", 0.10),
]

PAYMENT_METHODS = ["credit_card", "paypal", "bank_transfer", "gift_card"]
TX_STATUSES = ["completed", "processing", "shipped", "delivered"]


def make_page_flow(session_duration: int) -> List[int]:
    # 4–14 page views, with time slots sorted
    n_views = random.randint(4, 14)
    points = sorted({0, session_duration} | {random.randint(1, session_duration - 1) for _ in range(n_views - 1)})
    return points


def choose_page_type(i: int, prev: List[dict]) -> str:
    if i == 0:
        return "home"
    if prev and prev[-1]["page_type"] == "cart":
        return weighted_choice([("checkout", 0.35), ("product_detail", 0.40), ("search", 0.25)])
    if prev and prev[-1]["page_type"] == "checkout":
        return weighted_choice([("checkout", 0.20), ("cart", 0.30), ("product_detail", 0.50)])
    return weighted_choice([
        ("search", 0.20),
        ("category_listing", 0.25),
        ("product_detail", 0.40),
        ("cart", 0.15),
    ])


def pick_product_and_category(products: List[dict], categories: List[dict]) -> Tuple[Optional[dict], Optional[dict]]:
    p = random.choice(products)
    # Find category quickly by id (build once in caller if you want)
    cat = None
    return p, cat


def session_record(fake: Faker,
                   user: dict,
                   products: List[dict],
                   categories: List[dict],
                   inv: Inventory,
                   timespan_days: int) -> Tuple[dict, Optional[dict]]:
    """
    Build one session record.
    Returns (session_doc, maybe_transaction_doc).
    """
    session_id = f"sess_{rand_hex(10)}"
    start = fake.date_time_between(start_date=f"-{timespan_days}d", end_date="now")
    duration = random.randint(30, 3600)
    end = start + dt.timedelta(seconds=duration)

    device = {
        "type": random.choice(DEVICE_TYPES),
        "os": random.choice(OS_TYPES),
        "browser": random.choice(BROWSERS)
    }

    page_views = []
    viewed_products = set()
    cart_contents: Dict[str, dict] = {}

    # Map category_id for product_detail/category_listing pages
    cat_lookup = {c["category_id"]: c for c in categories}

    slots = make_page_flow(duration)
    for i in range(len(slots) - 1):
        view_duration = slots[i + 1] - slots[i]
        ptype = choose_page_type(i, page_views)

        product_id = None
        category_id = None

        if ptype == "category_listing":
            cat = random.choice(categories)
            category_id = cat["category_id"]

        elif ptype == "product_detail":
            # Prefer active, in-stock products for better conversion realism
            # Try a few picks
            chosen = None
            for _ in range(5):
                cand = random.choice(products)
                if cand["is_active"] and cand["current_stock"] > 0:
                    chosen = cand
                    break
            if chosen is None:
                chosen = random.choice(products)

            product_id = chosen["product_id"]
            category_id = chosen["category_id"]
            viewed_products.add(product_id)

            # Add to cart with 30% probability
            if random.random() < 0.30:
                if product_id not in cart_contents:
                    cart_contents[product_id] = {"quantity": 0, "price": chosen["base_price"]}

                # Add 1–3 units respecting current stock
                remaining = inv.products[product_id]["current_stock"] - cart_contents[product_id]["quantity"]
                if remaining > 0:
                    add_qty = random.randint(1, min(3, remaining))
                    cart_contents[product_id]["quantity"] += add_qty

        # Timestamp for this page view
        pv_time = start + dt.timedelta(seconds=slots[i])
        page_views.append({
            "timestamp": iso(pv_time),
            "page_type": ptype,
            "product_id": product_id,
            "category_id": category_id,
            "view_duration": int(view_duration)
        })

    # Derive conversion_status
    did_checkout = any(pv["page_type"] == "checkout" for pv in page_views)
    has_cart = any(v["quantity"] > 0 for v in cart_contents.values())

    # Conversion probability (tunable)
    convert_prob = 0.05
    if has_cart:
        convert_prob = 0.20
    if did_checkout and has_cart:
        convert_prob = 0.45

    converted = (random.random() < convert_prob) and has_cart

    session_doc = {
        "session_id": session_id,
        "user_id": user["user_id"],
        "start_time": iso(start),
        "end_time": iso(end),
        "duration_seconds": int(duration),
        "geo_data": {
            "city": user["geo_data"]["city"],
            "state": user["geo_data"]["state"],
            "country": user["geo_data"]["country"],
            "ip_address": fake.ipv4_public()
        },
        "device_profile": device,
        "viewed_products": sorted(viewed_products),
        "page_views": page_views,
        "cart_contents": {k: v for k, v in cart_contents.items() if v["quantity"] > 0},
        "conversion_status": "converted" if converted else ("abandoned_cart" if has_cart else "browsing"),
        "referrer": weighted_choice(REFERRERS)
    }

    tx_doc = None
    if converted:
        # Create transaction from cart (reserve stock)
        items = []
        subtotal = 0.0
        for pid, entry in session_doc["cart_contents"].items():
            qty = int(entry["quantity"])
            if qty <= 0:
                continue
            # Ensure we can reserve stock
            if inv.reserve(pid, qty):
                unit_price = float(entry["price"])
                line_sub = round(qty * unit_price, 2)
                items.append({
                    "product_id": pid,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "subtotal": line_sub
                })
                subtotal += line_sub

        if items:
            subtotal = round(subtotal, 2)
            discount = 0.0
            if random.random() < 0.20:
                discount_rate = random.choice([0.05, 0.10, 0.15, 0.20])
                discount = round(subtotal * discount_rate, 2)

            total = round(subtotal - discount, 2)

            tx_doc = {
                "transaction_id": f"txn_{rand_hex(12)}",
                "session_id": session_id,
                "user_id": user["user_id"],
                "timestamp": iso(end),
                "items": items,
                "subtotal": subtotal,
                "discount": discount,
                "total": total,
                "payment_method": random.choice(PAYMENT_METHODS),
                "status": random.choice(TX_STATUSES)
            }
        else:
            # If no items reserved (edge), mark as abandoned
            session_doc["conversion_status"] = "abandoned_cart"

    return session_doc, tx_doc


def gen_orphan_transaction(fake: Faker, user: dict, products: List[dict], inv: Inventory, timespan_days: int) -> Optional[dict]:
    """
    Transaction not linked to a session (session_id = null), per PDF sample.
    """
    # Try to create 1–3 items
    n_items = random.randint(1, 3)
    items = []
    subtotal = 0.0

    # Try a few picks to avoid out-of-stock
    for _ in range(n_items * 3):
        if len(items) >= n_items:
            break
        p = random.choice(products)
        if not p["is_active"] or p["current_stock"] <= 0:
            continue
        pid = p["product_id"]
        qty = random.randint(1, 3)
        if inv.reserve(pid, qty):
            unit_price = float(p["base_price"])
            line_sub = round(qty * unit_price, 2)
            items.append({"product_id": pid, "quantity": qty, "unit_price": unit_price, "subtotal": line_sub})
            subtotal += line_sub

    if not items:
        return None

    subtotal = round(subtotal, 2)
    discount = 0.0
    if random.random() < 0.20:
        discount_rate = random.choice([0.05, 0.10, 0.15, 0.20])
        discount = round(subtotal * discount_rate, 2)

    total = round(subtotal - discount, 2)

    return {
        "transaction_id": f"txn_{rand_hex(12)}",
        "session_id": None,
        "user_id": user["user_id"],
        "timestamp": iso(fake.date_time_between(start_date=f"-{timespan_days}d", end_date="now")),
        "items": items,
        "subtotal": subtotal,
        "discount": discount,
        "total": total,
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(TX_STATUSES)
    }


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="8GB-friendly AUCA e-commerce synthetic dataset generator")
    parser.add_argument("--out", default=".", help="Output directory (default: current folder)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")

    # 8GB-safe defaults
    parser.add_argument("--num-users", type=int, default=10000)
    parser.add_argument("--num-products", type=int, default=5000)
    parser.add_argument("--num-categories", type=int, default=25)
    parser.add_argument("--num-transactions", type=int, default=70000)
    parser.add_argument("--num-sessions", type=int, default=150000)
    parser.add_argument("--timespan-days", type=int, default=90)

    parser.add_argument("--sessions-chunk-size", type=int, default=30000, help="Sessions per file (default: 50,000)")
    parser.add_argument("--subcats-min", type=int, default=2)
    parser.add_argument("--subcats-max", type=int, default=6)

    parser.add_argument("--progress-every", type=int, default=20000, help="Print progress every N sessions (default 20,000)")

    args = parser.parse_args()

    random.seed(args.seed)
    fake = Faker()
    Faker.seed(args.seed)

    out_dir = args.out
    os.makedirs(out_dir, exist_ok=True)

    # --- Categories ---
    print("Generating categories...")
    categories = gen_categories(fake, args.num_categories, (args.subcats_min, args.subcats_max))
    cat_path = os.path.join(out_dir, "categories.json")
    write_json_array(cat_path, iter(categories), label="categories")
    print(f"Saved categories: {len(categories):,} -> {cat_path}")

    # --- Products ---
    print("Generating products...")
    products = gen_products(fake, categories, args.num_products, args.timespan_days)
    inv = Inventory(products)
    prod_path = os.path.join(out_dir, "products.json")
    write_json_array(prod_path, iter(products), label="products")
    print(f"Saved products: {len(products):,} -> {prod_path}")

    # --- Users ---
    print("Generating users...")
    users = gen_users(fake, args.num_users, args.timespan_days)
    user_path = os.path.join(out_dir, "users.json")
    write_json_array(user_path, iter(users), label="users")
    print(f"Saved users: {len(users):,} -> {user_path}")

    # --- Sessions + Transactions (streamed) ---
    print("Generating sessions and transactions (streamed)...")

    # Prepare transactions.json (streamed JSON array)
    tx_path = os.path.join(out_dir, "transactions.json")
    tx_written = 0
    tx_target = args.num_transactions
    sess_target = args.num_sessions

    # Session chunk files
    chunk_size = args.sessions_chunk_size
    sess_written = 0
    chunk_idx = 0

    def iter_sessions_for_chunk(n: int):
        nonlocal tx_written
        # Yield sessions and push transactions to transactions stream
        for _ in range(n):
            user = random.choice(users)
            sess_doc, tx_doc = session_record(fake, user, products, categories, inv, args.timespan_days)
            if tx_doc and tx_written < tx_target:
                # mark to be written in tx stream by outer scope
                tx_buffer.append(tx_doc)
                tx_written += 1
            yield sess_doc

    # We stream-write transactions.json as a JSON array
    os.makedirs(os.path.dirname(tx_path) or ".", exist_ok=True)
    with open(tx_path, "w", encoding="utf-8") as txf:
        txf.write("[\n")
        tx_first = True

        # Generate sessions in chunks
        while sess_written < sess_target:
            remaining = sess_target - sess_written
            this_chunk = min(chunk_size, remaining)

            sessions_file = os.path.join(out_dir, f"sessions_{chunk_idx}.json")
            tx_buffer: List[dict] = []

            # Stream-write this sessions chunk
            def sess_iter():
                return iter_sessions_for_chunk(this_chunk)

            wrote = write_json_array(
                sessions_file,
                sess_iter(),
                progress_every=0,
                label=f"sessions_{chunk_idx}"
                
            )
            sess_written += wrote

            # Flush any tx_buffer gathered during this chunk
            for tx_doc in tx_buffer:
                if not tx_first:
                    txf.write(",\n")
                tx_first = False
                txf.write(json.dumps(tx_doc, ensure_ascii=False))

            if args.progress_every and sess_written % args.progress_every == 0:
                print(f"Progress: {sess_written:,}/{sess_target:,} sessions, {tx_written:,}/{tx_target:,} transactions")

            chunk_idx += 1

        # If we still need more transactions, generate "orphan" transactions (session_id=null)
        while tx_written < tx_target:
            user = random.choice(users)
            tx_doc = gen_orphan_transaction(fake, user, products, inv, args.timespan_days)
            if not tx_doc:
                continue
            if not tx_first:
                txf.write(",\n")
            tx_first = False
            txf.write(json.dumps(tx_doc, ensure_ascii=False))
            tx_written += 1
            if tx_written % max(1, (args.progress_every // 2)) == 0:
                print(f"Progress: {sess_written:,}/{sess_target:,} sessions, {tx_written:,}/{tx_target:,} transactions")

        txf.write("\n]\n")

    # Re-save products at the end to reflect stock reductions from purchases
    prod_final_path = os.path.join(out_dir, "products.json")
    write_json_array(prod_final_path, iter(inv.products.values()), label="products_final")
    remaining_stock = sum(p["current_stock"] for p in inv.products.values())

    print("\nDataset generation complete!")
    print(f"- Sessions:      {sess_written:,} (target: {sess_target:,})")
    print(f"- Transactions:  {tx_written:,} (target: {tx_target:,})")
    print(f"- Users:         {len(users):,}")
    print(f"- Products:      {len(inv.products):,}")
    print(f"- Categories:    {len(categories):,}")
    print(f"- Remaining stock (sum): {remaining_stock:,}")
    print(f"Output folder: {os.path.abspath(out_dir)}")


if __name__ == "__main__":
    main()
