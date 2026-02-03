import os
import json
import glob
import argparse
import happybase


def b(x):
    """Convert any value to bytes safely."""
    if x is None:
        return b""
    if isinstance(x, bytes):
        return x
    return str(x).encode("utf-8", errors="ignore")


def safe_json_bytes(obj, default=b"[]"):
    try:
        return json.dumps(obj, ensure_ascii=False).encode("utf-8")
    except Exception:
        return default


def load_one_file(table, path, max_rows_total, batch_size, already_inserted):
    print(f"\n📥 Loading: {os.path.basename(path)}")

    with open(path, "r", encoding="utf-8") as f:
        sessions = json.load(f)

    batch = table.batch(batch_size=batch_size)
    inserted_this_file = 0

    for s in sessions:
        if max_rows_total and already_inserted + inserted_this_file >= max_rows_total:
            break

        user_id = (s.get("user_id") or "").strip()
        start_time = (s.get("start_time") or "").strip()
        session_id = (s.get("session_id") or "").strip()

        if not user_id or not session_id:
            continue

        # ✅ RowKey supports PrefixFilter('user_000042|')
        rowkey = f"{user_id}|{start_time}|{session_id}".encode("utf-8", errors="ignore")

        geo = s.get("geo_data") or {}
        device = s.get("device_profile") or {}
        page_views = s.get("page_views") or []
        cart_contents = s.get("cart_contents") or {}
        viewed_products = s.get("viewed_products") or []

        # ---- store pv family (compact summary + JSON) ----
        pv_count = len(page_views)
        pv_json = safe_json_bytes(page_views, default=b"[]")

        # ---- store cart family (summary + JSON) ----
        # cart_contents might be dict or list depending on your generator
        if isinstance(cart_contents, dict):
            cart_items_count = len(cart_contents)
        elif isinstance(cart_contents, list):
            cart_items_count = len(cart_contents)
        else:
            cart_items_count = 0

        cart_json = safe_json_bytes(cart_contents, default=b"{}")

        data = {
            # ----- meta -----
            b"meta:user_id": b(user_id),
            b"meta:session_id": b(session_id),
            b"meta:start_time": b(start_time),
            b"meta:end_time": b(s.get("end_time")),
            b"meta:conversion_status": b(s.get("conversion_status")),
            b"meta:referrer": b(s.get("referrer")),

            # ----- geo -----
            b"geo:city": b(geo.get("city")),
            b"geo:state": b(geo.get("state")),
            b"geo:country": b(geo.get("country")),
            b"geo:ip_address": b(geo.get("ip_address")),

            # ----- device -----
            b"device:type": b(device.get("type")),
            b"device:os": b(device.get("os")),
            b"device:browser": b(device.get("browser")),

            # ----- stats -----
            b"stats:duration_seconds": b(s.get("duration_seconds")),
            b"stats:page_views_count": b(pv_count),
            b"stats:viewed_products_count": b(len(viewed_products)),
            b"stats:cart_distinct_items": b(cart_items_count),

            # ----- pv (page views) -----
            b"pv:count": b(pv_count),
            b"pv:page_views_json": pv_json,

            # ----- cart -----
            b"cart:distinct_items": b(cart_items_count),
            b"cart:cart_json": cart_json,

            # ----- events (keep full JSON copies too) -----
            b"events:page_views_json": pv_json,
            b"events:cart_json": cart_json,
        }

        batch.put(rowkey, data)
        inserted_this_file += 1

        if inserted_this_file % 2000 == 0:
            print(f"✅ Inserted {already_inserted + inserted_this_file:,} total sessions...")

    batch.send()
    print(f"✅ Done {os.path.basename(path)}: inserted {inserted_this_file:,}")
    return inserted_this_file


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=9090)
    ap.add_argument("--table", default="user_sessions")

    ap.add_argument("--pattern", default="sessions_*.json")
    ap.add_argument("--max-files", type=int, default=0, help="0 = all files")
    ap.add_argument("--max-rows", type=int, default=0, help="0 = all rows total")
    ap.add_argument("--batch-size", type=int, default=500)

    args = ap.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    files = sorted(glob.glob(os.path.join(base_dir, args.pattern)))

    if not files:
        raise FileNotFoundError(f"No session files found in: {base_dir} (pattern={args.pattern})")

    if args.max_files and args.max_files > 0:
        files = files[:args.max_files]

    print("📁 Folder:", base_dir)
    print("🧾 Files:", [os.path.basename(f) for f in files])
    print(f"📌 Target table: {args.table}  |  Thrift: {args.host}:{args.port}")

    conn = happybase.Connection(host=args.host, port=args.port, timeout=60000)
    conn.open()
    table = conn.table(args.table)

    total = 0
    for fp in files:
        total += load_one_file(
            table=table,
            path=fp,
            max_rows_total=args.max_rows if args.max_rows > 0 else 0,
            batch_size=args.batch_size,
            already_inserted=total,
        )
        if args.max_rows and total >= args.max_rows:
            break

    conn.close()
    print(f"\n🎉 ALL DONE. Total inserted into '{args.table}': {total:,}")


if __name__ == "__main__":
    main()
