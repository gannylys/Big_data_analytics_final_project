# plots_from_spark_outputs.py
# --------------------------------------------
# Generate report-ready plots from Spark CSV outputs
# Works on 8GB RAM (small CSVs)
# --------------------------------------------

import os
import glob
import pandas as pd
import matplotlib.pyplot as plt


# =========================
# EDIT THIS BASE PATH ONLY
# =========================
BASE_DIR = r"C:\Users\Gahigi\Desktop\E_commercemultimodel\spark_out"

OUT_DIR = os.path.join(BASE_DIR, "_plots")
os.makedirs(OUT_DIR, exist_ok=True)


def find_single_csv(folder: str) -> str:
    """Find the single part-*.csv produced by Spark in a folder."""
    pattern = os.path.join(folder, "*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No CSV found in: {folder}")
    # pick first (Spark usually writes part-00000*.csv)
    return files[0]


def save_bar(df, x_col, y_col, title, out_png, top_n=10, rotate=45):
    """Generic bar plot saver (no manual colors)."""
    d = df.copy()
    d = d.sort_values(y_col, ascending=False).head(top_n)

    plt.figure(figsize=(10, 5))
    plt.bar(d[x_col].astype(str), d[y_col])
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=rotate, ha="right")
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()


def main():
    # -------------------------
    # 1) Revenue by Category
    # -------------------------
    rev_dir = os.path.join(BASE_DIR, "revenue_by_category")
    rev_csv = find_single_csv(rev_dir)
    rev = pd.read_csv(rev_csv)

    # Ensure numeric
    rev["revenue"] = pd.to_numeric(rev["revenue"], errors="coerce")
    rev["units_sold"] = pd.to_numeric(rev["units_sold"], errors="coerce")
    rev["orders"] = pd.to_numeric(rev["orders"], errors="coerce")

    save_bar(
        rev, "category_id", "revenue",
        "Revenue by Category (Top 10)",
        os.path.join(OUT_DIR, "01_revenue_by_category_top10.png"),
        top_n=10
    )

    save_bar(
        rev, "category_id", "units_sold",
        "Units Sold by Category (Top 10)",
        os.path.join(OUT_DIR, "02_units_sold_by_category_top10.png"),
        top_n=10
    )

    # -------------------------
    # 2) Top Spenders
    # -------------------------
    spend_dir = os.path.join(BASE_DIR, "top_spenders")
    spend_csv = find_single_csv(spend_dir)
    spend = pd.read_csv(spend_csv)

    spend["total_spent"] = pd.to_numeric(spend["total_spent"], errors="coerce")
    spend["num_orders"] = pd.to_numeric(spend["num_orders"], errors="coerce")

    save_bar(
        spend, "user_id", "total_spent",
        "Top Spenders (Top 10 Users)",
        os.path.join(OUT_DIR, "03_top_spenders_top10.png"),
        top_n=10
    )

    save_bar(
        spend, "user_id", "num_orders",
        "Most Orders (Top 10 Users)",
        os.path.join(OUT_DIR, "04_top_users_by_orders_top10.png"),
        top_n=10
    )

    # -------------------------
    # 3) Also-bought pairs
    # -------------------------
    also_dir = os.path.join(BASE_DIR, "also_bought_top50")
    also_csv = find_single_csv(also_dir)
    also = pd.read_csv(also_csv)

    also["co_purchase_count"] = pd.to_numeric(also["co_purchase_count"], errors="coerce")
    also["pair"] = also["product_x"].astype(str) + " + " + also["product_y"].astype(str)

    save_bar(
        also, "pair", "co_purchase_count",
        "Frequently Bought Together (Top 10 Pairs)",
        os.path.join(OUT_DIR, "05_also_bought_pairs_top10.png"),
        top_n=10,
        rotate=60
    )

    print("DONE: Plots saved in:", OUT_DIR)
    for f in sorted(glob.glob(os.path.join(OUT_DIR, "*.png"))):
        print(" -", f)


if __name__ == "__main__":
    main()
