import argparse
from pyspark.sql import SparkSession, functions as F

def main(in_dir: str, out_dir: str):
    spark = (
        SparkSession.builder
        .appName("EcommerceBatchAnalytics")
        .master("local[2]")               # safe for 8GB
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    # ---------- Load JSON ----------
    products = spark.read.json(f"{in_dir}/products.json")
    tx = spark.read.json(f"{in_dir}/transactions.json")

    # explode items
    tx_items = (
        tx.select("transaction_id", "user_id", "timestamp", F.explode("items").alias("it"))
          .select(
              "transaction_id", "user_id", "timestamp",
              F.col("it.product_id").alias("product_id"),
              F.col("it.quantity").alias("quantity"),
              F.col("it.subtotal").alias("subtotal")
          )
    )

    # =========================================================
    # Task 1: "Users who bought X also bought Y" (Top 50 pairs)
    # =========================================================
    pairs = (
        tx_items.select("transaction_id", "product_id")
        .distinct()
        .alias("a")
        .join(
            tx_items.select("transaction_id", "product_id").distinct().alias("b"),
            on="transaction_id"
        )
        .where(F.col("a.product_id") < F.col("b.product_id"))
        .groupBy(
            F.col("a.product_id").alias("product_x"),
            F.col("b.product_id").alias("product_y")
        )
        .agg(F.count("*").alias("co_purchase_count"))
        .orderBy(F.desc("co_purchase_count"))
        .limit(50)
    )

    pairs.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/also_bought_top50")

    # ==========================================
    # Task 2: Revenue by category (Spark SQL-ish)
    # ==========================================
    prod_sel = products.select("product_id", "category_id", "name")

    revenue_by_cat = (
        tx_items.join(prod_sel, on="product_id", how="left")
        .groupBy("category_id")
        .agg(
            F.round(F.sum("subtotal"), 2).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
            F.countDistinct("transaction_id").alias("orders")
        )
        .orderBy(F.desc("revenue"))
    )

    revenue_by_cat.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/revenue_by_category")

    # ==================================
    # Task 3: Top spenders (extra insight)
    # ==================================
    top_spenders = (
        tx.groupBy("user_id")
          .agg(F.round(F.sum("total"), 2).alias("total_spent"),
               F.count("*").alias("num_orders"))
          .orderBy(F.desc("total_spent"))
          .limit(20)
    )

    top_spenders.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/top_spenders")

    spark.stop()
    print("DONE ✅ Spark outputs saved to:", out_dir)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_dir", required=True)
    ap.add_argument("--out", dest="out_dir", required=True)
    args = ap.parse_args()
    main(args.in_dir, args.out_dir)
