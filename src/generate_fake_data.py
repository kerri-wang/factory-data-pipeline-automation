from __future__ import annotations

import random
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


SEED = 42
random.seed(SEED)
np.random.seed(SEED)

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_orders(
    start_date: str = "2024-01-01",
    num_days: int = 60,
    avg_orders_per_day: int = 40,
) -> pd.DataFrame:
    plants = ["NJ01", "NJ02", "SH01"]
    suppliers = [f"SUP{str(i).zfill(3)}" for i in range(1, 21)]
    materials = [f"MAT{str(i).zfill(4)}" for i in range(1, 81)]

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    all_rows: list[dict] = []
    order_counter = 100000

    for day_offset in range(num_days):
        current_day = start_dt + timedelta(days=day_offset)

        # Intentionally create one low-volume day for later alert testing
        if day_offset == 35:
            daily_orders = max(8, avg_orders_per_day // 4)
        else:
            daily_orders = max(15, int(np.random.normal(avg_orders_per_day, 8)))

        for _ in range(daily_orders):
            order_counter += 1
            order_ts = current_day + timedelta(
                hours=random.randint(6, 18),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            expected_delivery_date = order_ts.date() + timedelta(days=random.randint(2, 10))
            qty = int(max(1, np.random.normal(120, 35)))

            row = {
                "order_id": f"ORD{order_counter}",
                "plant_id": random.choice(plants),
                "supplier_id": random.choice(suppliers),
                "material_id": random.choice(materials),
                "order_date": order_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "expected_delivery_date": expected_delivery_date.strftime("%Y-%m-%d"),
                "quantity": qty,
                "status": random.choice(
                    ["Created", "CREATED", "Open", "OPEN", "Confirmed", "CONFIRMED"]
                ),
                "updated_at": (
                    order_ts + timedelta(hours=random.randint(0, 48))
                ).strftime("%Y-%m-%d %H:%M:%S"),
            }
            all_rows.append(row)

    df = pd.DataFrame(all_rows)

    # 1) Inject duplicate records
    dup_sample = df.sample(frac=0.03, random_state=SEED)
    df = pd.concat([df, dup_sample], ignore_index=True)

    # 2) Inject missing supplier_id values
    null_idx = df.sample(frac=0.02, random_state=SEED + 1).index
    df.loc[null_idx, "supplier_id"] = None

    # 3) Inject negative quantity values
    neg_idx = df.sample(frac=0.01, random_state=SEED + 2).index
    df.loc[neg_idx, "quantity"] = -df.loc[neg_idx, "quantity"].abs()

    # 4) Inject more inconsistent status values
    messy_status_idx = df.sample(frac=0.03, random_state=SEED + 3).index
    df.loc[messy_status_idx, "status"] = np.random.choice(
        ["created ", " open", "confirmed", "In Progress"], size=len(messy_status_idx)
    )

    # Shuffle rows to simulate a realistic raw data feed
    df = df.sample(frac=1, random_state=SEED + 4).reset_index(drop=True)
    return df


def generate_deliveries(orders_df: pd.DataFrame) -> pd.DataFrame:
    deliveries: list[dict] = []
    delivery_counter = 500000

    clean_orders = orders_df.drop_duplicates(subset=["order_id"]).copy()

    for _, row in clean_orders.iterrows():
        # Not every order gets delivered immediately; keep some orders undelivered
        if random.random() < 0.82:
            delivery_counter += 1

            order_dt = datetime.strptime(row["order_date"], "%Y-%m-%d %H:%M:%S")
            expected_dt = datetime.strptime(row["expected_delivery_date"], "%Y-%m-%d")

            # Some deliveries are on time, while others are delayed
            if random.random() < 0.72:
                delivery_dt = expected_dt - timedelta(days=random.randint(0, 1))
            else:
                delivery_dt = expected_dt + timedelta(days=random.randint(1, 5))

            delivered_qty = max(1, int(abs(np.random.normal(abs(row["quantity"]), 15))))

            deliveries.append(
                {
                    "delivery_id": f"DEL{delivery_counter}",
                    "order_id": row["order_id"],
                    "delivery_date": (
                        datetime.combine(delivery_dt.date(), datetime.min.time())
                        + timedelta(hours=random.randint(7, 20), minutes=random.randint(0, 59))
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    "delivered_quantity": delivered_qty,
                    "delivery_status": random.choice(
                        ["Delivered", "DELIVERED", "Complete", "Late", "On Time"]
                    ),
                    "updated_at": (
                        order_dt + timedelta(days=random.randint(1, 10), hours=random.randint(0, 24))
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    df = pd.DataFrame(deliveries)

    # 1) Inject duplicate delivery records
    dup_sample = df.sample(frac=0.025, random_state=SEED + 10)
    df = pd.concat([df, dup_sample], ignore_index=True)

    # 2) Inject orphan deliveries that do not match any order
    orphan_count = max(5, int(len(df) * 0.01))
    orphan_rows = []
    for i in range(orphan_count):
        delivery_counter += 1
        orphan_rows.append(
            {
                "delivery_id": f"DEL{delivery_counter}",
                "order_id": f"ORDX{1000 + i}",
                "delivery_date": random_date(
                    datetime(2024, 1, 5), datetime(2024, 3, 5)
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "delivered_quantity": random.randint(20, 180),
                "delivery_status": random.choice(["Delivered", "Complete"]),
                "updated_at": random_date(
                    datetime(2024, 1, 5), datetime(2024, 3, 5)
                ).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    df = pd.concat([df, pd.DataFrame(orphan_rows)], ignore_index=True)

    # 3) Inject negative delivered_quantity values
    neg_idx = df.sample(frac=0.01, random_state=SEED + 11).index
    df.loc[neg_idx, "delivered_quantity"] = -df.loc[neg_idx, "delivered_quantity"].abs()

    # 4) Inject inconsistent delivery status values
    messy_status_idx = df.sample(frac=0.03, random_state=SEED + 12).index
    df.loc[messy_status_idx, "delivery_status"] = np.random.choice(
        ["delivered", " delivered ", "completed", "ONTIME"], size=len(messy_status_idx)
    )

    df = df.sample(frac=1, random_state=SEED + 13).reset_index(drop=True)
    return df


def save_dataframes(orders_df: pd.DataFrame, deliveries_df: pd.DataFrame) -> None:
    orders_path = RAW_DIR / "factory_orders_raw.csv"
    deliveries_path = RAW_DIR / "factory_deliveries_raw.csv"

    orders_df.to_csv(orders_path, index=False)
    deliveries_df.to_csv(deliveries_path, index=False)

    print(f"Saved orders to: {orders_path}")
    print(f"Saved deliveries to: {deliveries_path}")
    print()
    print("Orders shape:", orders_df.shape)
    print("Deliveries shape:", deliveries_df.shape)
    print()
    print("Sample orders:")
    print(orders_df.head(5).to_string(index=False))
    print()
    print("Sample deliveries:")
    print(deliveries_df.head(5).to_string(index=False))


def main() -> None:
    orders_df = generate_orders()
    deliveries_df = generate_deliveries(orders_df)
    save_dataframes(orders_df, deliveries_df)


if __name__ == "__main__":
    main()
