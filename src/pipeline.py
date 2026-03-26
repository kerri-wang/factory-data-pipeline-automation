from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
CLEAN_DIR = BASE_DIR / "data" / "clean"
OUTPUT_DIR = BASE_DIR / "data" / "output"

CLEAN_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def standardize_order_status(series: pd.Series) -> pd.Series:
    status_map = {
        "created": "CREATED",
        "open": "OPEN",
        "confirmed": "CONFIRMED",
        "in progress": "IN_PROGRESS",
    }
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map(status_map)
        .fillna("UNKNOWN")
    )


def standardize_delivery_status(series: pd.Series) -> pd.Series:
    status_map = {
        "delivered": "DELIVERED",
        "complete": "DELIVERED",
        "completed": "DELIVERED",
        "late": "LATE",
        "on time": "ON_TIME",
        "ontime": "ON_TIME",
    }
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .map(status_map)
        .fillna("UNKNOWN")
    )


def build_source_layer(
    orders_raw: pd.DataFrame, deliveries_raw: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    orders_source = orders_raw.copy()
    deliveries_source = deliveries_raw.copy()

    orders_source["order_date"] = pd.to_datetime(orders_source["order_date"], errors="coerce")
    orders_source["expected_delivery_date"] = pd.to_datetime(
        orders_source["expected_delivery_date"], errors="coerce"
    )
    orders_source["updated_at"] = pd.to_datetime(orders_source["updated_at"], errors="coerce")

    deliveries_source["delivery_date"] = pd.to_datetime(
        deliveries_source["delivery_date"], errors="coerce"
    )
    deliveries_source["updated_at"] = pd.to_datetime(
        deliveries_source["updated_at"], errors="coerce"
    )

    return orders_source, deliveries_source


def build_prepare_layer(
    orders_source: pd.DataFrame, deliveries_source: pd.DataFrame
) -> pd.DataFrame:
    orders_prepare = orders_source.copy()
    deliveries_prepare = deliveries_source.copy()

    orders_prepare = orders_prepare.drop_duplicates(subset=["order_id"], keep="last")
    deliveries_prepare = deliveries_prepare.drop_duplicates(subset=["delivery_id"], keep="last")

    orders_prepare["status"] = standardize_order_status(orders_prepare["status"])
    deliveries_prepare["delivery_status"] = standardize_delivery_status(
        deliveries_prepare["delivery_status"]
    )

    orders_prepare = orders_prepare[orders_prepare["quantity"] > 0]
    deliveries_prepare = deliveries_prepare[deliveries_prepare["delivered_quantity"] > 0]

    orders_prepare = orders_prepare.dropna(
        subset=["order_id", "order_date", "expected_delivery_date", "quantity"]
    )
    deliveries_prepare = deliveries_prepare.dropna(
        subset=["delivery_id", "order_id", "delivery_date", "delivered_quantity"]
    )

    prepare_df = orders_prepare.merge(
        deliveries_prepare,
        on="order_id",
        how="left",
        suffixes=("_order", "_delivery"),
    )

    prepare_df["delivery_delay_days"] = (
        prepare_df["delivery_date"] - prepare_df["expected_delivery_date"]
    ).dt.days

    prepare_df["on_time_flag"] = (
        prepare_df["delivery_delay_days"].fillna(999) <= 0
    ).astype(int)

    prepare_df["has_delivery"] = prepare_df["delivery_id"].notna().astype(int)
    prepare_df["order_day"] = prepare_df["order_date"].dt.date

    return prepare_df


def build_semantic_layer(prepare_df: pd.DataFrame) -> pd.DataFrame:
    semantic_df = (
        prepare_df.groupby("order_day", as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_deliveries=("delivery_id", "nunique"),
            on_time_delivery_rate=("on_time_flag", "mean"),
            avg_delivery_delay_days=("delivery_delay_days", "mean"),
        )
        .rename(columns={"order_day": "date"})
    )

    semantic_df["on_time_delivery_rate"] = semantic_df["on_time_delivery_rate"].round(4)
    semantic_df["avg_delivery_delay_days"] = semantic_df["avg_delivery_delay_days"].round(2)

    return semantic_df


def main() -> None:
    orders_path = RAW_DIR / "factory_orders_raw.csv"
    deliveries_path = RAW_DIR / "factory_deliveries_raw.csv"

    orders_raw = pd.read_csv(orders_path)
    deliveries_raw = pd.read_csv(deliveries_path)

    orders_source, deliveries_source = build_source_layer(orders_raw, deliveries_raw)
    prepare_df = build_prepare_layer(orders_source, deliveries_source)
    semantic_df = build_semantic_layer(prepare_df)

    clean_output_path = CLEAN_DIR / "factory_orders_clean.csv"
    kpi_output_path = OUTPUT_DIR / "daily_factory_kpi.csv"

    prepare_df.to_csv(clean_output_path, index=False)
    semantic_df.to_csv(kpi_output_path, index=False)

    print(f"Saved prepare layer to: {clean_output_path}")
    print(f"Saved semantic layer to: {kpi_output_path}")
    print()
    print("Prepare layer shape:", prepare_df.shape)
    print("Semantic layer shape:", semantic_df.shape)
    print()
    print("Sample semantic output:")
    print(semantic_df.head().to_string(index=False))


if __name__ == "__main__":
    main()
