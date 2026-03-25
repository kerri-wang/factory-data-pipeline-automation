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


def clean_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    df = orders_df.copy()

    df = df.drop_duplicates(subset=["order_id"], keep="last")

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["expected_delivery_date"] = pd.to_datetime(df["expected_delivery_date"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    df["status"] = standardize_order_status(df["status"])

    df = df[df["quantity"] > 0]
    df = df.dropna(subset=["order_id", "order_date", "expected_delivery_date", "quantity"])

    return df


def clean_deliveries(deliveries_df: pd.DataFrame) -> pd.DataFrame:
    df = deliveries_df.copy()

    df = df.drop_duplicates(subset=["delivery_id"], keep="last")

    df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce")
    df["updated_at"] = pd.to_datetime(df["updated_at"], errors="coerce")

    df["delivery_status"] = standardize_delivery_status(df["delivery_status"])

    df = df[df["delivered_quantity"] > 0]
    df = df.dropna(subset=["delivery_id", "order_id", "delivery_date", "delivered_quantity"])

    return df


def build_clean_table(orders_df: pd.DataFrame, deliveries_df: pd.DataFrame) -> pd.DataFrame:
    df = orders_df.merge(
        deliveries_df,
        on="order_id",
        how="left",
        suffixes=("_order", "_delivery"),
    )

    df["delivery_delay_days"] = (
        df["delivery_date"] - df["expected_delivery_date"]
    ).dt.days

    df["on_time_flag"] = (
        df["delivery_delay_days"].fillna(999) <= 0
    ).astype(int)

    df["has_delivery"] = df["delivery_id"].notna().astype(int)
    df["order_day"] = df["order_date"].dt.date

    return df


def build_daily_kpi(clean_df: pd.DataFrame) -> pd.DataFrame:
    kpi_df = (
        clean_df.groupby("order_day", as_index=False)
        .agg(
            total_orders=("order_id", "nunique"),
            total_deliveries=("delivery_id", "nunique"),
            on_time_delivery_rate=("on_time_flag", "mean"),
            avg_delivery_delay_days=("delivery_delay_days", "mean"),
        )
        .rename(columns={"order_day": "date"})
    )

    kpi_df["on_time_delivery_rate"] = kpi_df["on_time_delivery_rate"].round(4)
    kpi_df["avg_delivery_delay_days"] = kpi_df["avg_delivery_delay_days"].round(2)

    return kpi_df


def main() -> None:
    orders_path = RAW_DIR / "factory_orders_raw.csv"
    deliveries_path = RAW_DIR / "factory_deliveries_raw.csv"

    orders_df = pd.read_csv(orders_path)
    deliveries_df = pd.read_csv(deliveries_path)

    clean_orders_df = clean_orders(orders_df)
    clean_deliveries_df = clean_deliveries(deliveries_df)

    clean_df = build_clean_table(clean_orders_df, clean_deliveries_df)
    kpi_df = build_daily_kpi(clean_df)

    clean_output_path = CLEAN_DIR / "factory_orders_clean.csv"
    kpi_output_path = OUTPUT_DIR / "daily_factory_kpi.csv"

    clean_df.to_csv(clean_output_path, index=False)
    kpi_df.to_csv(kpi_output_path, index=False)

    print(f"Saved clean table to: {clean_output_path}")
    print(f"Saved KPI table to: {kpi_output_path}")
    print()
    print("Clean table shape:", clean_df.shape)
    print("KPI table shape:", kpi_df.shape)
    print()
    print("Sample KPI output:")
    print(kpi_df.head().to_string(index=False))


if __name__ == "__main__":
    main()
