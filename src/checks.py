from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "data" / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def run_dq_checks(orders_raw: pd.DataFrame, deliveries_raw: pd.DataFrame) -> pd.DataFrame:
    dq_results = []

    duplicate_orders = orders_raw.duplicated(subset=["order_id"]).sum()
    dq_results.append(
        {
            "check_name": "duplicate_order_id_check",
            "table_name": "factory_orders_raw",
            "failed_count": int(duplicate_orders),
            "status": "PASS" if duplicate_orders == 0 else "FAIL",
        }
    )

    missing_supplier = orders_raw["supplier_id"].isna().sum()
    dq_results.append(
        {
            "check_name": "missing_supplier_id_check",
            "table_name": "factory_orders_raw",
            "failed_count": int(missing_supplier),
            "status": "PASS" if missing_supplier == 0 else "FAIL",
        }
    )

    invalid_quantity = (orders_raw["quantity"] <= 0).sum()
    dq_results.append(
        {
            "check_name": "invalid_order_quantity_check",
            "table_name": "factory_orders_raw",
            "failed_count": int(invalid_quantity),
            "status": "PASS" if invalid_quantity == 0 else "FAIL",
        }
    )

    orphan_deliveries = (~deliveries_raw["order_id"].isin(orders_raw["order_id"])).sum()
    dq_results.append(
        {
            "check_name": "orphan_delivery_check",
            "table_name": "factory_deliveries_raw",
            "failed_count": int(orphan_deliveries),
            "status": "PASS" if orphan_deliveries == 0 else "FAIL",
        }
    )

    dq_df = pd.DataFrame(dq_results)
    dq_df["check_time"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    return dq_df


def build_alert_log(kpi_df: pd.DataFrame, drop_threshold: float = -0.4) -> pd.DataFrame:
    df = kpi_df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date").reset_index(drop=True)

    df["previous_day_orders"] = df["total_orders"].shift(1)
    df["pct_change"] = (
        (df["total_orders"] - df["previous_day_orders"]) / df["previous_day_orders"]
    )

    alert_df = df[df["pct_change"] < drop_threshold].copy()

    if alert_df.empty:
        return pd.DataFrame(
            columns=[
                "alert_time",
                "alert_type",
                "date",
                "current_value",
                "previous_value",
                "pct_change",
                "message",
            ]
        )

    alert_df["alert_time"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    alert_df["alert_type"] = "ORDER_VOLUME_DROP"
    alert_df["current_value"] = alert_df["total_orders"]
    alert_df["previous_value"] = alert_df["previous_day_orders"]
    alert_df["pct_change"] = alert_df["pct_change"].round(4)
    alert_df["message"] = (
        "Daily total_orders dropped by more than 40% compared with the previous day."
    )

    return alert_df[
        [
            "alert_time",
            "alert_type",
            "date",
            "current_value",
            "previous_value",
            "pct_change",
            "message",
        ]
    ]


def main() -> None:
    orders_path = RAW_DIR / "factory_orders_raw.csv"
    deliveries_path = RAW_DIR / "factory_deliveries_raw.csv"
    kpi_path = OUTPUT_DIR / "daily_factory_kpi.csv"

    orders_raw = pd.read_csv(orders_path)
    deliveries_raw = pd.read_csv(deliveries_path)
    kpi_df = pd.read_csv(kpi_path)

    dq_df = run_dq_checks(orders_raw, deliveries_raw)
    alert_df = build_alert_log(kpi_df)

    dq_output_path = OUTPUT_DIR / "dq_report.csv"
    alert_output_path = OUTPUT_DIR / "alert_log.csv"

    dq_df.to_csv(dq_output_path, index=False)
    alert_df.to_csv(alert_output_path, index=False)

    print(f"Saved DQ report to: {dq_output_path}")
    print(f"Saved alert log to: {alert_output_path}")
    print()
    print("DQ report:")
    print(dq_df.to_string(index=False))
    print()
    print("Alert log:")
    if alert_df.empty:
        print("No alerts triggered.")
    else:
        print(alert_df.to_string(index=False))


if __name__ == "__main__":
    main()
