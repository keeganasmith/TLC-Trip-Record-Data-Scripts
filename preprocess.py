import pandas as pd

TARGET = "total_amount"

LEAKAGE_COLS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
]

DATETIME_COLS = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]


def main():
    in_path = "yellow_all.parquet"
    out_path = "yellow_all_preprocessed.parquet"

    print(f"Loading: {in_path}")
    df = pd.read_parquet(in_path)

    print(f"Rows: {len(df):,}")
    print("Columns:")
    for col in df.columns:
        print(col)

    # ---- Datetime feature engineering ----
    print("Parsing datetimes + creating time features...")

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_dayofweek"] = df["tpep_pickup_datetime"].dt.dayofweek
    df["pickup_month"] = df["tpep_pickup_datetime"].dt.month

    # Trip duration in minutes
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds()
        / 60.0
    )

    # Drop raw datetime columns
    df = df.drop(columns=DATETIME_COLS)

    # ---- Encode store_and_fwd_flag ----
    if "store_and_fwd_flag" in df.columns:
        # Y/N -> 1/0
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": 1, "N": 0})

    # ---- Drop leakage columns ----
    cols_to_drop = [c for c in LEAKAGE_COLS if c in df.columns]
    print("Dropping leakage columns:", cols_to_drop)
    df = df.drop(columns=cols_to_drop)

    # ---- Basic cleanup ----
    print("Cleaning data...")

    # Remove rows missing the target
    df = df.dropna(subset=[TARGET])

    # Remove rows missing critical engineered features
    df = df.dropna(subset=["trip_duration_min", "pickup_hour", "pickup_dayofweek"])

    # Filter out obvious garbage
    df = df[df["trip_distance"] >= 0]
    df = df[df["trip_duration_min"] > 0]

    # Some taxi datasets contain weird negative totals
    df = df[df[TARGET] >= 0]
    # Convert datetime-like columns
    for c in ["pickup_datetime", "dropoff_datetime"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
            df[c + "_epoch"] = (df[c].view("int64") // 10**9).astype("Int64")  # nullable
            df[c + "_hour"]  = df[c].dt.hour.astype("Int16")                  # nullable
            df[c + "_dow"]   = df[c].dt.dayofweek.astype("Int8")              # nullable
    
    cat_cols = ["payment_type", "vendor_id", "rate_code"]
    for c in cat_cols:
        if c in df.columns:
            df[c] = df[c].astype("category")


    print(f"Final rows: {len(df):,}")
    print("Final columns:")
    for col in df.columns:
        print(col)

    print(f"Writing: {out_path}")
    df.to_parquet(out_path, index=False)

    print("Done.")


if __name__ == "__main__":
    main()

