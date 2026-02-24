import pandas as pd
import numpy as np

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

def _downcast_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Downcast numeric columns in-place where safe."""
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_float_dtype(s):
            # NYC taxi is usually fine with float32; if you need exact cents, keep float64.
            df[c] = pd.to_numeric(s, downcast="float")
        elif pd.api.types.is_integer_dtype(s):
            df[c] = pd.to_numeric(s, downcast="integer")
    return df

def main():
    in_path = "yellow_all.parquet"
    out_path = "yellow_all_preprocessed.parquet"

    print(f"Loading: {in_path}")

    # ---- Column pruning: only read what you need ----
    # Start with all columns (from metadata) then drop what you definitely don't need.
    all_cols = pd.read_parquet(in_path, engine="pyarrow", columns=None).columns  # metadata-only load is not guaranteed
    # If the line above actually loads data for you, remove it and hardcode needed columns instead.

    # Safer: just hardcode a minimal set you know you use.
    needed = set([
        TARGET,
        "trip_distance",
        "store_and_fwd_flag",
        "payment_type",
        "vendor_id",
        "rate_code",
        *DATETIME_COLS,
        # optional legacy cols you referenced:
        "pickup_datetime",
        "dropoff_datetime",
        *LEAKAGE_COLS,  # read so we can drop? you can also just NOT read them.
    ])

    # Only keep columns that exist in file (avoid read error)
    # To do this without loading the full file, use pyarrow metadata if available:
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(in_path)
        file_cols = pf.schema.names
        columns = [c for c in needed if c in file_cols]
    except Exception:
        # fallback: try reading only needed (may error if missing)
        columns = list(needed)

    # IMPORTANT: if you never use leakage cols, don't read them at all (best).
    columns = [c for c in columns if c not in LEAKAGE_COLS]

    df = pd.read_parquet(in_path, engine="pyarrow", columns=columns)
    print(f"Rows: {len(df):,}")

    # ---- Parse datetimes (arrow often already gives datetime64[ns], so this may be no-op) ----
    for c in DATETIME_COLS:
        if c in df.columns and not pd.api.types.is_datetime64_any_dtype(df[c]):
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # ---- Feature engineering using smaller dtypes ----
    # Use UInt8 / Int16 / float32 to cut memory hard.
    if "tpep_pickup_datetime" in df.columns:
        dt = df["tpep_pickup_datetime"]
        df["pickup_hour"] = dt.dt.hour.astype("UInt8")
        df["pickup_dayofweek"] = dt.dt.dayofweek.astype("UInt8")
        df["pickup_month"] = dt.dt.month.astype("UInt8")

    if "tpep_dropoff_datetime" in df.columns and "tpep_pickup_datetime" in df.columns:
        dur = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
        df["trip_duration_min"] = (dur / 60.0).astype("float32")

    # Drop raw datetime cols in-place (no df= copy)
    drop_dt = [c for c in DATETIME_COLS if c in df.columns]
    if drop_dt:
        df.drop(columns=drop_dt, inplace=True)

    # ---- Encode store_and_fwd_flag in-place to tiny ints ----
    if "store_and_fwd_flag" in df.columns:
        df["store_and_fwd_flag"] = df["store_and_fwd_flag"].map({"Y": 1, "N": 0}).astype("UInt8")

    # ---- Basic cleanup with ONE combined mask (avoids multiple full-frame copies) ----
    mask = pd.Series(True, index=df.index)

    if TARGET in df.columns:
        mask &= df[TARGET].notna()
        mask &= df[TARGET] >= 0

    for c in ["trip_duration_min", "pickup_hour", "pickup_dayofweek"]:
        if c in df.columns:
            mask &= df[c].notna()

    if "trip_distance" in df.columns:
        mask &= df["trip_distance"].notna()
        mask &= df["trip_distance"] >= 0

    if "trip_duration_min" in df.columns:
        mask &= df["trip_duration_min"] > 0

    df = df.loc[mask].copy()  # one copy, once

    # ---- Optional legacy datetime conversions (keep them lean) ----
    for c in ["pickup_datetime", "dropoff_datetime"]:
        if c in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df[c]):
                df[c] = pd.to_datetime(df[c], errors="coerce")

            # Use int64 epoch seconds (non-nullable) if you can tolerate dropping NaT rows,
            # otherwise keep nullable Int64 (more memory).
            epoch = (df[c].view("int64") // 10**9)
            df[c + "_epoch"] = pd.Series(epoch, index=df.index).astype("int64")

            df[c + "_hour"] = df[c].dt.hour.astype("UInt8")
            df[c + "_dow"] = df[c].dt.dayofweek.astype("UInt8")

            # Consider dropping original c to save memory:
            # df.drop(columns=[c], inplace=True)

    # ---- Categoricals (big memory win) ----
    for c in ["payment_type", "vendor_id", "rate_code"]:
        if c in df.columns:
            df[c] = df[c].astype("category")

    # ---- Downcast remaining numeric columns ----
    _downcast_numeric(df)

    print(f"Final rows: {len(df):,}")
    print(f"Writing: {out_path}")

    # ---- Smaller parquet output ----
    # dictionary encoding helps for categories; zstd is great compression if available.
    df.to_parquet(
        out_path,
        index=False,
        engine="pyarrow",
        compression="zstd",
        use_dictionary=True,
    )

    print("Done.")

if __name__ == "__main__":
    main()
