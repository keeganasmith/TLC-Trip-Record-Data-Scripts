#!/bin/bash
for year in {2009..2025}; do
  for m in {01..12}; do

    file="yellow_tripdata_${year}-${m}.parquet"
    url="https://d37ci6vzurychx.cloudfront.net/trip-data/${file}"

    # Skip if file already exists
    if [ -f "$file" ]; then
      echo "[SKIP] $file already exists"
      continue
    fi

    echo "[DOWNLOADING] $file ..."

    # Download with curl safely
    curl -L --fail --retry 5 --retry-delay 5 \
      -A "Mozilla/5.0" \
      -o "${file}.part" \
      "$url"

    # If curl succeeded, rename temp file
    if [ $? -eq 0 ]; then
      mv "${file}.part" "$file"
      echo "[OK] Saved $file"
    else
      echo "[ERROR] Failed to download $file"
      rm -f "${file}.part"
    fi

    # Sleep to avoid overwhelming the server
    sleep 2

  done
done

