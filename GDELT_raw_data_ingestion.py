# Databricks notebook source
import os
import requests
import zipfile
from io import BytesIO

def download_and_extract_to_volume(timestamp: str, volume_path: str = "/Volumes/prod_lakehouse/wikigdelt_schema/gdelt_volume/"):
    url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.export.CSV.zip"
    os.makedirs(volume_path, exist_ok=True)

    print(f"📥 Downloading: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Download failed for {timestamp}. Status code: {response.status_code}")
        return

    zip_path = os.path.join(volume_path, f"{timestamp}.zip")
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(volume_path)
        print(f"✅ Extracted to: {volume_path}")
    except zipfile.BadZipFile:
        print(f"⚠️ Invalid ZIP file: {zip_path}")
    finally:
        os.remove(zip_path)  # optional: clean up zip after extraction

if __name__ == "__main__":
    user_input = input("Enter GDELT timestamp (e.g., 20240714000000) or multiple comma-separated: ")
    timestamps = [t.strip() for t in user_input.split(",")]
    volume_output = "/Volumes/prod_lakehouse/wikigdelt_schema/gdelt_volume/"

    for ts in timestamps:
        download_and_extract_to_volume(ts, volume_output)

    print("✅ All downloads and extractions completed.")
