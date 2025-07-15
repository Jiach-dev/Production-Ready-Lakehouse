import os
import requests
import zipfile
from io import BytesIO

def download_and_extract_to_local(timestamp: str, local_dir: str = "/Users/jhenock/Documents/project-04/project-04/gdelt"):
    url = f"http://data.gdeltproject.org/gdeltv2/{timestamp}.export.CSV.zip"
    os.makedirs(local_dir, exist_ok=True)

    print(f"📥 Downloading: {url}")
    response = requests.get(url)
    if response.status_code != 200:
        print(f"❌ Download failed for {timestamp}. Status code: {response.status_code}")
        return

    zip_path = os.path.join(local_dir, f"{timestamp}.zip")
    with open(zip_path, 'wb') as f:
        f.write(response.content)

    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(local_dir)
        print(f"✅ Extracted to: {local_dir}")
    except zipfile.BadZipFile:
        print(f"⚠️ Invalid ZIP file: {zip_path}")

if __name__ == "__main__":
    user_input = input("Enter GDELT timestamp (e.g., 20240714000000) or multiple comma-separated: ")
    timestamps = [t.strip() for t in user_input.split(",")]
    output_dir = input("Enter local directory to save (default: /tmp/gdelt): ").strip() or "/tmp/gdelt"

    for ts in timestamps:
        download_and_extract_to_local(ts, output_dir)
    print("All downloads completed.")