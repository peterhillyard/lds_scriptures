import datetime
import urllib.request
if __name__ == "__main__":
    urls = [
        "https://ldsguy.tripod.com/Iron-rod/kjv-lds.zip",
        "https://ldsguy.tripod.com/Iron-rod/bom.zip",
        "https://ldsguy.tripod.com/Iron-rod/dnc.zip",
        "https://ldsguy.tripod.com/Iron-rod/pofgp.zip",
    ]


    for url in urls:
        output_path = f"downloads/{url.split('/')[-1]}"

        # Download the file from the URL and save it locally
        urllib.request.urlretrieve(url, output_path)

        print(f"File downloaded and saved to {output_path}")
    
    with open("downloads/readme.md", "w") as f:
        out = f"Zip files downloaded on {datetime.datetime.now()}"
        f.write(out)
