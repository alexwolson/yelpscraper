from bs4 import BeautifulSoup
from tqdm import tqdm
from pathlib import Path
import pandas as pd

import re


def extract_all(path, is_nr=None):
    file = []
    bid = path.parts[-1]
    for pi in path.iterdir():
        try:
            if pi.is_file() and pi.parts[-1] == "base.html":
                with open(pi, "r") as f:
                    soup = BeautifulSoup(f, "lxml")
                result = {}
                result["business_id"] = bid
                result["business_name"] = soup.find("h1", "biz-page-title").text.strip()

                t = soup.find("div", "claim-status_teaser")
                if t and t.text.strip() == "Claimed":
                    result["claimed"] = True
                else:
                    result["claimed"] = False

                t = soup.find("div", "rating-very-large")
                if t:
                    result["rating"] = float(t.img.attrs["alt"].split(" ")[0])

                t = soup.find("span", "review-count")
                if t:
                    result["total_reviews"] = int(t.text.strip().split(" ")[0])

                t = soup.find("span", "price-range")
                if t:
                    result["price_range"] = len(t.text)

                result["categories"] = [
                    t.strip()
                    for t in soup.find("span", "category-str-list").text.split(",")
                ]

                t = soup.find("div", "biz-hours")
                if t:
                    hours = t.text
                    hours = hours.replace("            ", "").replace("        ", "")
                    for i in range(100):
                        hours = hours.replace("\n\n", "\n")
                    hours = hours.strip()
                    hours = hours.split("\n")
                    if "Hours" in hours:
                        hours.remove("Hours")
                    if "Edit business info" in hours:
                        hours.remove("Edit business info")
                    if "Open now" in hours:
                        hours.remove("Open now")
                    if "Closed now" in hours:
                        hours.remove("Closed now")
                    if "By appointment only" in hours:
                        result["appointment"] = True
                        hours.remove("By appointment only")
                    else:
                        result["appointment"] = False
                    if "Add business hours" in hours:
                        result["hours"] = None
                    else:
                        result["hours"] = dict(
                            [hours[i : i + 2] for i in range(0, len(hours), 2)]
                        )

                t = soup.find("div", "short-def-list")
                if t:
                    t = t.text
                    t = t.replace("  ", "").strip()
                    for i in range(100):
                        t = t.replace("\n\n", "\n")
                    t = t.split("\n")
                    if len(t) > 1:
                        try:
                            result["attrs"] = dict(
                                [t[i : i + 2] for i in range(0, len(t), 2)]
                            )
                        except ValueError as e:
                            print(t)
                            raise e
                t = soup.find("div", "partner-attributions")
                if t:
                    result["advertiser"] = t.text

                t = soup.find("div", "from-biz-owner-content")
                if t:
                    result["from_owner_text"] = t.text.strip()
                    from_owner_images = t.find_all("img")
                    result["from_owner_images"] = [
                        (t.attrs["src"], t.attrs["alt"]) for t in from_owner_images
                    ]

                t = soup.find("div", "js-related-businesses")
                if t:
                    result["related"] = [
                        tt.attrs["href"] for tt in t.find_all("a", "biz-name")
                    ]
                file.append(result)
        except Exception as e:
            print(pi)
            raise e
    return file


if __name__ == "__main__":
    root = Path("results")
    everything = []
    for pi in tqdm(root.iterdir(), total=32165):
        file = extract_all(pi)
        for li in file:
            everything.append(li)
    df = pd.DataFrame(everything)
    df.to_csv("output_businesses.csv")
