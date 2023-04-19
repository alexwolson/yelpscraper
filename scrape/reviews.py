from bs4 import BeautifulSoup
from tqdm import tqdm
from pathlib import Path
import pandas as pd

import re


def extract_count(review):
    unnamed_other = re.compile("and (\d+) others?")
    exact = re.compile("^(\d+).+ voted for this review")
    single_name = re.compile("(.+ .\.) voted for this review")
    nobody = re.compile("Was this review")
    m = unnamed_other.findall(review)
    if len(m) == 1:
        return int(m[0]) + 1
        # continue
    m = exact.findall(review)
    if len(m) == 1:
        return int(m[0])
        # continue
    m = single_name.findall(review)
    if len(m) == 1:
        return 1
        # continue
    m = nobody.findall(review)
    if len(m) == 1:
        return 0
        # continue
    return None


def extract_all(path, is_nr=None):
    if is_nr:
        bid = is_nr
    else:
        bid = path.parts[-1]
    file = []
    for pi in path.iterdir():
        try:
            if pi.is_file():
                with open(pi, "r") as f:
                    soup = BeautifulSoup(f, "lxml")
                x = soup.find_all("div", "review--with-sidebar")
                if is_nr:
                    start = 0
                else:
                    start = 1
                for rcount in range(start, len(x)):
                    xi = x[rcount]
                    result = {}
                    result["business_id"] = bid
                    result["review_id"] = xi.attrs["data-review-id"]

                    t = xi.find("span", "ghost-user")
                    if t:
                        ghost = True
                        result["user_id"] = t.text.strip()
                    else:
                        ghost = False
                        if not is_nr:
                            result["user_id"] = xi.find("a").attrs["href"].split("=")[1]

                    if not is_nr and not ghost:
                        result["vote_count"] = extract_count(
                            xi.find("p", "voting-intro").text.strip()
                        )

                    t = xi.find("li", "user-location")
                    if t:
                        result["user_loc"] = t.b.text
                    t = xi.find("li", "friend-count")
                    if t:
                        result["friend_count"] = int(t.b.text)
                    t = xi.find("li", "review-count")
                    if t:
                        result["review_count"] = int(t.b.text)
                    t = xi.find("li", "photo-count")
                    if t:
                        result["photo_count"] = int(t.b.text)

                    t = xi.find("div", "i-stars").attrs["title"]
                    if "no rating" in t:
                        result["rating"] = -1
                    else:
                        result["rating"] = float(t.split(" ")[0])

                    t = xi.find("span", "rating-qualifier")
                    if t:
                        result["review_date"] = t.text.strip()

                    t = xi.find("p")
                    if t and "lang" in t.attrs:
                        result["review_language"] = t.attrs["lang"]
                        result["review_text"] = xi.find("p").text

                    pb = xi.find("ul", "photo-box-grid")
                    img_dsc = []
                    img_url = []
                    if pb:
                        imgs = pb.find_all("img")
                        for img in imgs:
                            if "yelp_styleguide" in img.attrs["src"]:
                                continue
                            img_dsc.append(img.attrs["alt"])
                            img_url.append(img.attrs["src"])
                    result["img_dsc"] = img_dsc
                    result["img_url"] = img_url

                    if not ghost:
                        counts = xi.find_all("span", "count")
                        fixed_counts = []
                        for count in counts:
                            if count.text == "":
                                fixed_counts.append(0)
                            else:
                                fixed_counts.append(int(count.text))

                        result["ufc"] = fixed_counts
                    result["nr"] = is_nr is not None
                    result["ghost"] = ghost
                    file.append(result)
            else:
                adf = extract_all(pi, bid)
                for li in adf:
                    file.append(li)
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
    df.to_csv("output.csv")
