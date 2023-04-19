import pandas as pd
import re
import requests
import os
import socket
from sys import argv
from bs4 import BeautifulSoup
from time import sleep

nr = re.compile("not_recommended")


def assert_singleton(result):
    if len(result) != 1:
        return False, None
    return True, result[0]


def find_accessory_urls(soup, is_nr=False):
    successors = []
    pagecount = soup.find_all("div", "page-of-pages")
    if is_nr:
        if len(pagecount) > 0:
            pagecount = pagecount[0]
            valid = True
    else:
        valid, pagecount = assert_singleton(pagecount)
    if valid:
        pagecount = re.findall("Page 1 of (.+)", pagecount.text.strip())
        valid, pagecount = assert_singleton(pagecount)
        if valid:
            for i in range(1, int(pagecount)):
                if is_nr:
                    successors.append(f"?not_recommended_start={i*10}")
                else:
                    successors.append(f"?start={i*20}")
    return successors


def get_url(url, bid, info):
    print(f"Downloading {url}")
    sleep(1)
    r = requests.get(url)
    if r.status_code == 200:
        with open(f"results/{bid}/{info}.html", "w") as f:
            f.write(r.text)
        print(f"Download successful")
        return True, r.text
    else:
        print(f"Download unsuccessful")
        return False, None


def generate_nr_url(url):
    url_critical = url.split("/")[-1]
    return f"https://www.yelp.com/not_recommended_reviews/{url_critical}"


def handle_crawling(row, url, bid, strikes, bad_rows, is_nr=False):
    valid, data = get_url(url, bid, "base")
    if valid:
        soup = BeautifulSoup(data, "html.parser")
        successors = find_accessory_urls(soup, is_nr)

        for i, s_url in enumerate(successors):
            valid, data = get_url(f"{url}{s_url}", bid, f"succ_{i}")
            if not valid:
                strikes += 1
                bad_rows.add(bid)
                return strikes, bad_rows

    else:
        strikes += 1
        bad_rows.add(bid)
    return strikes, bad_rows


def crawl(df, completed=None):
    bad_rows = set()
    strikes = 0
    for row in df.iterrows():
        if completed is not None:
            if row[1]["id"] in completed:
                print(f"Skipping this one (completed)...")
                continue
        try:
            print(f'Scraping {row[1]["name"]}...')
            url = row[1]["url"].split("?")[0]
            nr_url = generate_nr_url(url)
            bid = row[1]["id"]
            os.system(f"mkdir results/{bid}")
            os.system(f"mkdir results/{bid}/nr")
            strikes, bad_rows = handle_crawling(row, url, bid, strikes, bad_rows)
            handle_crawling(row, nr_url, f"{bid}/nr", 0, set(), is_nr=True)
        except:
            bad_rows.add(bid)
            strikes += 1
        if strikes >= 10:
            break
    return bad_rows


if __name__ == "__main__":
    i = argv[1]
    with open(f"businesses/businesses_{i}.csv", "r") as f:
        bf = pd.read_csv(f)
    print(f"bf is {len(bf)} long")
    bad_rows = crawl(bf)
    print(f"Bad rows: {bad_rows}")
    os.system(f"echo {bad_rows} > results/bad_rows_{i}.txt")
    os.system(f"tar -zcvf results_{i}.tar.gz results")
