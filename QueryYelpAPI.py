import pickle
from shapely.geometry import box
from geopy.distance import geodesic
import tqdm
import argparse
import json
import pprint
import requests
import sys
import urllib
import numpy as np
import time
import math
import shapefile
import logging
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
# API constants, you shouldn't have to change these.
API_HOST = "https://api.yelp.com"
SEARCH_PATH = "/v3/businesses/search"
BUSINESS_PATH = "/v3/businesses/"  # Business ID will come after slash.
RADUIS_RATIO = 120000

# Defaults for our simple example.
DEFAULT_TERM = ""
SEARCH_LIMIT = 50


class Keychain:
    def __init__(self):
        self.next_key = 0
        self.request_count = 0
        with open("keys.txt", "r") as f:
            self.keys = f.read().split("\n")

    def key(self):
        if self.request_count <= 5000:
            self.request_count += 1
            return self.keys[self.next_key]
        else:
            self.request_count = 1
            self.next_key += 1
            return self.keys[self.next_key]


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.

    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.

    Returns:
        dict: The JSON response from the request.

    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = "{0}{1}".format(host, quote(path.encode("utf8")))
    headers = {"Authorization": "Bearer %s" % api_key.key()}

    # print(u'Querying {0} ...'.format(url))

    response = requests.request("GET", url, headers=headers, params=url_params)
    time.sleep(1)
    return response.json()


def search(term, api_key, latitude, longitude, radius, categories, offset):
    """Query the Search API by a search term and location.

    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.

    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        "longitude": float(longitude),
        "latitude": float(latitude),
        "categories": categories,
        "limit": SEARCH_LIMIT,
        "radius": int(radius),
        "offset": int(offset),
    }

    # print(url_params)

    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)


def query_by_cood(term, latitude, longitude, radius, categories, API_KEY, offset=0):
    response = search(term, API_KEY, latitude, longitude, radius, categories, offset)
    if "error" in response.keys():
        return None  # , None
    biz_total = response.get("total")
    allresults.append(response)
    if not biz_total:
        biz_total = 0
    if (biz_total - offset) > SEARCH_LIMIT:
        t = query_by_cood(
            term,
            latitude,
            longitude,
            radius,
            categories,
            API_KEY,
            offset + SEARCH_LIMIT,
        )
        if t is None:
            return None
        biz_total += t
    # print(longitude, latitude, radius, biz_total)

    return biz_total  # , response


def get_radius_from_shape(shape):
    bounds = shape.bounds
    return geodesic(bounds[0:2], bounds[2:4]).meters


def fishnet(geometry, threshold):
    bounds = geometry.bounds
    xmin = int(bounds[0] // threshold)
    xmax = int(bounds[2] // threshold)
    ymin = int(bounds[1] // threshold)
    ymax = int(bounds[3] // threshold)
    result = []
    for i in range(xmin, xmax + 1):
        for j in range(ymin, ymax + 1):
            b = box(
                i * threshold, j * threshold, (i + 1) * threshold, (j + 1) * threshold
            )
            g = geometry.intersection(b)
            if g.is_empty:
                continue
            result.append(g)
    return result


def too_big(poly, category, API_KEY):
    r = get_radius_from_shape(poly)
    if r > 40000:
        return True
    c = poly.centroid.bounds[0:2]
    result = query_by_cood("", c[1], c[0], r, category, API_KEY)
    if result is None:
        return None
    if isinstance(result, int):
        return result > 1000
    # print(result)
    return True


def recursive_split(poly, threshold, category, API_KEY):
    tb = too_big(poly, category, API_KEY)
    if tb is None:
        return -1
    if tb:
        fish = fishnet(poly, threshold)
        return [recursive_split(f, threshold / 2, category, API_KEY) for f in fish]
    return poly


allresults = []

if __name__ == "__main__":
    sf = shapefile.Reader("torontoBoundary_wgs84/citygcs_regional_mun_wgs84")
    with open("categories.pickle", "rb") as f:
        categories = pickle.load(f)
    lol = [sr.shape.points for sr in sf.shapeRecords()]
    points = [y for x in lol for y in x]
    torpolygon = Polygon(points).convex_hull
    keychain = Keychain()

    for category in tqdm.tqdm(categories, total=len(categories)):
        try:
            recursive_split(torpolygon, 0.5, category, keychain)
            alias = "".join([c.lower() for c in category if c.isalnum()]).replace(
                " ", ""
            )
            if len(allresults) > 0:
                with open(f"output/{alias}.pickle", "wb") as f:
                    pickle.dump(allresults, f)
            allresults = []
        except Exception as e:
            # raise e
            print(e)
            continue
