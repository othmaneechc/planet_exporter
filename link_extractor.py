"""
PlanetScope Satellite Image Downloader

This script provides functions to interact with the Planet API to search, retrieve, and download satellite images of which uses the template provided by
planet.com, the added parts are for handling activation delays and direct dowload.
It includes:
- `init_cnx`: Initiates a connection to the Planet API with a bounding box and sends a search request.
- `grab_img_id`: Extracts image IDs from the API response.
- `get_asset_status`: Retrieves the status of a specific image asset.
- `activate_asset`: Activates an image asset for download.
- `wait_for_activation`: Polls the asset status until it is active, printing the status every 10 seconds.
"""


import math
import time
import os
from requests.auth import HTTPBasicAuth
from concurrent.futures import ThreadPoolExecutor
from argparse import ArgumentParser
from dotenv import load_dotenv
import requests
import csv
from tqdm import tqdm


load_dotenv('/dkucc/home/nt140/planet_downloader/secret.env')

# Debug print to check if the environment variable is loaded
print("API_KEY from .env file:", os.getenv('API_KEY'))

API_KEY = os.getenv('API_KEY')

def boundingBox(lat, lon, size, res):
    """
    Return the bounding box of the AOI that is compatible with the Geojson format
    """

    earth_radius = 6371000
    angular_distance = math.degrees(0.5 * ((size * res) / earth_radius))
    osLat = angular_distance
    osLon = angular_distance
    xMin = lon - osLon
    xMax = lon + osLon
    yMin = lat - osLat
    yMax = lat + osLat
    return [[xMin, yMin], [xMax, yMin], [xMax, yMax], [xMin, yMax], [lat, lon]]

def create_filters(bounding_box, start_date="2016-08-31T00:00:00.000Z", end_date="2020-09-01T00:00:00.000Z", cloud_pct=0.5):

    """
    Create the filters for the request
    params:
        - bounding_box: defining the AOI
        - start_date: Start date of the temporal filter
        - end date: End date of the temporal filter
        - cloud_pct: Max cloud percent of the image
    """


    # geojson_geometry = {
    #     "type": "Polygon",
    #     "coordinates": [
    #         [ 
    #         [-121.59290313720705, 37.93444993515032],
    #         [-121.27017974853516, 37.93444993515032],
    #         [-121.27017974853516, 38.065932950547484],
    #         [-121.59290313720705, 38.065932950547484],
    #         [-121.59290313720705, 37.93444993515032]
    #         ]
    #     ]
    #     }

    geojson_geometry = {
        "type": "Polygon",
        "coordinates": [bounding_box]
    }
    geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": geojson_geometry
    }

    # get images acquired within a date range
    date_range_filter = {
        "type": "DateRangeFilter",
        "field_name": "acquired",
        "config": {
        "gte": start_date,
        "lte": end_date
        }
    }

    # only get images which have <50% cloud coverage
    cloud_cover_filter = {
        "type": "RangeFilter",
        "field_name": "cloud_cover",
        "config": {
        "lte": cloud_pct
        }
    }

    # combine our geo, date, cloud filters
    combined_filter = {
        "type": "AndFilter",
        "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }

    return combined_filter

def init_cnx(bounding_box, API_KEY, item_type='PSScene'):
    """
    Initiate connection to the API and sent request
    """

    combined_filter = create_filters(bounding_box)
    # API request object
    search_request = {
    "item_types": [item_type], 
    "filter": combined_filter
    }

    # fire off the POST request
    search_result = requests.post(
        'https://api.planet.com/data/v1/quick-search',
        auth=HTTPBasicAuth(API_KEY, ''),
        json=search_request
    )

    geojson = search_result.json()

    return geojson

def grab_img_id(geojson):
    image_ids = [feature['id'] for feature in geojson['features']]
    return image_ids

def get_asset_status(id0, API_KEY, item_type='PSScene'):
    id0_url = f'https://api.planet.com/data/v1/item-types/{item_type}/items/{id0}/assets'
    result = requests.get(id0_url, auth=HTTPBasicAuth(API_KEY, ''))
    return result.json()

def activate_asset(asset, API_KEY):
    activation_link = asset["_links"]["activate"]
    activation_result = requests.get(activation_link, auth=HTTPBasicAuth(API_KEY, ''))
    return activation_result.status_code

def wait_for_activation(asset, API_KEY):
    """
    This function will check if the activation status, if it is active fetch the link
    otherwise wait for activee status and immediatly get the link
    """
    self_link = asset["_links"]["_self"]
    while True:
        activation_status_result = requests.get(self_link, auth=HTTPBasicAuth(API_KEY, ''))
        status = activation_status_result.json()["status"]
        #print(f'Current status: {status}')
        if status == 'active':
            break
        time.sleep(10)
        
    return activation_status_result.json()["location"]

# def process_coordinate(lat, lon, size, res, API_KEY):
#     """
#     function to process a single pair of coordinates
#     """
#     print('starting process of coordinates')
#     bounding_box = boundingBox(lat, lon, size, res)
#     print(f'bounding box for {lat}, {lon} computed')
#     geojson = init_cnx(bounding_box, API_KEY)
#     print(f'request sent, response feature: {geojson}')
#     image_ids = grab_img_id(geojson)
#     print(f'image fetched {image_ids}')
#     if not image_ids:
#         print(f"No images found for coordinates: {lat}, {lon}")
#         return
#     id0 = image_ids[0]
#     asset_status = get_asset_status(id0, API_KEY)
#     print(f'asset_status: {asset_status}')
#     if 'ortho_analytic_4b' in asset_status:

#         if asset_status["ortho_analytic_4b"]["status"] != 'active':
#             activate_asset(asset_status["ortho_analytic_4b"], API_KEY)
#         download_link = wait_for_activation(asset_status["ortho_analytic_4b"], API_KEY)
#         print(f"Download link: {download_link}")
#     elif 'ortho_analytic_3b' in asset_status:
#         if asset_status["ortho_analytic_3b"]["status"] != 'active':
#             activate_asset(asset_status["ortho_analytic_3b"], API_KEY)
#         download_link = wait_for_activation(asset_status["ortho_analytic_3b"], API_KEY)
#         print(f"Download link: {download_link}")

#     elif 'ortho_analytic_8b' in asset_status:
#         if asset_status["ortho_analytic_8b"]["status"] != 'active':
#             activate_asset(asset_status["ortho_analytic_8b"], API_KEY)
#         download_link = wait_for_activation(asset_status["ortho_analytic_8b"], API_KEY)
#         print(f"Download link: {download_link}")

#     else:
#         print('could not find ortho_analytic_4b, ortho_analytic_3b, ortho_analytic_8b')

#     return download_link

def save_link_to_file(download_link, output_file):
    with open(output_file, 'a') as file:
        file.write(download_link + '\n')

def process_coordinate(lat, lon, size, res, API_KEY, output_file):
    """
    Function to process a single pair of coordinates
    """
    #print(f'Starting process of coordinates: {lat}, {lon}')
    bounding_box = boundingBox(lat, lon, size, res)
    geojson = init_cnx(bounding_box, API_KEY)
    image_ids = grab_img_id(geojson)
    if not image_ids:
        #print(f"No images found for coordinates: {lat}, {lon}")
        return

    id0 = image_ids[0]
    asset_status = get_asset_status(id0, API_KEY)
    download_link = None

    for asset_key in ['ortho_analytic_4b', 'ortho_analytic_3b', 'ortho_analytic_8b']:
        if asset_key in asset_status:
            asset = asset_status[asset_key]
            if asset.get('status') != 'active':
                activate_asset(asset, API_KEY)
            download_link = wait_for_activation(asset, API_KEY)
            #print(f"Download link for {lat}, {lon}: {download_link}")
            save_link_to_file(download_link, output_file)
            return

    print('Could not find any valid asset.')


def process_csv(file_path, size, res, API_KEY, output_file, max_workers=1):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        rows = list(reader)  # Convert to list to count rows for tqdm
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(rows)) as pbar:
                futures = []
                for row in rows:
                    try:
                        lat = float(row['EA_GPS_LA'])
                        lon = float(row['EA_GPS_LO'])
                        future = executor.submit(process_coordinate, lat, lon, size, res, API_KEY, output_file)
                        futures.append(future)
                    except ValueError as e:
                        print(f"Skipping row with invalid data: {row} - Error: {e}")

                # Ensure progress bar updates as futures complete
                for future in futures:
                    future.result()  # This will re-raise exceptions if they occurred
                    pbar.update(1)

        # for row in reader:
        #     try:
        #         lat = float(row['EA_GPS_LA'])
        #         lon = float(row['EA_GPS_LO'])
        #         process_coordinate(lat, lon, size, res, API_KEY)
        #         time.sleep(1)
        #     except ValueError as e:
        #         print(f"Skipping row with invalid data: {row} - Error: {e}")

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-f", "--filepath", help="path to coordinates csv file", default='../sdg6_data/locations.csv',  type=str)
    parser.add_argument("-s", "--start_date", help="start date for getting images", default='"2016-08-31T00:00:00.000Z"', type=str)
    parser.add_argument("-e", "--end_date", help="end date for getting images", default='"2016-09-30T00:00:00.000Z"', type=str)
    parser.add_argument("-he", "--height", help="height of output images (in px)", default=512, type=int)
    parser.add_argument("-w", "--width", help="width of output images (in px)", default=512, type=int)
    parser.add_argument("-r", "--resolution", help="resolution of the image", default=3, type=int)
    parser.add_argument("-o", "--output_dir", help="path to output directory", default="output_images/", type=str)
    # parser.add_argument('--parallel', action='store_true')
    # parser.add_argument('--no-parallel', dest='parallel', action='store_false')
    # parser.set_defaults(parallel=True)
    parser.add_argument("-pn", "--parallel_number", help="number of parallel processes", default=10, type=int)
    args = parser.parse_args()

    print(args)
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)

    # output file path
    output_file = os.path.join(args.output_dir, 'download_links.txt')

    process_csv(args.filepath, args.width, args.resolution, API_KEY, output_file=output_file, max_workers=args.parallel_number)




