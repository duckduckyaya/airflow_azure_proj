import requests
from bs4 import BeautifulSoup
import re
from bs2json import bs2json
import json
import csv
import collections
from joblib import Parallel, delayed
from azure.storage.blob import BlockBlobService

header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
    'Referer': 'https://www.immoweb.be/'
}


def set_up_blob_service(account_name, account_key, container_name):
    block_blob_service = BlockBlobService(account_name=account_name, account_key=account_key)
    block_blob_service.create_container(container_name)
    return block_blob_service



def get_properties_data(root_url, header, page_count):
    for page in range(1, page_count + 1):
        links = root_url + str(page)
        house_html = requests.get(links, headers=header)
        soup = BeautifulSoup(house_html.text, 'html.parser')
        house_container = soup.find_all('main', class_='main')
        json1 = bs2json().convertAll(house_container, join=True)
        house_json = json1[0]['main'][0]['iw-search']['attributes'][':results']
        house_dict = json.loads(house_json)
    yield house_dict


def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.abc.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def get_links(house_dict):
    link_list = []
    for i in range(30):
        property_id = house_dict[i]['id']
        property_location = house_dict[i]['property']['location']["district"]
        property_postcode = house_dict[i]['property']['location']['postalCode']
        property_type = house_dict[i]['property']['type']
        property_transaction_type = "te-koop" if house_dict[i]['property']['isForSale'] else "te-huur"
        property_price = house_dict[i]['price']['priceValue']
        property_link = "https://www.immoweb.be/en/property/" + str(property_id)
    link_list.append({
    "id": property_id,
    "location": property_location,
    "postcode": property_postcode,
    "type": property_type,
    "transaction_type": property_transaction_type,
    "price": property_price,
    "link": property_link
    })
    return link_list

def scrape_property_info(url, header):
    page = requests.get(url, headers=header)
    soup = BeautifulSoup(page.text, 'html.parser')
    property_json = bs2json().convertAll(soup, join=True)
    property_dict = json.loads(property_json[0]['main'][0]['iw-property']['attributes'][':property'])
    property_dict = flatten(property_dict)
    return property_dict


def main(account_name, account_key, container_name, root_url, header, page_count):
    block_blob_service = set_up_blob_service(account_name, account_key, container_name)
    for house_dict in get_properties_data(root_url, header, page_count):
        links = get_links(house_dict)
    for link in links:
        try:
            property_info = scrape_property_info(link['link'], header)
            combined_dict = {**link, **property_info}
            filename = str(link['id']) + ".json"
            block_blob_service.create_blob_from_text(container_name, filename, json.dumps(combined_dict))
        except:
            pass

if __name__ == '__main__':
    main(
    account_name='<account_name>',
    account_key='<account_key>',
    container_name='<container_name>',
    root_url='https://www.immoweb.be/en/search/property/for-sale/list?page=',
    header=header,
    page_count=5
    )
