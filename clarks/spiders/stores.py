import datetime
import json
import os.path

import scrapy
from scrapy.cmdline import execute as ex
from clarks.common_func import page_write, create_md5_hash
from clarks.db_config import  DbConfig
from fake_useragent import UserAgent
from clarks.items import dataItem

ua = UserAgent()
obj = DbConfig()
today_date = datetime.datetime.today().strftime("%d_%m_%Y")

class StoresSpider(scrapy.Spider):
    name = "stores"
    # allowed_domains = ["."]
    def start_requests(self):

        url = "https://kij46symwd-dsn.algolia.net/1/indexes/prod_store/query?x-algolia-agent=Algolia%20for%20JavaScript%20(4.14.3)%3B%20Browser%20(lite)&x-algolia-api-key=4e28949f630787f93a3ee5d8b6ede50e&x-algolia-application-id=KIJ46SYMWD"
        hashid = create_md5_hash(url)
        pagesave_dir = rf"C:/Users/Actowiz/Desktop/pagesave/clarks/{today_date}"
        file_name = fr"{pagesave_dir}/{hashid}.html"
        meta = {}
        meta['file_name'] = file_name
        meta['pagesave_dir'] = pagesave_dir
        meta['hashid'] = hashid

        if os.path.exists(file_name):
            yield scrapy.Request(
                url= 'file:///'+file_name,
                cb_kwargs= meta
            )

        payload = "{\"query\":\"\",\"hitsPerPage\":1000,\"getRankingInfo\":true,\"aroundLatLng\":\"41.8781136, -87.6297982\",\"aroundRadius\":500000000}"
        # payload = {"query":"","hitsPerPage":1000,"getRankingInfo":True,"aroundLatLng":"41.8781136, -87.6297982","aroundRadius":500000000}
        headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Origin': 'https://www.clarks.com',
            'Referer': 'https://www.clarks.com/',
            'User-Agent': ua.random,
            'content-type': 'application/x-www-form-urlencoded',
        }

        yield scrapy.Request(method='POST', url=url, headers=headers, body=payload, callback=self.parse, cb_kwargs=meta)
    def parse(self, response, **kwargs):
        file_name = kwargs['file_name']
        if not os.path.exists(file_name):
            page_write(kwargs['pagesave_dir'], file_name, response.text)

        jsn = json.loads(response.text)
        for data in jsn['hits']:
            country = data['address']['country']
            if country == 'US':
                lat = data['_geoloc']['lat']
                lng = data['_geoloc']['lng']
                store_name = data['displayName']

                address_list = list()
                street_address_list = list()

                try:town = data['address']['town']
                except:town = ''
                try:postal_code = data['address']['postalCode']
                except:postal_code = ''
                try:street_name = data['address']['streetName']
                except:street_name = ''
                try:street_number = data['address']['streetNumber']
                except:street_number = ''
                try:phone = data['address']['phone']
                except:phone = ''
                try:region = data['address']['region']
                except:region = ''
                try:province = data['address']['province']
                except:province = ''
                try:mall_name = data['address']['mallName']
                except:mall_name = ''

                if mall_name:
                    address_list.append(mall_name)
                    street_address_list.append(mall_name)
                if street_number:
                    address_list.append(street_number)
                    street_address_list.append(street_number)
                if street_name:
                    address_list.append(street_name)
                    street_address_list.append(street_name)
                if town:
                    address_list.append(town)
                if region:
                    address_list.append(region)
                if postal_code:
                    address_list.append(postal_code)
                if country:
                    address_list.append(country)

                address = ', '.join(address_list)
                street_address = ', '.join(street_address_list)

                try:
                    store_timing_list = data['openingHours']
                    store_timings = list()
                    for timing in store_timing_list:
                        day = timing['day']
                        day = day.capitalize()
                        timings = f"{timing['openingTime']}-{timing['closingTime']}"
                        store_timings.append(f'{day}: {timings}')
                    store_timings_final = ' | '.join(store_timings)
                except:
                    store_timings_final = ''
                store_no = data['objectID']
                town_split = town.split()
                town_url = ''.join(town)
                if town_url:
                    store_url = f"https://www.clarks.com/en-us/store-locator/UnitedStates/{town_url.upper()}/{store_no}"
                else:
                    store_url = f"https://www.clarks.com/en-us/store-locator/UnitedStates/{store_no}"
                city = data['timezone'].split('/')[-1]
                direction_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lng}"
                item = dataItem()
                item['store_no'] = store_no
                item['name'] = store_name
                item['latitude'] = lat
                item['longitude'] = lng
                item['street'] = street_address
                item['city'] = city
                item['state'] = region
                item['zip_code'] = postal_code
                item['county'] = city
                item['phone'] = phone
                item['open_hours'] = store_timings_final
                item['url'] = store_url
                item['provider'] = "Clarks"
                item['category'] = "Apparel And Accessory Stores"
                item['updated_date'] = datetime.datetime.today().strftime("%d-%m-%Y")
                item['country'] = "US"
                if store_timings_final:
                    day_today = datetime.datetime.today().strftime("%A")
                    if day_today in store_timings_final:
                        item['status'] = "Open"
                    else:
                        item['status'] = 'Close'
                else:
                    item['status'] = "Close"
                item['direction_url'] = direction_url
                item['pagesave_path'] = file_name
                yield item

if __name__ == '__main__':
    ex("scrapy crawl stores".split())