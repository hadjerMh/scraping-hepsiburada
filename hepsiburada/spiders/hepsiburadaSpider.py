import scrapy
import json
from math import ceil
import pandas as pd


class HPSpider(scrapy.Spider):
    #the spider name
    name = "HPSpider"
    # Initiating a data frame
    df = pd.DataFrame()
    # the url of the product to scrap as an example a used those 2 urls
    start_urls = ['https://www.hepsiburada.com/hp-15-dw3017nt-intel-core-i3-1115g4-4gb-256-gb-ssd-freedos-15-6-fhd-tasinabilir-bilgisayar-2n2r4ea-p-HBCV000007PQ8B']
    #start_urls = ['https://www.hepsiburada.com/tefal-ingenio-xl-intense-2x-19-parca-titanyum-tencere-tava-seti-2100125500-p-HBCV00001UF6H6']
    #Geting the item sku (exemple HBCV000007PQ8B)
    sku = start_urls[0].rsplit('-', 1)[-1]
    #initiating the dataframe for the csv creation for the product

    headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en,fr-FR;q=0.9,fr;q=0.8,en-US;q=0.7",
    "authorization": "Bearerundefined",
    "cache-control": "no-cache",
    "referer": f"{start_urls}-yorumlari?sayfa=1",
    "pragma": "no-cache",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Mobile Safari/537.36",
    "X-Requested-With": "Fetch",
    }

    def generate_data(self, data):
        """This method generates the dataframe and the csv file out of the scraping result

        :param data: json object containing the information from the web scraping
        """
        for item in data['data']['approvedUserContent']['approvedUserContentList']:
            temp = pd.DataFrame(
                {
                    "cargoFirm": [item['order']['cargoFirm']],
                    "merchantId": [item['order']['merchantId']],
                    "merchantName": [item['order']['merchantName']],
                    "shippingAddressCity": [item['order']['shippingAddressCity']],
                    "shippingAddressCounty": [item['order']['shippingAddressCounty']],
                    "product": [item['product']['name']],
                    "productUrl": [item['product']['url']],
                    "productSku": [item['product']['sku']],
                    "reviewContent": [item['review']['content']],
                    "nStar": [item['star']],
                    "createdAt": [item['createdAt']],
                    "customerName": [f"{item['customer']['name']} {item['customer']['surname']}"],
                    "customerBirthdate": [item['customer']['birthDate']],
                    "customerGender": [item['customer']['gender']],
                }
            )
            # Concatinating the result of the loop with the main dataframe
            self.df = pd.concat([self.df, temp])
        # Saving the dataframe to a csv file
        self.df.to_csv(f"../scraped_csv/{self.sku}.csv", index=False, sep=";", encoding='utf-8-sig')

    def parse(self, response):
        url = f"https://user-content-gw-hermes.hepsiburada.com/queryapi/v2/ApprovedUserContents?skuList={self.sku}&from=0&size=10"
        request = scrapy.Request(url, callback=self.parse_api, headers=self.headers)
        yield request

    def parse_api(self, response):
        raw_data = response.body
        data = json.loads(raw_data)
        total_elements = data['totalItemCount']
        count = data['currentItemCount']
        number_pages = ceil(total_elements/count)
        if number_pages > 1:
            list_range = [i for i in range(0, total_elements+1, count)]
            for i in list_range:
                url = f"https://user-content-gw-hermes.hepsiburada.com/queryapi/v2/ApprovedUserContents?skuList={self.sku}&from={i}&size=10"
                yield scrapy.Request(url, callback=self.hp_parse, headers=self.headers, dont_filter=True)

        elif number_pages == 1:
            self.generate_data(data=data)
            yield {
                "response": response,
            }
        else:
            yield None

    def hp_parse(self, response):
        raw_data = response.body
        data = json.loads(raw_data)
        self.generate_data(data=data)
        yield {
            "response": response,
        }
