# -*- coding: utf-8 -*-
import scrapy
from urllib.parse import urlencode
from urllib.parse import urlparse
import json
from datetime import datetime

# scrapy crawl scholar_profile -o test.csv


API_KEY = '02110d4064bbd76bd0f894465d454dab'


def get_attr_list(selector, path):
    attributes = []
    for element in selector.xpath(path):
        for index, attribute in enumerate(element.xpath('@*'), start=1):
            attribute_name = element.xpath('name(@*[%d])' % index).extract_first()
            # attributes.append((attribute_name, attribute.extract()))
            attributes.append(attribute_name)
    return attributes


def get_url(url):
    payload = {'api_key': API_KEY, 'url': url, 'country_code': 'br'}
    proxy_url = 'http://api.scraperapi.com/?' + urlencode(payload)
    return proxy_url


class ExampleSpider(scrapy.Spider):
    name = 'scholar_profile'
    allowed_domains = ['api.scraperapi.com']

    def start_requests(self):
        queries = ['twRpocQAAAAJ'] # , 'twRpocQAAAAJ', 'NVdPYsQAAAAJ, z8m6hYEAAAAJ'
        for query in queries:
            url = 'https://scholar.google.com/citations?' +\
                  urlencode({'hl': 'pt-BR', 'user': query, 'pagesize': 8000,
                             'cstart': 0})
            print(f"\n{url}")
            yield scrapy.Request(get_url(url), callback=self.parse,
                                 meta={'position': 0, "user": query})

    def parse(self, response):
        global url_paper
        print(f"\n{response.url}\n")
        position = response.meta['position']
        user = response.meta['user']
        for res in response.xpath('/html/body/div/div[13]/div[2]/div/div[4]/form/div[1]/table/tbody/tr'):
            # link = res.xpath('.//h3/a/@href').extract_first()
            title = res.xpath('.//td[1]/a//text()').extract()[0]
            authors = res.xpath('.//td[1]/div[1]//text()').extract()[0]
            location = res.xpath('.//td[1]/div[2]//text()').extract()
            location = location[0] if location else None
            year = res.xpath('.//td[3]/span//text()').extract()
            year = year[0] if year else None
            modal_url = 'https://scholar.google.com' + \
                        res.xpath('td[1]/a/@data-href').extract_first()
            item = {'title': title, 'authors': authors,
                    'location': location, 'year': year, 'url': modal_url}
            yield item
        if "disabled" not in get_attr_list(response, "//*[@id='gsc_bpf_more']"):
            url = 'https://scholar.google.com/citations?' + \
                  urlencode({'hl': 'pt-BR', 'user': user, 'pagesize': 100,
                             'cstart': position + 100})
            yield scrapy.Request(get_url(url), callback=self.parse,
                                 meta={'position': position + 100,
                                       "user": user})
