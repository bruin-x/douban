import logging
import os
import re
import threading
import time
import random
import threading
import pymongo
import requests
from fake_useragent import UserAgent
from lxml import etree
from retrying import retry

from getCookies import save_cookies
from getCookies import read_data


logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s %(filename)s[%(lineno)d] %(levelname)s: %(message)s'
    )


BASE_URL = 'https://movie.douban.com/'
movie_api = 'j/new_search_subjects?sort=U&range=0,10&tags=电影&start={}'
PAGE = 10000
PROXY = {'http': '47.104.14.188:16819', 'https': '47.104.14.188:16819'}

class DoubanSpider(threading.Thread):
    def __init__(self, smp,page):
        super().__init__()
        self.smp = smp
        self.page = page
        self.ua = UserAgent()
        self.init_mongodb_client()
        
    def init_mongodb_client(self):
        # 初始化mongodb数据库，创建douban数据库
        client = pymongo.MongoClient(host='localhost', port=27017)
        db = client['douban']
        self.movies = db['movies']

    @retry(stop_max_attempt_number=2)
    def crwal_api(self, url):
        # crwal a url and return its json data
        logging.info('Crwaling %s' % url)
        try:
            headers = {'User-Agent': self.ua.random}
            r = requests.get(url, headers=headers, proxies=PROXY, timeout=10)
            if r.status_code == 200:
                return r
            logging.warning('Get invalid status {} while crwal {}'.format(r.status_code, url))
        except requests.RequestException:
            logging.error('Error occurred while crwal {}'.format(url), exc_info=True)

    def crwal_index(self, start):
        # crwal detail page list
        # :param start: offset of the page list
        # :return urls: the datail pages
        url = os.path.join(BASE_URL, movie_api.format(start))
        data = self.crwal_api(url).json()
        
        try:
            urls = [url['url'] for url in data['data']]
            return urls
        except KeyError:
            logging.warning('Get None data while crwal %s' % url)
    
    def crwal_detail(self, url):
        return self.crwal_api(url).text

    def parse_detail(self, html):
        html = etree.HTML(html)
        data = dict()

        # 提取电影名和年份
        name = html.xpath('//h1/span[@property="v:itemreviewed"]/text()')
        if name:
            data['name'] = name[0]
        if re.search('演唱会|演唱會', data['name']):
            return
        
        year = html.xpath('//h1/span[@property="v:itemreviewed"]/following-sibling::span[1]/text()')
        if year:
            if re.search(r'\d+',year[0]):
                data['year'] = re.search(r'\d+',year[0]).group()

        # 匹配详细信息
        spans = html.xpath('//div[@class="indent clearfix"]//div[@id="info"]/span')
        if spans:
            data['director'] = spans[0].xpath('./span[@class="attrs"]/a/text()')
            data['writer']  =  spans[1].xpath('./span[@class="attrs"]/a/text()')
            data['actor'] =  spans[2].xpath('./span[@class="attrs"]//a/text()')
            data['type'] =  spans[4].xpath('./text()')
            
            infos = spans[5].xpath('../text()')
            infos = [i.strip() for i in infos if i.strip()]
            data['country'] = infos[0]
            data['language'] = infos[1]
            data['imdbLink'] = spans[0].xpath('../a[last()]/text()')
            
        # 评分
        data['score'] = html.xpath('//div[@id="interest_sectl"]//strong/text()')[0]
        data['commetNum'] = html.xpath('//div[@id="interest_sectl"]//span[@property="v:votes"]/text()')[0]
        data['rate'] = html.xpath('//div[@id="interest_sectl"]//div[@class="ratings-on-weight"]/div/span[@class="rating_per"]/text()')
        data['rate'] = [(i+1,j) for i,j in enumerate(reversed(data['rate']))]

        return data

    def save_mongo(self, data):
        self.movies.update_one(
            {'name': data['name']},
            {'$set': data},
            upsert=True
        )
        logging.info('{} saved successful!'.format(data['name']))

    def run(self):
        """
        :param page: the page number offset
        """
        with self.smp:
            urls = self.crwal_index(self.page)
            for url in urls:
                html = self.crwal_detail(url)
                data = self.parse_detail(html)
                self.save_mongo(data)
                time.sleep(2)

    def test_ip(self):
        url = 'http://ip100.info/ip.php'
        r = requests.get(url, proxies=PROXY)
        print(r.text)

if __name__ == '__main__':
    smp = threading.Semaphore(6)
    for i in range(0, PAGE*20, 20):
        spider = DoubanSpider(smp ,i)
        spider.start()

    

