import gc
import logging
import os
import random
import re
import threading
import time

import pymongo
import requests
from fake_useragent import UserAgent
from lxml import etree
from retrying import retry

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s[%(lineno)d] %(levelname)s: %(message)s'
    )


BASE_URL = 'https://movie.douban.com/'
movie_api = 'j/new_search_subjects?sort=U&range=0,10&tags=电影&start={}'
PAGE = 9900
PROXY = {'http': '47.104.14.188:16819', 'https': '47.104.14.188:16819'}
KDL_URL = 'https://dps.kdlapi.com/api/getdps/?orderid=911145855043046&num=1&pt=1&format=json&sep=1'


class DoubanSpider(threading.Thread):
    def __init__(self, page):
        super().__init__()
        self.page = page    # 页面偏移数量 start
        self.ua = UserAgent()
        self.init_mongodb_client()

    def get_proxy(self):
         # 创建一次线程获取一次代理IP
        # 一个代理IP有效时常3-5分钟 折算200s
        # 一个IP获取数量： 200s ÷ 0.5 s/条 = 200 条
        r = requests.get(KDL_URL).json()
        proxy = r['data']['proxy_list']
        logging.info('Get proxy{} from kuai agent'.format(proxy))
        if len(proxy) > 0:
            return {'http': proxy[0], 'https': proxy[0]}

    def init_mongodb_client(self):
        # 初始化mongodb数据库，创建douban数据库
        self.client = pymongo.MongoClient(host='localhost', port=27017)
        self.db = self.client['douban']
        self.movies = self.db['movies']

    
    def crwal_api(self, url):
        logging.info('Crwaling %s' % url)
        headers = {'User-Agent': self.ua.random}
        try:
            r = requests.get(url, headers=headers, proxies=self.proxy)
            if r.status_code==200:
                return r
        except requests.RequestException:
            logging.warning('Error occurred while crwal {}, retry again!'.format(url))
        except Exception:
            logging.warning('Error occurred while crwal {}, retry again!'.format(url))
        
    @retry(stop_max_attempt_number=3)
    def crwal_index(self, page):
        # crwal detail page list
        # :param page: offset of the page list
        # :return urls: the datail pages
        url = os.path.join(BASE_URL, movie_api.format(page))
        data = self.crwal_api(url)

        try:
            urls = [url['url'] for url in data.json()['data']]
            return urls
        except Exception:
            logging.warning('Get None data while crwal %s' % url)
    
    @retry(stop_max_attempt_number=3)
    def crwal_detail(self, url):
        r = self.crwal_api(url)
        if r:
            return r.text

    def parse_detail(self, html, url):
        if html:
            html = etree.HTML(str(html))
            data = dict()
        else:
            return
        
        try:
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
        except Exception:
            logging.warning('parse {} failed!'.format(url))
        
        return data

    def save_mongo(self, data):
        if data:
            try:
                self.movies.update_one(
                    {'name': data['name']},
                    {'$set': data},
                    upsert=True
                )
            except Exception:
                logging.warning('{} saved faied!'.format(data['name']))

    def run(self):
        try:
            self.proxy = self.get_proxy()  # 一个线程启动时获取一个代理IP
            
            for p in range(self.page, self.page + 60, 20):
                urls = self.crwal_index(p)
                if urls:
                    for url in urls:
                        html = self.crwal_detail(url)
                        data = self.parse_detail(html, url)
                        self.save_mongo(data)
                        # time.sleep(random.random())
        finally:
            # 关闭mongodb客户端
            self.client.close()
            
            
       


if __name__ == '__main__':
    smp = threading.Semaphore(8)
    l = []
    for s in range(7000, PAGE, 20*3):
        with smp:
            db_spider = DoubanSpider(s)
            db_spider.start()
            gc.collect()
            l.append(db_spider)
            time.sleep(1)

    for i in l:      
        i.join()
            
        

