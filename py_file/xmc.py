# -*- coding: utf-8 -*-
# @Author: monkey-hjy
# @Date:   2021-03-17 15:15:36
# @Last Modified by:   Monkey
# @Last Modified time: 2021-05-07 09:58:05
import requests
from lxml import etree
import logging
import datetime
import json
import os

BasePath=os.getcwd()
print(BasePath)
ExistsPath=os.path.join(BasePath, "exist_news_url.txt")
SpiderPath = os.path.join(BasePath, "spider_log")
spider_log = os.path.join(SpiderPath, f'xmc-{str(datetime.datetime.now()).split(" ")[0]}.log')
if not os.path.exists(SpiderPath):
    os.makedirs(SpiderPath)   

school_name_file = os.path.join(BasePath, "school_name.txt")
EXIST_NEWS_URL = [i.replace('\n', '') for i in open(ExistsPath, 'r', encoding='utf8').readlines()]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    #filename=f'/home/ec2-user/spider_log/xmc-{str(datetime.datetime.now()).split(" ")[0]}.log',
    filename=spider_log,
    filemode='w',
)


logging.info('xmc start at:{}'.format(datetime.datetime.now()))

class Spider:
    """
    小木虫爬虫
    """

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36'}
        self.url = 'http://muchong.com/bbs/search.php'
        self.wds = [
            "Artificial Intelligence",
            "machine learning",
            "deep learning",
            "Computer Vision",
            "natural language processing",
            "recommender system",
            "big data",
            "data science",
            "Computer Science",
        ]
        self.school_name = [i.replace('\n', '') for i in open(school_name_file, encoding='utf8').readlines()]

    @staticmethod
    def save_data(item):
        #url = 'http://ape-ai-api.ap-southeast-1.elasticbeanstalk.com/mapeai/post/addPost'
        url = "http://localhost:3000/mapeai/post/addPost"
        headers = {"content-type": "application/json"}
        with open(ExistsPath, 'a', encoding='utf8') as f:
            f.write(item['Url'] + '\n')
        response = requests.post(url, json=item, headers=headers)
        if response.status_code != 200:
            logging.error(f' - {datetime.datetime.now()} = 入库失败 === {json.dumps(item)} === {response.text}')
        else:
            logging.info("item:", item.title)

    def get_response(self, params):
        while True:
            try:
                response = requests.get(url=self.url, params=params, headers=self.headers, timeout=15)
                response.encoding = response.apparent_encoding
                if response.text:
                    return response
                logging.error('请求失败 === {}'.format(params))
                # print('请求失败 === {}'.format(params))
            except:
                pass

    def get_info(self, wd):
        page = 1
        while True:
            params = {
                'wd': wd.encode('gbk'),
                'fid': 430,
                'page': page
            }
            html = etree.HTML(self.get_response(params=params).text)
            content_url_list = html.xpath('//th[@class="t_new"]/span/a/@href')
            news_date = html.xpath('//td[@width="120"]/nobr/text()')
            need_crawler_url = []
            for i in range(len(news_date)):
                if (datetime.datetime.now() - datetime.datetime.strptime(news_date[i].strip(), '%Y-%m-%d %H:%M')).days <= 7:
                    need_crawler_url.append(content_url_list[i])
            logging.info('- {}'.format(params))
            logging.info('- 新数据占比：{}/{}'.format(len(need_crawler_url), len(news_date)))
            # print(' -{}'.format(params))
            if not need_crawler_url:
                break
            for content_url in need_crawler_url:
                if content_url in EXIST_NEWS_URL:
                    continue
                while True:
                    try:
                        response = requests.get(content_url, headers=self.headers, timeout=15)
                        response.encoding = response.apparent_encoding
                        if response.text:
                            break
                    except:
                        pass
                html = etree.HTML(response.text)
                title = ''.join(''.join(html.xpath('//title/text()')).strip().split('-')[:-4])
                content = ''.join(html.xpath('//*[@id="pid1"]/tr[1]/td[2]/div/div[1]//text()')).strip()
                school_name = None
                for sn in self.school_name:
                    if sn in title + content:
                        school_name = sn
                        break
                item = {
                    "Title": title if len(title) else "-",
                    "Department": school_name,
                    "Content": content if len(content) else "-",
                    "Url": content_url,
                    "Subject": "小木虫",
                }
                self.save_data(item)
                logging.info(content_url, title, len(content), school_name)
                # print(datetime.datetime.now(), content_url, title, len(content), school_name, '入库成功')
            page += 1

    def run(self):
        for key in self.wds:
            self.get_info(key)


if __name__ == "__main__":
    t = Spider()
    t.run()
