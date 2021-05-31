import requests
import json
from lxml import etree
from posixpath import normpath
from urllib import parse
import pandas as pd
import os
import time
import re
import datetime
from lxml.html.clean import Cleaner
import logging

BasePath=os.getcwd()
ExistsPath=os.path.join(BasePath, "exist_news_url.txt")
SpiderPath = os.path.join(BasePath, "spider_log")
tmpfile = f'{str(datetime.datetime.now()).split(" ")[0]}.log'
spider_log = os.path.join(SpiderPath, tmpfile)
if not os.path.exists(SpiderPath):
    os.makedirs(SpiderPath)
school_name_file = os.path.join(BasePath, "school_name.txt")

EXIST_NEWS_URL = [i.replace('\n', '') for i in open(ExistsPath, 'r', encoding='utf8').readlines()]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(filename)s [line:%(lineno)d] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    #filename=f'/home/ec2-user/spider_log/{str(datetime.datetime.now()).split(" ")[0]}.log',
    filename=spider_log,
    filemode='w',
)

logging.info('normal start at:{}'.format(datetime.datetime.now()))


def myjoin(base, url):
    url1 = parse.urljoin(base, url)
    arr = parse.urlparse(url1)
    path = normpath(arr[2])
    return parse.urlunparse((arr.scheme, arr.netloc, path, arr.params, arr.query, arr.fragment))


def save_data(item):
    cleaner = Cleaner(safe_attrs=['href', 'src'], remove_tags=['*'])
    #url = 'http://ape-ai-api.ap-southeast-1.elasticbeanstalk.com/mapeai/post/addPost'
    url = 'http://localhost:3000/mapeai/post/addPost'
    headers = {"content-type": "application/json"}
    data = {
        "Title": item.get('职位名称'),
        "Department": item.get('学校/院系/部门'),
        "Subject": "Xiaoyu",
        "Country": "",
        "Address": item.get('国家/地区'),
        "Type": item.get('全职/兼职'),
        "Value": item.get('薪资待遇'),
        "ContractType": item.get('合同类别'),
        "Content": cleaner.clean_html(item.get('内容')).replace('&amp', '&').replace('&lt;', '<').replace('&gt;', '>'),
        "Url": item.get('链接'),
        "PostDate": item.get('张贴日期'),
        "ExpireDate": item.get('截止日期'),
        "STag": item.get('技术标签'),
        "WTag": item.get('工作类型标签'),
        "Medias": item.get('medias')
    }
    #with open('/home/ec2-user/py_file/exist_news_url.txt', 'a', encoding='utf8') as f:
    with open(ExistsPath, 'a', encoding='utf8') as f:
        f.write(data['Url'] + '\n')
    #logging.error(f' - {datetime.datetime.now()} = 入库 === {json.dumps(data)}')
    response = requests.post(url, json=data, headers=headers)
    if response.status_code != 200:
        logging.error(f' - {datetime.datetime.now()} = 入库失败 === {json.dumps(data)}')


class Jobs:
    def __init__(self):
        self.headers = {'user-agent': 'Mozilla/5.0'}
        self.file_path = 'jobs.xlsx'

    def get_html(self, url):
        err_count = 0
        while err_count < 5:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return etree.HTML(response.text)
            else:
                err_count += 1

    @staticmethod
    def get_info(html, url, keyword):
        info_dict = re.findall(r'<script type="application/ld\+json">(.*?)</script>',
                               etree.tostring(html, encoding='utf8').decode('utf8'), re.S)
        if not info_dict:
            return
        info_dict = json.loads(info_dict[0])
        role = ','.join(html.xpath('//div[@class="row-13"]/div[1]//input/@value'))
        subject_area = ','.join(
            list(set([i.lower() for i in html.xpath('//div[@class="row-13"]/div[2]//input/@value')])))
        medias = []
        item = {
            '职位名称': info_dict.get('title', None),
            '学校/院系/部门': (info_dict.get('hiringOrganization').get('name', '') + ' - ' + info_dict.get('hiringOrganization').get('department', {'name': ''}).get('name')).strip(' - ').replace('&amp;', '&'),
            '国家/地区': info_dict.get('jobLocation')[0].get('address').get('addressLocality'),
            '合同类别': info_dict.get('employmentType').split(',')[1].split('/'),
            '薪资待遇': info_dict.get('baseSalary').get('value', None),
            '全职/兼职': info_dict.get('employmentType').split(',')[0].split('/'),
            '张贴日期': info_dict.get('datePosted', None).split('T')[0],
            '截止日期': info_dict.get('validThrough', None).split('T')[0],
            '内容': info_dict.get('description', None),
            '技术标签': subject_area.split(','),
            '工作类型标签': role.split(','),
            '链接': url,
            'medias': medias,
        }
        if keyword in item['内容'] or keyword in item['职位名称']:
            save_data(item)
        else:
            logging.info('- 疑似垃圾信息\tURL:{}\tkeyword:{}'.format(url, keyword))

    def run(self, url=None, count=1, key=None):
        if url is None:
            url = f'https://www.jobs.ac.uk/search/?keywords={key}'
        html = self.get_html(url=url)
        jobs_url = html.xpath('//div[@class="j-search-result__text"]/a/@href')
        logging.info(f' -网站：Jobs    关键词：{key}    页码：{count} = {datetime.datetime.now()} - {len(jobs_url)}')
        for i in range(len(jobs_url)):
            jobs_url[i] = myjoin('https://www.jobs.ac.uk/search/', jobs_url[i])
            if jobs_url[i] in EXIST_NEWS_URL:
                # logging.info(f' - 已存在URL === {jobs_url[i]}')
                continue
            logging.info(
                f' -网站：Jobs    关键词：{key}    页码：{count} = {datetime.datetime.now()} === {i}/{len(jobs_url)} = {jobs_url[i]}')
            try:
                self.get_info(self.get_html(jobs_url[i]), jobs_url[i], key)
            except:
                pass
        next_url = html.xpath("//a[text()=' Next ']/@href|//a[text()=' Next  ']/@href")
        if next_url:
            self.run(url=myjoin(base='https://www.jobs.ac.uk/search/', url=next_url[0]), count=count + 1, key=key)


class Academicpositions:
    def __init__(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0',
        }
        self.url = 'https://oonkp3uljw-3.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20vanilla%20JavaScript%20(lite)%203.27.0%3Binstantsearch.js%202.10.0%3BJS%20Helper%202.26.0&x-algolia-application-id=OONKP3ULJW&x-algolia-api-key=eb15157a5951afee593ed4771d2070d7'
        self.file_name = 'Academicpositions.xlsx'

    def get_html(self, url, method, data=None):
        if method == 'POST':
            response = requests.post(url, headers=self.headers, data=data, timeout=10)
            if response.status_code == 200:
                return response.json()
            else:
                logging.error(f' - {datetime.datetime.now()} === 出错 === {url}')
        else:
            response = requests.get(url, headers=self.headers, timeout=10)
            if response.status_code == 200:
                return etree.HTML(response.text)
            else:
                logging.error(f' - {datetime.datetime.now()} === 出错 === {url}')

    @staticmethod
    def get_info(html, url, keyword):
        end_time = html.xpath('//div[@class="job-details-table-wrapper"]/table//tr[5]/td[2]//text()')[0]
        if end_time != 'Unspecified':
            end_time = str(datetime.datetime.strptime(end_time, '%B %d, %Y')).split(' ')[0]
        img_name = html.xpath('//div[@class="l-col1"]//p/img/@alt')[0],
        img_src = html.xpath('//div[@class="l-col1"]//p/img/@src')
        medias = []
        for i in range(len(img_name)):
            medias.append(
                {
                    'Title': img_name[i],
                    'MediaUrl': myjoin(base='https://academicpositions.com/', url=img_src[i]),
                    "FileType": img_src[i].split('.')[-1],
                    "Type": "S",
                    "Status": "A"
                }
            )
        item = {
            '职位名称': html.xpath('//div[@class="job-details-table-wrapper"]/table//tr[1]/td[2]/text()')[0],
            '学校/院系/部门': html.xpath('//div[@class="job-details-table-wrapper"]/table//tr[2]/td[2]//text()')[0],
            '国家/地区': html.xpath('//div[@class="job-details-table-wrapper"]/table//tr[3]/td[2]//text()')[0],
            '合同类别': None,
            '薪资待遇': None,
            '全职/兼职': None,
            '张贴日期': str(datetime.datetime.strptime(html.xpath('//div[@class="job-details-table-wrapper"]/table//tr[4]/td[2]//text()')[0], '%B %d, %Y')).split(' ')[0],
            '截止日期': end_time,
            '内容': re.findall(r'JOB DESCRIPTION</div>(.*?)<div class="job-details-block-wrapper">', etree.tostring(html, encoding='utf8').decode('utf8'), re.S)[0].replace('<span class="continue-reading-btn read-more-content__read-more btn">Continue reading</span>', '').strip(),
            '工作类型标签': ''.join(html.xpath('//div[@class="job-details-table-wrapper"]/table//tr[6]/td[2]//text()')).replace('\n', '').replace('&nbsp', '').replace(' ', '').split(','),
            '技术标签': ''.join(html.xpath('//div[@class="job-details-table-wrapper"]/table//tr[7]/td[2]/span/a/text()')).replace('\n', '').replace(' ', '').split(','),
            '链接': url,
            'medias': medias,
        }
        if keyword in item['内容'] or keyword in item['职位名称']:
            save_data(item)
        else:
            logging.info('- 疑似垃圾信息\tURL:{}\tkeyword:{}'.format(url, keyword))

    def run(self, key=None):
        page = 0
        while True:
            data = {
                "requests": [
                    {
                        "indexName": "jobs:en:1",
                        "params": f"query={key}&maxValuesPerFacet=100&page={page}&filters=publishingDateTimestamp%3A1578801448%20TO%201610423848&facets=%5B%22locationArea%22%2C%22locationCity%22%2C%22locationCountry%22%2C%22mainFields%22%2C%22positions%22%2C%22employer.name%22%2C%22Agricultural%20Science%22%2C%22Anthropology%22%2C%22Architecture%20and%20Design%22%2C%22Arts%20and%20Culture%22%2C%22Biology%22%2C%22Business%20and%20Economics%22%2C%22Chemistry%22%2C%22Computer%20Science%22%2C%22Education%22%2C%22Engineering%22%2C%22Geosciences%22%2C%22History%22%2C%22Law%22%2C%22Linguistics%22%2C%22Literature%22%2C%22Mathematics%22%2C%22Medicine%22%2C%22Philosophy%22%2C%22Physics%22%2C%22Political%20Science%22%2C%22Psychology%22%2C%22Social%20Science%22%2C%22Space%20Science%22%2C%22Theology%22%2C%22location.lvl0%22%2C%22location.lvl0%22%2C%22location.lvl0%22%2C%22location.lvl0%22%5D&tagFilters="
                    }
                ]
            }
            response = self.get_html(url=self.url, method='POST', data=json.dumps(data))
            logging.info(f" -网站：Academicpositions    关键词：{key}    页码：{page} = {datetime.datetime.now()} = {len(response['results'][0]['hits'])}")
            if not len(response['results'][0]['hits']):
                break
            for i in range(len(response['results'][0]['hits'])):
                jobs_url = 'https://academicpositions.com' + response['results'][0]['hits'][i]['renderedSlug']
                if jobs_url in EXIST_NEWS_URL:
                    continue
                logging.info(
                    f" -网站：Academicpositions    关键词：{key}    页码：{page} = {datetime.datetime.now()} === {i+1}/{len(response['results'][0]['hits'])} = {jobs_url}")
                try:
                    self.get_info(self.get_html(jobs_url, method='GET'), jobs_url, key)
                except:
                    pass
            page += 1


class Timeshighereducation:
    def __init__(self):
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'}
        self.file_path = 'Timeshighereducation.xlsx'

    def get_html(self, url):
        response = requests.get(url=url, headers=self.headers, timeout=10)
        return etree.HTML(response.text)

    @staticmethod
    def get_info(html, url, keyword):
        info_dict = re.findall(r'<script type="application/ld\+json">(.*?)</script>',
                               etree.tostring(html, encoding='utf8').decode('utf8'), re.S)
        contract_type = html.xpath(
            '//div[@class="cf margin-bottom-5 job-detail-description__category-ContractType"]/dd/a/text()')
        salary = html.xpath('//div[@class="cf margin-bottom-5 job-detail-description__salary"]/dd/span/text()')
        salary = salary[0] if salary else None
        role = html.xpath(
            '//div[@class="cf margin-bottom-5 job-detail-description__category-AcademicDiscipline"]//a/text()')
        subject_area = list(set([i.lower() for i in html.xpath(
            '//div[@class="cf margin-bottom-5 job-detail-description__category-JobType"]//a/text()')]))
        medias = []
        if not info_dict:
            item = {
                '职位名称': html.xpath('//h1/text()')[0],
                '学校/院系/部门': html.xpath('//dd[@class="grid-item three-fifths portable-one-whole palm-one-half"]/a/span/text()')[0],
                '国家/地区': html.xpath('//div[@class="cf margin-bottom-5 job-detail-description__location"]/dd/span/text()')[0],
                '全职/兼职': html.xpath('//div[@class="cf job-detail-description__category-Hours"]/dd/a/text()'),
                '张贴日期': html.xpath('//div[@class="cf margin-bottom-5 job-detail-description__posted-date"]/dd/span/text()')[0],
                '截止日期': html.xpath('//div[@class="cf margin-bottom-5 job-detail-description__end-date"]/dd/text()')[0].strip(),
                '内容': re.findall(r'class="block fix-text job-description">(.*?)</div>', etree.tostring(html, encoding='utf8').decode('utf8'), re.S)[0],
            }
        else:
            info_dict = json.loads(info_dict[0].replace('&#13;', '').strip())
            hours = info_dict.get('employmentType', None)
            hours = hours if isinstance(hours, list) else [hours]
            item = {
                '职位名称': info_dict.get('title', None),
                '学校/院系/部门': info_dict.get('hiringOrganization').get('name', '') + info_dict.get('hiringOrganization').get('department', {'name': ''}).get('name'),
                '国家/地区': info_dict.get('jobLocation').get('name', None),
                '张贴日期': info_dict.get('datePosted', None).split(' ')[0],
                '截止日期': info_dict.get('validThrough', None).split(' ')[0],
                '内容': info_dict.get('description', None),
                '全职/兼职': hours
            }
        item['链接'] = url
        item['工作类型标签'] = subject_area
        item['技术标签'] = role
        item['合同类别'] = contract_type
        item['薪资待遇'] = salary
        item['medias'] = medias
        if keyword in item['内容'] or keyword in item['职位名称']:
            save_data(item)
        else:
            logging.info('- 疑似垃圾信息\tURL:{}\tkeyword:{}'.format(url, keyword))

    def run(self, url=None, count=1, key=None):
        if url is None:
            url = f'https://www.timeshighereducation.com/unijobs/listings/?Keywords={key}'
        html = self.get_html(url)
        jobs_url = list(set([myjoin(url, i.replace('\n', '').strip()).split('?')[0]
                             for i in html.xpath('//h3[@class="lister__header"]/a/@href')]))
        logging.info(
            f' -网站：Timeshighereducation    关键词：{key}    页码：{count} = {datetime.datetime.now()} === {len(jobs_url)}')
        for i in range(len(jobs_url)):
            if jobs_url[i] in EXIST_NEWS_URL:
                # logging.info(f' - 已存在URL === {jobs_url[i]}')
                continue
            try:
                self.get_info(self.get_html(jobs_url[i]), jobs_url[i], key)
            except:
                pass
            logging.info(
                f' -网站：Timeshighereducation    关键词：{key}    页码：{count} = {datetime.datetime.now()} === {i}/{len(jobs_url)} = {jobs_url[i]}')
        next_url = html.xpath('//a[@rel="next"]/@href')
        if next_url:
            self.run(url=myjoin(base='https://www.timeshighereducation.com', url=next_url[0]), count=count + 1, key=key)


if __name__ == '__main__':
    SEARCH_KEYS = ['artificial intelligence', 'machine learning', 'deep learning',
                   'computer science', 'data science', 'reinforcement learning', 'big data', 'computing']
    for key in SEARCH_KEYS:
        t = Academicpositions()
        try:
            t.run(key=key)
        except Exception as e:
            logging.error(f' -网站：Academicpositions    关键词：{key} = {e}')
        t = Timeshighereducation()
        try:
            t.run(key=key)
        except Exception as e:
            logging.error(f' -网站：Timeshighereducation    关键词：{key} = {e}')
        t = Jobs()
        try:
            t.run(key=key)
        except Exception as e:
            logging.error(f' -网站：Jobs    关键词：{key} = {e}')
        logging.info('结束')
