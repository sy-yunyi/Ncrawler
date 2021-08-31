
import asyncio
import logging
import time
import os
import urllib.parse
from asyncio.exceptions import CancelledError
import pyppeteer
import aiofiles
import ipwhois
import tldextract
import hashlib
import dns.resolver
import dns.rdatatype
import whois
import redis
import datetime
import json
from ipwhois import IPWhois
from lxml import etree
from pyppeteer import launch
from asyncio import Queue
from collections import namedtuple

clogger = logging.getLogger(__name__)

URLStatistic = namedtuple("URLSstatistic",
                            ["url",
                            "fqdn",
                            "sld",
                            "redirect",
                            "pre_url",
                            "next_url",
                            "url_nums",
                            "title",
                            "html",
                            "screenshot",
                            "status",
                            "exception",
                            ])

DNSRecordStatistic = namedtuple("DNSRecordStatistic",
                            ["fqdn",
                            "sld",
                            "records",
                            "status",
                            "exception"
                            ])
WhoisStatistic = namedtuple("WhoisStatistic",
                            ["fqdn",
                            "sld",
                            "whois",
                            "status",
                            "exception"
                            ])
IPStatistic = namedtuple("IPStatistic",
                            ["ip",
                            "fqdn",
                            "ipinfo",
                            "status",
                            "exception"
                            ])

class ncrawler:
    def __init__(self,max_tasks=10,max_tries=4,loop=None,task_queue=None,redis_db = None,redis_set=None,page_timeout = 20,timeout_index = 1):
        self.max_tasks = max_tasks
        self.max_tries = max_tries
        self.loop = loop or asyncio.get_event_loop()
        self.task_queue = task_queue or Queue(loop = self.loop)
        self.done = []
        self.pool = redis.ConnectionPool(host="127.0.0.1",port=6379,db=redis_db)
        self.t0 = time.time()
        self.set_index = 0
        self.redis_set = redis_set
    
    def default(self,o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()

    def record_statistic(self,key,info_statistic):
        # 将每个URL信息，保存到redis 或 mysql
        resdb = redis.Redis(connection_pool=self.pool)
        res_id = hashlib.md5(key.encode()).hexdigest()
        resdb.set(res_id,json.dumps(info_statistic._asdict(),default=self.default))
        resdb.zadd(self.redis_set,{key:self.set_index})
        self.set_index += 1
        print(info_statistic)
        self.done.append(info_statistic)

    
    async def is_seen(self,key):
        # 是否收集过，没有收集过返回True,否则返回False
        resdb = redis.Redis(connection_pool=self.pool)
        res_id = hashlib.md5(key.encode()).hexdigest()
        if not resdb.get(res_id):
            return True
        else:
            return False
    def report(self):
        pass

    

class IPcrawler(ncrawler):
    def __init__(self,targets, max_tries=3,max_tasks = 1,loop=None,task_queue=None,redis_db = None,redis_set=None,page_timeout = 20,timeout_index = 1):
        super(IPcrawler,self).__init__(max_tries= max_tries,max_tasks=max_tasks,loop=loop,task_queue=task_queue,redis_db = redis_db,redis_set=redis_set,page_timeout = page_timeout,timeout_index = timeout_index)
        if targets:
            for target,fqdn in targets:
                self.task_queue.put_nowait((target,fqdn))
    
    async def fetch(self,ip,fqdn):
        if not await self.is_seen(ip):
            print("have done")
            return
        exception = None
        try:
            ipinfo = IPWhois(ip).lookup_whois()
        except ipwhois.exceptions.WhoisLookupError as e:
            ipinfo = "failed"
        except Exception as e:
            ipinfo = "unknown"
        self.record_statistic(ip,IPStatistic(ip=ip,
                                            fqdn=fqdn,
                                            ipinfo=ipinfo,
                                            status=200,
                                            exception=exception))
    
    async def work(self):
        try:
            while True:
                target,fqdn = await self.task_queue.get()
                if target =="done":   # 结束信号
                    return 1
                await self.fetch(target,fqdn)
                self.task_queue.task_done()
                await asyncio.sleep(3)

        except asyncio.CancelledError as e:
            print(e)
            return 

    async def crawl(self):
        self.t0 = time.time()
        workers = [asyncio.Task(self.work(),loop=self.loop) for _ in range(self.max_tasks)]
        for wi in workers:
            done = await wi
            if done:
                for wi in workers:
                    wi.cancel()

        self.t1 = time.time()

class DNSRecordcrawler(ncrawler):
    def __init__(self,targets, max_tries=3,max_tasks = 1,loop=None,task_queue=None,ip_queue=None,redis_db = None,redis_set=None,page_timeout = 20,timeout_index = 1):
        super(DNSRecordcrawler,self).__init__(max_tries= max_tries,max_tasks=max_tasks,loop=loop,task_queue=task_queue,redis_db = redis_db,redis_set=redis_set,page_timeout = page_timeout,timeout_index = timeout_index)
        if targets:
            for target in targets:
                self.task_queue.put_nowait(target)
        self.ip_queue = ip_queue
    

    # def record_statistic(self,dns_statistic):
    #     # 将每个URL信息，保存到redis 或 mysql
    #     print(dns_statistic)
    #     self.done.append(dns_statistic)

    # async def find_dns(self,target):
    #     record_types = [r.name for r in list(dns.rdatatype.RdataType)]
    #     return dns.resolver.resolve(target,rti)

    

    async def fetch(self,target):
        if not await self.is_seen(target):
            print("have done")
            return
        tries = 0
        tlde = tldextract.extract(target)
        sld = tlde.domain+"."+tlde.suffix
        exception = None
        # while tries < self.max_tries:
        #     try:
        record_types = [r.name for r in list(dns.rdatatype.RdataType)]
        records = {}
        ip_list = []
        for rti in record_types:
            try:
                answers = dns.resolver.resolve(target,rti)
                records[rti]=answers.rrset.to_text()
                if rti=="A" or rti=="AAAA":
                    ip_list.extend([ai.address for ai in answers])
            except dns.resolver.NoAnswer as e:
            # No Answer
                records[rti]="No Answer"
            except dns.resolver.NoMetaqueries as e:
                # Not Allow
                records[rti]="Not Allow"
            except dns.resolver.NoNameservers as e:
                # SERVFAIL
                records[rti]="SERVFAIL"
            except dns.resolver.NXDOMAIN as e:
                records[rti]="NXDOMAIN"
            except Exception as e:
                records[rti]="UNKNOWN"
        self.record_statistic(target,DNSRecordStatistic(fqdn=target,
                                                sld=sld,
                                                records=records,
                                                status=200,
                                                exception=exception))
        for ip in ip_list:
            if await self.is_seen(ip):
                self.ip_queue.put_nowait((ip,target))

        return 
    async def work(self):
        try:
            while True:
                target = await self.task_queue.get()
                if target =="done":   # 结束信号
                    return 1
                await self.fetch(target)
                self.task_queue.task_done()
                await asyncio.sleep(3)
 
        except asyncio.CancelledError as e:
            print(e)
            return 
    
    async def crawl(self):
        self.t0 = time.time()
        workers = [asyncio.Task(self.work(),loop=self.loop) for _ in range(self.max_tasks)]
        for wi in workers:
            done = await wi
            if done:
                self.ip_queue.put_nowait(("done",""))
                for wi in workers:
                    wi.cancel()

        self.t1 = time.time()


class Whoiscrawler(ncrawler):
    def __init__(self,targets,max_tries=3,max_tasks = 1,loop=None,task_queue=None,redis_db = None,redis_set=None,page_timeout = 20,timeout_index = 1):
        super(Whoiscrawler,self).__init__(max_tries= max_tries,max_tasks=max_tasks,loop=loop,task_queue=task_queue,redis_db = redis_db,redis_set=redis_set,page_timeout = page_timeout,timeout_index = timeout_index)
        if targets:
            for target in targets:
                self.task_queue.put_nowait(target)
    
    async def fetch(self,target):
        if not await self.is_seen(target):
            print("have done")
            return
        tlde = tldextract.extract(target)
        sld = tlde.domain+"."+tlde.suffix
        exception = None
        try:
            tar_whois = whois.whois(target)
        except whois.parser.PywhoisError as e:
            tar_whois = {target:"unknown"}
        except:
            tar_whois = {target:"failed"}
        
        self.record_statistic(sld,WhoisStatistic(fqdn=target,
                                                sld=sld,
                                                whois=tar_whois,
                                                status=200,
                                                exception=exception))
        return

    async def work(self):
        try:
            while True:
                target = await self.task_queue.get()
                if target =="done":   # 结束信号
                    return 1
                await self.fetch(target)
                self.task_queue.task_done()
                await asyncio.sleep(5)

        except asyncio.CancelledError as e:
            print(e)
            return 

    async def crawl(self):
        self.t0 = time.time()
        workers = [asyncio.Task(self.work(),loop=self.loop) for _ in range(self.max_tasks)]
        for wi in workers:
            done = await wi
            if done:
                for wi in workers:
                    wi.cancel()

        self.t1 = time.time()

    


class URLcrawler(ncrawler):
    def __init__(self,urls,max_tries=4,max_tasks=10,loop=None, html_out=None,screen_out=None,
                        mul_layer=None,task_queue = None,dns_queue = None,redis_db = None,redis_set=None,
                        page_timeout = 20,timeout_index = 1):
        super(URLcrawler,self).__init__(max_tries= max_tries,max_tasks=max_tasks,loop=loop,task_queue=task_queue,redis_db = redis_db,redis_set = redis_set,page_timeout = page_timeout,timeout_index = timeout_index)

        # self.loop = loop or asyncio.get_event_loop()
        self.urls = urls
        # self.max_tries = max_tries
        # self.max_tasks = max_tasks
        # self.task_queue = task_queue or Queue(loop=self.loop)
        self.seen_urls = set()  # 从redis中取已经处理过的
        # self.done = []
        self.html_out = html_out
        self.screen_out = screen_out
        self.dns_queue = dns_queue
        self.mul_layer =mul_layer
        self.page_timeout = page_timeout
        self.timeout_index = timeout_index

        # self.t0 = time.time()
        for ui in urls:
            self.task_queue.put_nowait((ui,None))

    # def record_statistic(self,url_statistic):
    #     # 将每个URL信息，保存到redis 或 mysql
    #     print(url_statistic)
    #     self.done.append(url_statistic)
    
    async def is_seen(self,url,link):
        # 判断URL是否已经抓取过，若抓取过，将当前URL设置为该URL的pre_url
        if await super().is_seen(link):
            return True
        else:
            resdb = redis.Redis(connection_pool=self.pool)
            res_id = hashlib.md5(link.encode()).hexdigest()
            sdata = json.loads(resdb.get(res_id))
            sdata["pre_url"].append(url)
            return False
    
    async def put_dns_task(self,links):
        for link in links:
            await self.dns_queue.put(urllib.parse.urlparse(link).netloc)



    
    def parse_url(self,response):
        dom = etree.HTML(response)
        if dom:
            a_link = dom.xpath('//*/a/@href')
            style_link = dom.xpath('//*/link/@href')
            js_link = dom.xpath('//*/script/@src')
            image_link = dom.xpath('//*/img/@src')
            return a_link,style_link,js_link,image_link
        else:
            return [],[],[],[]

    def link_ver(self,url,links):
        res_links = []
        for link in set(links):
            normalized = urllib.parse.urljoin(url, link)
            defragmented, frag = urllib.parse.urldefrag(normalized)
            res_links.append(defragmented)
        return res_links

    async def fetch(self,url,pre_url):
        print(url)
        if not await self.is_seen("",url):
            print("have done")
            return
        tries = 0
        title = None
        url_parse = urllib.parse.urlparse(url)
        url_hash = hashlib.md5(url.encode()).hexdigest()
        filename = url_parse.netloc +"_"+ url_hash
        tlde = tldextract.extract(url_parse.netloc)
        sld = tlde.domain+"."+tlde.suffix
        html_out_path = os.path.join(self.html_out,filename+".html") if self.html_out else None
        screen_out_path = os.path.join(self.screen_out,filename+".png") if self.screen_out else None
        exception = None
        redirect = None
        my_timeout = self.page_timeout
        while tries < self.max_tries:
            try:
                # ,'--proxy-server=127.0.0.1:1080'
                browser = await launch(headless=True, args=['--disable-infobars','--proxy-server=127.0.0.1:1080'])
                page = await browser.newPage()
                await page.evaluateOnNewDocument('function(){Object.defineProperty(navigator, "webdriver", {get: () => undefined})}')
                await page.setViewport(viewport={'width': 1920, 'height': 1080})
                try:
                    await page.goto(url,timeout=my_timeout)
                    title = await page.title()
                except pyppeteer.errors.NetworkError as ne:
                    if ne.__str__()=="Execution context was destroyed, most likely because of a navigation.":
                        await page.goto(page.url,timeout=self.page_timeout * self.timeout_index)
                    
                    title = await page.title()
                if url != page.url:
                    redirect = page.url
                content = await page.content()
                a_link,style_link,js_link,image_link = self.parse_url(content)
                
                a_link = self.link_ver(url,a_link)
                style_link = self.link_ver(url,style_link)
                js_link = self.link_ver(url,js_link)
                image_link = self.link_ver(url,image_link)
                links = a_link

                if self.html_out:
                    async with aiofiles.open(html_out_path,"w",encoding="utf-8") as fp:
                        await fp.write(content)
                    clogger.info('%r collected to %r'%(url,self.html_out))
                    # 输出到文件
                if self.screen_out:
                    await page.waitFor(3)
                    await page.screenshot({"path":screen_out_path})
                await browser.close()
                
                self.record_statistic(url,URLStatistic(url=url,
                                               fqdn = url_parse.netloc,
                                               sld =sld,
                                               redirect=redirect,
                                               pre_url = [pre_url],
                                               next_url=[a_link,style_link,js_link,image_link],
                                               url_nums = len(links),
                                               title = title,
                                               html = html_out_path,
                                                screenshot=screen_out_path,
                                               status = 200,
                                               exception=exception))
                if self.mul_layer:
                    for link in links:
                        if await self.is_seen(url,link):
                            self.task_queue.put_nowait((link,url))
                        # self.dns_queue.put_nowait(urllib.parse.urlparse(link).netloc)
                self.seen_urls.update(links)
                # await self.put_dns_task(links)
                return 

            except Exception as e:
                await browser.close()
                my_timeout = my_timeout *self.timeout_index
                clogger.info('try %r for %r raised %r'%(tries,url,e))
                print('try %r for %r raised %r'%(tries,url,e))
                exception = e
                tries += 1
                if not url.startswith("https://www."):
                    url = url.replace("https://","https://www.")
                continue
            
        else:
            print("________________________")
            await browser.close()
            clogger.error('%r failed after %r tries',url,self.max_tries)
            self.record_statistic(url,URLStatistic(url=url,
                                               fqdn = url_parse.netloc,
                                               sld = sld,
                                               redirect=redirect,
                                               pre_url = [pre_url],
                                               next_url=[[],[],[],[]],
                                               url_nums = 0,
                                               title = title,
                                               html = html_out_path,
                                               screenshot=screen_out_path,
                                               status = 400,
                                               exception=exception))
            return

    async def work(self):
        try:
            while True:
                url,pre_url = await self.task_queue.get()
                await self.fetch(url,pre_url)
                self.task_queue.task_done()
        except asyncio.CancelledError as e:
            print(e)
    
    async def crawl(self):
        workers = [asyncio.Task(self.work(),loop=self.loop) for _ in range(self.max_tasks)]
        self.t0 = time.time()
        await self.task_queue.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()
