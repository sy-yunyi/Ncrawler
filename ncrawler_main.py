import asyncio
from crawler import URLcrawler,DNSRecordcrawler,Whoiscrawler,IPcrawler
import logging
import platform
import sys
import pandas as pd
from asyncio import Queue

def fix_url(url):
    """Prefix a schema-less URL with http://."""
    if '://' not in url:
        url = 'http://' + url
    return url

def get_data():
    file_path = r"D:\nuts\silex\bn-zone.csv"
    with open(file_path,"r") as fp:
        lines = fp.readlines()
        lines = [line.strip().split(",")[2] for line in lines]

    return ["https://"+line for line in lines],lines
    


def main():
    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[min(2, len(levels)-1)])
    # args.level  控制日志的记录程度

    if platform.system()=="Windows":
        from asyncio.windows_events import ProactorEventLoop
        loop = ProactorEventLoop()
        asyncio.set_event_loop(loop)
    else:
        loop = asyncio.get_event_loop()
    urls,fqdns = get_data()
    df = pd.read_csv(r"D:\nuts\silex\porn.csv")
    urls = df["host"].tolist()
    urls = ["http://"+url for url in urls]
    # urls = ["http://constance.com.bn/","https://zhuanlan.zhihu.com/p/352405533"]
    fqdns = df["host"].tolist()
    fqdns.append("done")
    targets = fqdns
    # ip_targets = [["220.181.38.148","www.baidu.com"]]
    ip_targets = zip(df["ip"].tolist(),df["host"].tolist())
    whois_target = fqdns
    html_out = r"D:\workspace\data\pron"
    screen_out = r"D:\workspace\data\pron"
    dns_queue = Queue(loop=loop)
    whois_queue = Queue(loop=loop)
    ip_queue = Queue(loop=loop)
    # redis_db 将不同数据放入不同的db里面
    url_redis,dns_redis,whois_redis,ip_redis = 3,4,5,6
    urlcrawler = URLcrawler(urls,max_tasks=3,mul_layer=True,html_out=html_out,screen_out=screen_out,dns_queue=dns_queue,redis_db=url_redis,redis_set="url_set",page_timeout = 15000,timeout_index = 2)
    dnscrawler = DNSRecordcrawler(targets=targets,max_tasks=2,task_queue=dns_queue,ip_queue=ip_queue,redis_db = dns_redis,redis_set="dns_set")
    whoiscrawler = Whoiscrawler(targets=whois_target,max_tasks=2,task_queue=whois_queue,redis_db = whois_redis,redis_set="whois_set")
    ipcrawler = IPcrawler(targets=ip_targets,max_tasks=2,task_queue=ip_queue,redis_db = ip_redis,redis_set="ip_set")
# ,whoiscrawler.crawl(),ipcrawler.crawl(),dnscrawler.crawl()
    try:
        # urlcrawler.crawl(),whoiscrawler.crawl(),ipcrawler.crawl(),dnscrawler.crawl()
        workers = [urlcrawler.crawl()]
        loop.run_until_complete(asyncio.gather(*workers))
    except KeyboardInterrupt:
        sys.stderr.flush()
        print("\nInterrupted\n")
    
    finally:
        loop.stop()
        loop.run_forever()
        loop.close()
    
if __name__=="__main__":
    main()




