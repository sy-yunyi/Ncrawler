import asyncio
import pyppeteer
from pyppeteer import launch
import threading
from asyncio.windows_events import ProactorEventLoop
async def main():
    start_parm = {
        # 启动chrome的路径
        # "executablePath": r"E:\tmp\chrome-win\chrome.exe",
        # 关闭无头浏览器
        # "headless": False,
        "args": [
            # '--disable-infobars',  # 关闭自动化提示框
            # '--no-sandbox',  # 关闭沙盒模式
            '--start-maximized',  # 窗口最大化模式
        ],
    }
    browser = await launch(**start_parm)

    # browser = await launch(headless=True)
    page = await browser.newPage()
    await page.evaluateOnNewDocument('function(){Object.defineProperty(navigator, "webdriver", {get: () => undefined})}')
    # dimensions = await page.evaluate(pageFunction='''() => {
    #         return {
    #             width: document.documentElement.clientWidth,  // 页面宽度
    #             height: document.documentElement.clientHeight,  // 页面高度
    #             deviceScaleFactor: window.devicePixelRatio,  // 像素比 1.0000000149011612
    #         }
    #     }''', force_expr=False)  # force_expr=False  执行的是函数
    await page.setViewport(viewport={'width': 1920, 'height': 1080})
    try:
        await page.goto('https://www.baidu.com')
        title = await page.title()
    except pyppeteer.errors.TimeoutError as te:
        print(te)
    except pyppeteer.errors.NetworkError as ne:
        print(ne)
        if ne.__str__() == "Execution context was destroyed, most likely because of a navigation.":
            await page.goto(page.url)
        title = await page.title()
    except Exception as e:
        print(e)
    
    content = await page.content()
    
    await page.screenshot({'path': 'example1.png'})
    await browser.close()
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
# asyncio.run(main())

async def producter(q):
    task = 1
    nums = 4
    while task <10 :
        # await asyncio.sleep(1)
        await q.put(task)
        print(f'produce task {task}')
        task += 1
    await asyncio.sleep(2)
    print(3)
    await asyncio.sleep(2)
    print(4)
    await q.join()
    return 

async def consumer(q):
    while True:
        await asyncio.sleep(1)
        task = await q.get()
        
        print(f"finished {task}")
        q.task_done()
        # if task ==3:
        #     return 

async def work(q,loop):
    # loop = asyncio.get_event_loop()
    # pro = asyncio.create_task(producter(q))
    # con = asyncio.create_task(consumer(q))
    pro = asyncio.Task(producter(q),loop=loop)
    con = asyncio.Task(consumer(q),loop=loop)
    await pro
    await con


if __name__=="__main__":
    
    loop = ProactorEventLoop()
    asyncio.set_event_loop(loop)
    q = asyncio.Queue(maxsize=3)
    workers = [asyncio.Task(producter(q),loop=loop),asyncio.Task(consumer(q),loop=loop)]
    loop.run_until_complete(asyncio.gather(*workers))

    




