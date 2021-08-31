import asyncio
import pyppeteer
from pyppeteer import launch
import threading
from asyncio.windows_events import ProactorEventLoop
from multiprocessing import Process,Manager
import concurrent.futures

async def producter(q):
    task = 1
    nums = 4
    while task:
        # await asyncio.sleep(4)
        await q.put(task)
        print(f'produce task {task}')
        task += 1
    await q.join()
    return 

async def consumer(q):
    while True:
        # await asyncio.sleep(4)
        task = await q.get()
        
        print(f"finished {task}")
        q.task_done()
        # if task ==3:
        #     return 

async def work(q,loop):
    loop = asyncio.get_event_loop()
    # pro = asyncio.create_task(producter(q))
    # con = asyncio.create_task(consumer(q))
    pro = asyncio.Task(producter(q),loop=loop)
    con = asyncio.Task(consumer(q),loop=loop)
    await pro
    await con


async def toworkp(q,loop):
    asyncio.set_event_loop(loop)
    pro = asyncio.Task(producter(q),loop=loop)
    # pro = asyncio.create_task(producter(q))
    # con = asyncio.Task(consumer(q),loop=loop)
    await pro
    # con.cancel()
    # await con
    # await q.join()
async def toworkc(q,loop):
    asyncio.set_event_loop(loop)
    con = asyncio.Task(consumer(q),loop=loop)
    await con

def pro_work(q,loop):
    loop.run_until_complete(toworkp(q,loop))

def con_work(q,loop):
    # asyncio.set_event_loop(loop)
    # loop.run_forever()
    loop.run_until_complete(toworkc(q,loop))

def gather_work(q,loop):
    loop.run_until_complete(work(q,loop))

def finish():
    print("finished")


def get_task():
    loop = asyncio.get_event_loop()
    # with concurrent.futures.ThreadPoolExecutor() as pool:
    #     await loop.run_in_executor(pool,producter,q)


if __name__=="__main__":
    
    loop = ProactorEventLoop()
    asyncio.set_event_loop(loop)
    
    # asyncio.run(work(q,loop))
    # loop.run_until_complete(work(q,loop))
    # thread_loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(thread_loop)
    q = asyncio.Queue(maxsize=3)
    t1 = threading.Thread(target=gather_work, args=(q,loop,))
    t1.start()
    # t1.join()
    print("hello")
    
    
    # t2 = threading.Thread(target=con_work, args=(q,thread_loop,))
    # t2.start()
    # loop.call_soon_threadsafe(finish)
    # loop.run_until_complete(towork(q,loop))
    # loop.run_until_complete(toworkp(q,loop))
    # t1.join()
    # t2.join()
    # asyncio.run_coroutine_threadsafe
    
    




