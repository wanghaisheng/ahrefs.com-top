# https://static.ahrefs.com/static/assets/js/top/2/top2TopWebsitesList__PageApp_1720189995308-L6MU7BQX.js
# https://ahrefs.com/top/10


# https://static.ahrefs.com/static/assets/js/top/albania/topalbaniaTopWebsitesListByCountry__PageApp_1720189995308-SUO45CRL.js


#!/usr/bin/env python
# MassRDAP - developed by acidvegas (https://git.acid.vegas/massrdap)

import asyncio
import logging
import json
import re
import os, random
from datetime import datetime

import pandas as pd
from DataRecorder import Recorder
import time
# try:
#     import aiofiles
# except ImportError:
#     raise ImportError('missing required aiofiles library (pip install aiofiles)')

try:
    import aiohttp
except ImportError:
    raise ImportError("missing required aiohttp library (pip install aiohttp)")
import aiohttp
import asyncio
from contextlib import asynccontextmanager
from dbhelper import DatabaseManager
# Usage
# Now you can use db_manager.add_screenshot(), db_manager.read_screenshot_by_url(), etc.
from loguru import logger

# Replace this with your actual test URL
test_url = "http://example.com"

# Replace this with your actual outfile object and method for adding data
# outfile = YourOutfileClass()
# Color codes
BLUE = "\033[1;34m"
CYAN = "\033[1;36m"
GREEN = "\033[1;32m"
GREY = "\033[1;90m"
PINK = "\033[1;95m"
PURPLE = "\033[0;35m"
RED = "\033[1;31m"
YELLOW = "\033[1;33m"
RESET = "\033[0m"

MAX_RETRIES = 3
INITIAL_DELAY = 1
MAX_DELAY = 10

# Setup basic logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Global variable to store RDAP servers
RDAP_SERVERS = {}


def get_title_from_html(html):
    title = "not content!"
    try:
        title_patten = r"<title>(\s*?.*?\s*?)</title>"
        result = re.findall(title_patten, html)
        if len(result) >= 1:
            title = result[0]
            title = title.strip()
    except:
        logger.error("cannot find title")
    return title


async def fetch_rdap_servers():
    """Fetches RDAP servers from IANA's RDAP Bootstrap file."""

    async with aiohttp.ClientSession() as session:
        async with session.get("https://data.iana.org/rdap/dns.json") as response:
            data = await response.json()
            for entry in data["services"]:
                tlds = entry[0]
                rdap_url = entry[1][0]
                for tld in tlds:
                    RDAP_SERVERS[tld] = rdap_url


async def get_proxy():
    proxy=None
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get('http://demo.spiderpy.cn/get') as response:
                data = await response.json()
                proxy=data['proxy']
                return proxy
        except:
            pass
async def get_proxy_proxypool():
    async with aiohttp.ClientSession() as session:

        if proxy is None:
            try:
                async with session.get('https://proxypool.scrape.center/random') as response:
                    proxy = await response.text()
                    return proxy
            except:
                return None

def get_tld(domain: str):
    """Extracts the top-level domain from a domain name."""
    parts = domain.split(".")
    return ".".join(parts[1:]) if len(parts) > 1 else parts[0]


async def submit_radar_with_retry(
    domain: str,
    valid_proxies: list,
    proxy_url: str,
    semaphore: asyncio.Semaphore,
    outfile: Recorder,
):
    retry_count = 0
    while retry_count < MAX_RETRIES:
        if retry_count>0:
            pro_str=None
            proxy_url=None
            if valid_proxies:
                proxy_url=random.choice(valid_proxies)
            else:
                try:
                    pro_str=await get_proxy()

                    if pro_str is None:
                    
                        pro_str=await get_proxy_proxypool()


                except Exception as e:
                    logger.error('get proxy error:{} use backup',e)
            if pro_str:
                proxy_url = "http://{}".format(pro_str)            
    
        logger.info("current proxy{}", proxy_url)

        try:
            async with semaphore:
                result = await asyncio.wait_for(
                    submit_radar(domain, proxy_url, semaphore, outfile), timeout=30
                )
            if result:
                if proxy_url and proxy_url not in valid_proxies:
                    valid_proxies.append(proxy_url)
                return result
        except asyncio.TimeoutError:
            logger.error(
                f"Timeout occurred for domain: {domain} with proxy: {proxy_url}"
            )
        except Exception as e:
            logger.error(f"Error occurred: {e}")
        retry_count += 1
        # if retry_count < MAX_RETRIES:
        #     delay = min(INITIAL_DELAY * (2 ** retry_count), MAX_DELAY)
        #     logger.info(f"Retrying in {delay} seconds with proxy {proxy_url}...")
        #     await asyncio.sleep(delay)

    logger.error(f"Max retries reached for domain: {domain}")
    return None

import uuid

def is_valid_uuid(uuid_to_test, version=4):
    try:
        # This will check if the UUID is valid and raise a ValueError if not
        val = uuid.UUID(uuid_to_test, version=version)
        return str(val) == uuid_to_test
    except ValueError:
        # The UUID is not valid
        return False



async def get_top(browser,
    domain: str, proxy_url: str, outfile: Recorder
):
    """
    Looks up a domain using the RDAP protocol.

    :param domain: The domain to look up.
    :param proxy_url: The proxy URL to use for the request.
    :param semaphore: The semaphore to use for concurrency limiting.
    """

    tab=browser.driver.new_tab()

    page = browser.driver.get_tab(tab)



    # query_url='https://ahrefs.com/top'
    query_url=domain
    

    logger.info("use proxy_url:{}", proxy_url)

    logger.info("querying:{}", query_url)
    browser=None
    try:
        headless
    except:
        headless=True
        
    try:


        page.get(query_url)    


        page.wait.load_start()

        trs=page.ele('.css-e8l0hj-table css-sb72kg-tableWithStickyHeader css-1ce0at0').ele('tag:tbody').children()
        for tr in trs:
            text=[]
            for i in tr.children():
                text.append(i.text)
            print(text)
            text.append(query_url)
            # browser.saveCookie(cookiepath)

            # data =','.join(text)
            outfile.add_data(text)


            logger.info(
                f"{GREEN}SUCCESS {GREY}| {BLUE} {GREY}| {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain}{GREEN}"
            )


    except asyncio.TimeoutError as e:
        logger.info(
            f"{RED} TimeoutError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
        )
        raise

    except aiohttp.ClientError as e:
        logger.info(
            f"{RED} ClientError {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
        )
        raise

    except Exception as e:
        # page.quit()
        # 需要注意的是，程序结束时浏览器不会自动关闭，下次运行会继续接管该浏览器。

# 无头浏览器因为看不见很容易被忽视。可在程序结尾用page.quit()将其关闭 不 quit 还是会无头模式
        # headless=False
        print('start a new browser to get fresh cookie')

        logger.info(
            f"{RED}Exception  {GREY}| --- | {PURPLE}{query_url.ljust(50)} {GREY}| {CYAN}{domain} {RED}| {e}{RESET}"
        )
        raise

    finally:
        page.close()
        print('finally')

@asynccontextmanager
async def aiohttp_session(url):
    async with aiohttp.ClientSession() as session:
        yield session


async def test_proxy(test_url, proxy_url):
    try:
        async with aiohttp_session(test_url) as session:
            # Determine the type of proxy and prepare the appropriate proxy header

            # Make the request
            async with session.get(test_url, timeout=10, proxy=proxy_url) as response:
                if uuid == 200:

                    # outfile.add_data(proxy_url)  # Uncomment and replace with your actual implementation
                    return True
                else:

                    return False
    except asyncio.TimeoutError:
        # print(f"{Style.BRIGHT}{Color.red}Invalid Proxy (Timeout) | {proxy_url}{Style.RESET_ALL}")
        return False
    except aiohttp.ClientError:

        return False


# To run the async function, you would do the following in your main code or script:
# asyncio.run(test_proxy('your_proxy_url_here'))
def cleandomain(domain):
    domain=domain.strip()
    if "https://" in domain:
        domain = domain.replace("https://", "")
    if "http://" in domain:
        domain = domain.replace("http://", "")
    if "www." in domain:
        domain = domain.replace("www.", "")
    if domain.endswith("/"):
        domain = domain.rstrip("/")
    return domain

async def process_domains_screensht( outfile,counts):
        from DPhelper import DPHelper
        browser=DPHelper(browser_path=None,HEADLESS=False,proxy_server='socks5://127.0.0.1:1080')
        concurrency=5

        tasks = []
        domains =['https://ahrefs.com/top',
                  
                  'https://ahrefs.com/top/2',
                  'https://ahrefs.com/top/3',
                  'https://ahrefs.com/top/4',
                  'https://ahrefs.com/top/5',
                  'https://ahrefs.com/top/6',
                  'https://ahrefs.com/top/7',
                  'https://ahrefs.com/top/8',
                  'https://ahrefs.com/top/9',
                  'https://ahrefs.com/top/10'
                  , 'https://ahrefs.com/top/afghanistan', 'https://ahrefs.com/top/albania', 'https://ahrefs.com/top/algeria', 'https://ahrefs.com/top/american-samoa', 'https://ahrefs.com/top/andorra', 'https://ahrefs.com/top/angola', 'https://ahrefs.com/top/anguilla', 'https://ahrefs.com/top/antigua-and-barbuda', 'https://ahrefs.com/top/argentina', 'https://ahrefs.com/top/armenia', 'https://ahrefs.com/top/australia', 'https://ahrefs.com/top/austria', 'https://ahrefs.com/top/azerbaijan', 'https://ahrefs.com/top/bahamas', 'https://ahrefs.com/top/bahrain', 'https://ahrefs.com/top/bangladesh', 'https://ahrefs.com/top/belarus', 'https://ahrefs.com/top/belgium', 'https://ahrefs.com/top/belize', 'https://ahrefs.com/top/benin', 'https://ahrefs.com/top/bhutan', 'https://ahrefs.com/top/bolivia', 'https://ahrefs.com/top/bosnia-and-herzegovina', 'https://ahrefs.com/top/botswana', 'https://ahrefs.com/top/brazil', 'https://ahrefs.com/top/brunei-darussalam', 'https://ahrefs.com/top/bulgaria', 'https://ahrefs.com/top/burkina-faso', 'https://ahrefs.com/top/burundi', 'https://ahrefs.com/top/cambodia', 'https://ahrefs.com/top/cameroon', 'https://ahrefs.com/top/canada', 'https://ahrefs.com/top/cape-verde', 'https://ahrefs.com/top/central-african-republic', 'https://ahrefs.com/top/chad', 'https://ahrefs.com/top/chile', 'https://ahrefs.com/top/china', 'https://ahrefs.com/top/colombia', 'https://ahrefs.com/top/congo', 'https://ahrefs.com/top/congo-democratic-republic', 'https://ahrefs.com/top/cook-islands', 'https://ahrefs.com/top/costa-rica', 'https://ahrefs.com/top/cote-divoire', 'https://ahrefs.com/top/croatia', 'https://ahrefs.com/top/cuba', 'https://ahrefs.com/top/cyprus', 'https://ahrefs.com/top/czech-republic', 'https://ahrefs.com/top/denmark', 'https://ahrefs.com/top/djibouti', 'https://ahrefs.com/top/dominica', 'https://ahrefs.com/top/dominican-republic', 'https://ahrefs.com/top/ecuador', 'https://ahrefs.com/top/egypt', 'https://ahrefs.com/top/el-salvador', 'https://ahrefs.com/top/estonia', 'https://ahrefs.com/top/ethiopia', 'https://ahrefs.com/top/fiji', 'https://ahrefs.com/top/finland', 'https://ahrefs.com/top/france', 'https://ahrefs.com/top/gabon', 'https://ahrefs.com/top/gambia', 'https://ahrefs.com/top/georgia', 'https://ahrefs.com/top/germany', 'https://ahrefs.com/top/ghana', 'https://ahrefs.com/top/gibraltar', 'https://ahrefs.com/top/greece', 'https://ahrefs.com/top/greenland', 'https://ahrefs.com/top/guadeloupe', 'https://ahrefs.com/top/guatemala', 'https://ahrefs.com/top/guernsey', 'https://ahrefs.com/top/guyana', 'https://ahrefs.com/top/haiti', 'https://ahrefs.com/top/honduras', 'https://ahrefs.com/top/hong-kong', 'https://ahrefs.com/top/hungary', 'https://ahrefs.com/top/iceland', 'https://ahrefs.com/top/india', 'https://ahrefs.com/top/indonesia', 'https://ahrefs.com/top/iraq', 'https://ahrefs.com/top/ireland', 'https://ahrefs.com/top/isle-of-man', 'https://ahrefs.com/top/israel', 'https://ahrefs.com/top/italy', 'https://ahrefs.com/top/jamaica', 'https://ahrefs.com/top/japan', 'https://ahrefs.com/top/jersey', 'https://ahrefs.com/top/jordan', 'https://ahrefs.com/top/kazakhstan', 'https://ahrefs.com/top/kenya', 'https://ahrefs.com/top/kiribati', 'https://ahrefs.com/top/korea', 'https://ahrefs.com/top/kuwait', 'https://ahrefs.com/top/kyrgyzstan', 'https://ahrefs.com/top/lao-peoples-democratic-republic', 'https://ahrefs.com/top/latvia', 'https://ahrefs.com/top/lebanon', 'https://ahrefs.com/top/lesotho', 'https://ahrefs.com/top/libyan-arab-jamahiriya', 'https://ahrefs.com/top/liechtenstein', 'https://ahrefs.com/top/lithuania', 'https://ahrefs.com/top/luxembourg', 'https://ahrefs.com/top/macedonia', 'https://ahrefs.com/top/madagascar', 'https://ahrefs.com/top/malawi', 'https://ahrefs.com/top/malaysia', 'https://ahrefs.com/top/maldives', 'https://ahrefs.com/top/mali', 'https://ahrefs.com/top/malta', 'https://ahrefs.com/top/mauritius', 'https://ahrefs.com/top/mexico', 'https://ahrefs.com/top/micronesia', 'https://ahrefs.com/top/moldova', 'https://ahrefs.com/top/mongolia', 'https://ahrefs.com/top/montenegro', 'https://ahrefs.com/top/morocco', 'https://ahrefs.com/top/mozambique', 'https://ahrefs.com/top/myanmar', 'https://ahrefs.com/top/namibia', 'https://ahrefs.com/top/nauru', 'https://ahrefs.com/top/nepal', 'https://ahrefs.com/top/netherlands', 'https://ahrefs.com/top/new-zealand', 'https://ahrefs.com/top/nicaragua', 'https://ahrefs.com/top/niger', 'https://ahrefs.com/top/nigeria', 'https://ahrefs.com/top/norway', 'https://ahrefs.com/top/oman', 'https://ahrefs.com/top/pakistan', 'https://ahrefs.com/top/palestine', 'https://ahrefs.com/top/panama', 'https://ahrefs.com/top/papua-new-guinea', 'https://ahrefs.com/top/paraguay', 'https://ahrefs.com/top/peru', 'https://ahrefs.com/top/philippines', 'https://ahrefs.com/top/poland', 'https://ahrefs.com/top/portugal', 'https://ahrefs.com/top/puerto-rico', 'https://ahrefs.com/top/qatar', 'https://ahrefs.com/top/romania', 'https://ahrefs.com/top/russian-federation', 'https://ahrefs.com/top/rwanda', 'https://ahrefs.com/top/saint-vincent-and-grenadines', 'https://ahrefs.com/top/samoa', 'https://ahrefs.com/top/san-marino', 'https://ahrefs.com/top/sao-tome-and-principe', 'https://ahrefs.com/top/saudi-arabia', 'https://ahrefs.com/top/senegal', 'https://ahrefs.com/top/serbia', 'https://ahrefs.com/top/seychelles', 'https://ahrefs.com/top/sierra-leone', 'https://ahrefs.com/top/singapore', 'https://ahrefs.com/top/slovakia', 'https://ahrefs.com/top/slovenia', 'https://ahrefs.com/top/solomon-islands', 'https://ahrefs.com/top/somalia', 'https://ahrefs.com/top/south-africa', 'https://ahrefs.com/top/spain', 'https://ahrefs.com/top/sri-lanka', 'https://ahrefs.com/top/suriname', 'https://ahrefs.com/top/sweden', 'https://ahrefs.com/top/switzerland', 'https://ahrefs.com/top/taiwan', 'https://ahrefs.com/top/tajikistan', 'https://ahrefs.com/top/tanzania', 'https://ahrefs.com/top/thailand', 'https://ahrefs.com/top/timor-leste', 'https://ahrefs.com/top/togo', 'https://ahrefs.com/top/tonga', 'https://ahrefs.com/top/trinidad-and-tobago', 'https://ahrefs.com/top/tunisia', 'https://ahrefs.com/top/turkey', 'https://ahrefs.com/top/turkmenistan', 'https://ahrefs.com/top/uganda', 'https://ahrefs.com/top/ukraine', 'https://ahrefs.com/top/united-arab-emirates', 'https://ahrefs.com/top/united-kingdom', 'https://ahrefs.com/top/united-states', 'https://ahrefs.com/top/uruguay', 'https://ahrefs.com/top/uzbekistan', 'https://ahrefs.com/top/vanuatu', 'https://ahrefs.com/top/venezuela', 'https://ahrefs.com/top/vietnam', 'https://ahrefs.com/top/virgin-islands-british', 'https://ahrefs.com/top/virgin-islands-us', 'https://ahrefs.com/top/zambia', 'https://ahrefs.com/top/zimbabwe']



        for domain in domains:


                print(domain)

                proxy = None


                try:

                    task = asyncio.create_task(
                        get_top(browser,domain,  proxy, outfile)
                    )
                    # Ensure the semaphore is released even if the task fails
                    # task.add_done_callback(lambda t: semaphore.release())
                    # print('done', url)
                    tasks.append(task)
                    if len(tasks) >= concurrency:
                        # Wait for the current batch of tasks to complete
                        await asyncio.gather(*tasks)
                        tasks = []
                except Exception as e:
                    print(f"{RED}An error occurred while processing {domain}: {e}")
        await asyncio.gather(*tasks)
        # page.close()

counts=0
headless=True
cookiepath='cookie.txt'
start=datetime.now()
folder_path='.'
# logger.add(f"{folder_path}/domain-index-ai.log")
# print(domains)
outfilepath='ahref-top.csv'
columns=[

    "rank","rankchanges","domain","traffic","trafficchanges","url"

]
outfile = Recorder(folder_path+'/'+outfilepath, cache_size=50)
if os.path.exists(outfilepath)==False:
    outfile.set.head(columns)
donedomains=[]

semaphore = asyncio.Semaphore(5)
       
asyncio.run(process_domains_screensht(outfile,counts))
end=datetime.now()
print('costing',end-start)
outfile.record()
