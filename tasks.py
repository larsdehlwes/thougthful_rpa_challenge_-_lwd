import os

try:
    # Check for custom library
    from formatted_logging import get_and_configure_logger
    # Create logger with adequate formatting
    loglevel = os.getenv("ROBOCORP_LOGLEVEL", "20")
    logger = get_and_configure_logger(__name__, int(loglevel))
except ImportError:
    # If custom library is not found, use default logging
    import logging
    logger = logging.getLogger()

from robocorp import workitems
from robocorp.tasks import task
from RPA.Excel.Files import Files as Excel

from typing import Literal
from datetime import datetime, timedelta, date
import re
from bs4 import BeautifulSoup

# import uniform from random
from random import uniform, randint

from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlretrieve
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import asyncio
from aiofiles import open as aio_open
from playwright.async_api import async_playwright, Page

@task
def thoughtful_automation_challenge():
    """
    Main task that solves the Thoughtful Automation Challenge
    """
    # Get the current work item and log the payload if in debug mode
    item = workitems.inputs.current
    logger.debug("Received payload:", item.payload)

    ### BEGIN INPUT PROCESSING ###
    logger.info("Starting input processing...")

    today = datetime.today()
    query = item.payload.get("query", "Brazil")
    months = item.payload.get("months", 1)
    category = item.payload.get("category", "Business")

    if months < 1:
        months_offset = 0
    else:
        months_offset = months - 1

    # Calculate the cutoff date based on the current date and the months offset
    cutoff_month, cutoff_year = ((today.month - months_offset) %
                                 12, today.year + (today.month - months_offset) // 12)
    if cutoff_month == 0:
        cutoff_month = 12
        cutoff_year -= 1
    cutoff_date = date(year=cutoff_year, month=cutoff_month, day=1)

    logger.info("Input processing finished!")
    ### END INPUT PROCESSING ###

    output_payload = asyncio.run(async_process(today, query, category, cutoff_date, months))
    workitems.outputs.create(payload=output_payload)


async def async_process(today, query, category, cutoff_date: date, months: int) -> dict:
    output_payload = {}

    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=False)
        try:
            ### BEGIN PAGE LOAD, SEARCH AND SORTING ###
            logger.info("Starting page load, search and sorting...")
            page = await browser.new_page()
            await open_page(page)
            #reject_cookies_popup_if_available(page)
            random_wait = randint(300, 500)
            await page.wait_for_timeout(random_wait)

            cookie_task = asyncio.create_task(reject_cookies_popup_if_available(page))
            search_task = asyncio.create_task(search(page, query))

            # Wait for the first button to appear and be clicked
            done, pending = await asyncio.wait(
                [cookie_task, search_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            random_wait = randint(300, 500)
            await page.wait_for_timeout(random_wait)
            await select_category(page, category)
            random_wait = randint(300, 500)
            await page.wait_for_timeout(random_wait)
            await sortby(page, 'Newest')
            
            logger.info("Page load, search and sorting finished!")
            ### END PAGE LOAD, SEARCH AND SORTING ###
    
            page.on("response", response_handler)
    
            ### BEGIN SEARCH RESULT EXTRACTION ###
            logger.info("Starting search result extraction...")
            # Keep track of the current page number and whether the limit has been reached
            pagenum = 0
            limit_reached = False
            # Register results
            list_of_url_filename_pairs = []
            
            imgs_basenames_done = set()
            imgs_basenames = set()
            posts = []
            
            async with asyncio.TaskGroup() as task_group:
    
                while True:
                    pagenum += 1
        
                    # Parse the search results using BeautifulSoup
                    await page.wait_for_load_state()
                    height_to_be_scrolled = await page.evaluate('document.body.scrollHeight')
                    window_inner_height = await page.evaluate('window.innerHeight')
                    mouse = page.mouse
    
                    scroll_dir = 1
                    while True:
                        soup = await parse_search_results(page)
                        limit_reached = await fetch_all_new_image_links(page, soup, posts, imgs_basenames, imgs_basenames_done, cutoff_date, task_group)
                        print(f"diff: {imgs_basenames.difference(imgs_basenames_done)}")
                        if imgs_basenames == imgs_basenames_done:
                            break
                        random_scroll = randint(1, window_inner_height)
                        random_wait = randint(500, 2000)
                        await mouse.wheel(0, scroll_dir*random_scroll)
                        await page.wait_for_timeout(random_wait)
                        scroll_y = await page.evaluate('window.scrollY')
                        if scroll_y + window_inner_height >= height_to_be_scrolled or scroll_y == 0:
                            scroll_dir *= -1

                    if limit_reached:
                        break
                    # Try to click the "Next stories" button, if it does not exist, break the loop
                    try:
                        await page.click("//button[contains(@aria-label, 'Next stories')]")
                    except:
                        break
                logger.info("Search result extraction finished!")
                ### END SEARCH RESULT EXTRACTION ###

            ### BEGIN DATA PROCESSING ###
            logger.info("Starting data processing...")
            words = query.split()
            # quick and easy, regular expression approach matching full words
            # more appropriate for this task would be a proper tokenization and stemming approach using spacy or nltk

            # Drop unnecessary keys and remove duplicates and sort by date in descending order
            set_of_keys_to_drop = {'img_resized_link', 'img_basename', 'img_link'}
            posts_crop = [{k: v for k, v in post.items() if k not in set_of_keys_to_drop} for post in posts]
            posts_unique = [dict(t) for t in {tuple(d.items()) for d in posts_crop}]
            sorted_posts = sorted(posts_unique, key=lambda x: x['date'], reverse=True)
            for row in sorted_posts:
                # Calculate the count of the words of the query occuring in the title and image description
                count = 0
                test_string = row['title'] + ' ' + row['description']
                for word in words:
                    occurences = re.findall(
                        rf"\b{word}\b", row['title'], re.IGNORECASE)
                    count += len(occurences)
                row['count'] = count
                # Check whether a price is mentioned in the title
                row['price'] = validate_price(test_string)
            logger.info("Data processing finished!")
            ### END DATA PROCESSING ###

            ### BEGIN OUTPUT WRITING ###
            logger.info("Starting output writing...")
            # Create a new blank workbook with a worksheet "Results"
            keep_chars = re.compile('[^a-zA-Z0-9 ]')
            query_simple = "-".join([keep_chars.sub('', word) for word in words])
            write_rows_to_excel(
                sorted_posts, filepath=f"output/reuters_query-{query_simple}_cat-{category}_months-{months}.xlsx")
            output_payload['filename'] = (
                f"reuters_query-{query_simple}_",
                f"cat-{category}_months-{months}.xlsx"
            )
            output_payload['results_found'] = len(sorted_posts)
            logger.info("Output writing finished!")
            ### END OUTPUT WRITING ###

        except Exception as e:
            logger.error(f"An error occurred: {e}")
            output_payload['error'] = str(e)
            raise
    
        finally:
            await browser.close()
            return output_payload


async def open_page(page: Page) -> None:
    """ Open the Reuters website and wait for it to load """
    await page.goto("https://www.reuters.com", timeout=60000)


async def reject_cookies_popup_if_available(page: Page) -> None:
    """ Reject the cookies popup if it is displayed """
    try:
        await page.click("button#onetrust-reject-all-handler", timeout=30000)
    except Exception:
        logger.info("Could not find the cookies popup.")
        pass


async def search(page: Page, query: str) -> None:
    """ Perform a search on the Reuters website """
    await page.click("//button[@aria-label='Open search bar']")
    random_wait = randint(300, 500)
    await page.wait_for_timeout(random_wait)
    await page.get_by_test_id('FormField:input').fill(query)
    random_wait = randint(300, 500)
    await page.wait_for_timeout(random_wait)
    await page.click("//button[@aria-label='Search']")


async def select_category(page: Page, category: str) -> None:
    """ Select a category on the Reuters website """
    await page.click("button#sectionfilter")
    random_wait = randint(300, 500)
    await page.wait_for_timeout(random_wait)
    await page.click(f"//li[@data-key='{category}']")


async def sortby(page: Page, sortby: Literal['Newest', 'Oldest', 'Relevance'] = 'Newest') -> None:
    """ Sort the search results on the Reuters website (Newest, Oldest, Relevance) """
    await page.click("button#sortby")
    random_wait = randint(300, 500)
    await page.wait_for_timeout(random_wait)
    await page.click(f"//li[@data-key='{sortby}']")


def validate_price(test_string: str) -> bool:
    """
    Validates whether a price is mentioned in the given string.

    Args:
        test_string, str: The string to be tested for a price.

    Returns:
        bool: True if a price is found, False otherwise.
    """
    # The pattern to isolate the potential price candidates. Match sequences of digits, commas and periods, making sure the string does not end with a comma or period.
    pattern_isolate = r'\$[\d,\.]+(?<![,\.])|\b[\d,\.]+(?<![,\.]) (dollars|USD)\b$'
    # The pattern to validate the isolated candidates. Make sure the string either starts with a dollar sign or ends with "dollars" or "USD". Enforce that the number starts with a non-zero digit and that commas are used as separators for thousands.
    pattern_validate = r'^\$[1-9]\d{0,2}(,\d{3})*(\.\d+)?$|\b[1-9]\d{0,2}(,\d{3})*(\.\d+)? (dollars|USD)\b$'
    candidates = re.finditer(pattern_isolate, test_string)
    for candidate in candidates:
        if re.match(pattern_validate, candidate.group()):
            return True
    return False


async def parse_search_results(page: Page) -> BeautifulSoup:
    """ Parse the search results using BeautifulSoup """
    ul_locator = page.locator("//ul[contains(@class, 'search-results')]")
    await ul_locator.wait_for()
    html = await ul_locator.evaluate("(e) => e.outerHTML")
    soup = BeautifulSoup(html, "html.parser")
    return soup


def lazy_loaded_images_on_page(soup: BeautifulSoup) -> bool:
    """ Check if all jpg images on the page have been loaded """
    for li in soup.find_all('li'):
        img = li.find('img')
        img_src = img.get('src')
        filetype = img_src.split('.')[-1]
        if filetype == 'jpg' and img.get('srcset') is None:
            return False
    return True


async def fetch_all_new_image_links(page: Page, soup: BeautifulSoup, posts: list, basenames: set, done_basenames: set, cutoff_date: date, task_group) -> bool:
    for li in soup.find_all('li'):
        row = extract_information_from_list_item(li)
        
        # Check if the date is before the cutoff date, if so, stop the loop
        if row.get('date') < cutoff_date:
            return True
        
        posts.append(row)
        basenames.add(row.get('img_basename'))
        if row.get('img_basename') not in done_basenames and row.get("img_resized_link") is not None:
            done_basenames.add(row.get('img_basename'))
            task_group.create_task(fetch_resized_image(page, row.get("img_resized_link")))

    return False


def extract_information_from_list_item(li: BeautifulSoup) -> dict:
    """ Extract information from a list item in the search results """
    title = li.find('span', attrs={'data-testid': 'Heading'}).text
    datetime_string = li.find('time').get('datetime')

    # Parse the datetime string into a date object
    try:
        post_datetime = datetime.strptime(
            datetime_string, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        post_datetime = datetime.strptime(
            datetime_string, '%Y-%m-%dT%H:%M:%SZ')
    post_date = post_datetime.date()

    image = li.find('img')
    image_link = image.get('src')
    img_filename = image_link.split('/')[-1]
    img_basename = img_filename.split('.')[0]

    description = image.get('alt', '')

    try:
        image_srcset_link = li.find('img').get('srcset').split(',')[-1].split(' ')[0]
        url_match = re.match(r"https?://www\.reuters\.com/resizer/v\d/(\S+)\.(jpg|png|gif)\?auth=([a-f0-9]+)&width=(\d+)&quality=(\d+)", image_srcset_link)
        basename, ext, auth, width, quality = url_match.groups()
        resized_link = f"https://www.reuters.com/resizer/v2/{basename}.{ext}?auth={auth}&width=480&quality={quality}"
    except:
        resized_link = None

    row = {
        'date': post_date,
        'title': title,
        'description': description,
        "img_link": image_link,
        "img_basename": img_basename,
        "img_filename": img_filename,
        "img_resized_link": resized_link
    }
    return row

async def fetch_resized_image(page: Page, resized_link) -> None:
    await page.evaluate('async() => { return await fetch("' + resized_link + '").then(response => { if (!response.ok) { throw new Error(response.status); } else { return response.text(); } })}')

async def start_fetching_resized_img_urls(page: Page, urls):
    return await asyncio.gather(
        *(fetch_resized_image(page, url) for url in urls)
    )

def write_rows_to_excel(rows: dict, filepath: str = "output/reuters_results.xlsx") -> None:
    """ Write the extracted rows to an Excel file """
    excel = Excel()
    excel.create_workbook(filepath)
    excel.create_worksheet(name="Results", content=rows, header=True)
    excel.save_workbook()

async def response_handler(*args, **kw):
    try:
        response = args[0]
        url = response.url
        #if re.findall(r'https://reuters.com/resizer/\S+\.(png|jpg)\?\S+', url, re.IGNORECASE):
        url_match = re.match(r"https?://www\.reuters\.com/resizer/v\d/(\S+)\.(jpg|png|gif)\?auth=([a-f0-9]+)&width=(\d+)&quality=(\d+)", url)
        basename, ext, auth, width, quality = url_match.groups()
        if url_match and int(width) >= 400:
            print(response.request)
            groups = url_match.groups()
            logger.debug("response_handler:%s", url)
            logger.debug("groups:%s", groups)
            img_name = f"output/{groups[0]}.{groups[1]}"
            # use aiofiles instead
            async with aio_open(img_name, 'wb') as f:
                await f.write(await response.body())
    except Exception:
        pass
