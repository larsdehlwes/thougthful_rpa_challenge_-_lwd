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

from robocorp import browser
from robocorp import workitems
from robocorp.tasks import task
from RPA.Excel.Files import Files as Excel

from typing import Literal
from datetime import datetime, timedelta, date
from hashlib import shake_128
import re
from bs4 import BeautifulSoup

from concurrent.futures import ThreadPoolExecutor
from urllib.request import urlretrieve
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse


@task
def thoughtful_automation_challenge():
    """
    Main task that solves the Thoughtful Automation Challenge
    """
    # Get the current work item and log the payload if in debug mode
    item = workitems.inputs.current
    logger.debug("Received payload:", item.payload)

    # Configure the browser engine
    browser.configure(
        browser_engine="firefox",
        screenshot="only-on-failure",
        headless=True
    )

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

    output_payload = {}
    try:
        ### BEGIN PAGE LOAD, SEARCH AND SORTING ###
        logger.info("Starting page load, search and sorting...")
        page = open_page()
        reject_cookies_popup_if_available(page)
        search(page, query)
        select_category(page, category)
        sortby(page, 'Newest')
        logger.info("Page load, search and sorting finished!")
        ### END PAGE LOAD, SEARCH AND SORTING ###

        ### BEGIN SEARCH RESULT EXTRACTION ###
        logger.info("Starting search result extraction...")
        # Keep track of the current page number and whether the limit has been reached
        pagenum = 0
        limit_reached = False
        # Register results
        rows = []
        list_of_url_filename_pairs = []
        while True:
            pagenum += 1

            # Parse the search results using BeautifulSoup
            soup = parse_search_results(page)

            # Loop through the search results and extract the relevant information
            for li in soup.find_all('li'):
                page.wait_for_load_state()
                row = extract_information_from_list_item(li)

                post_date = row['date']
                title = row['title']
                description = row['description']
                image_link = row['img_link']
                img_filename = row['img_filename']

                logger.debug(f"{post_date} - {title} - {description}")

                # Check if the date is before the cutoff date, if so, stop the loop
                if post_date < cutoff_date:
                    limit_reached = True
                    break

                # Add the image link and filename to the list of pairs
                list_of_url_filename_pairs.append((image_link, img_filename))
                # Drop the image link from the row
                row.pop('img_link')
                # Add the row to the list of rows
                rows.append(row)

            # If the limit has been reached, break the loop
            if limit_reached:
                break
            # Try to click the "Next stories" button, if it does not exist, break the loop
            try:
                page.click("//button[contains(@aria-label, 'Next stories')]")
            except:
                break
        logger.info("Search result extraction finished!")
        ### END SEARCH RESULT EXTRACTION ###

        ### BEGIN IMAGE DOWNLOAD ###
        logger.info("Starting image download...")
        with ThreadPoolExecutor(max_workers=10) as executor:
            for url, filename in list_of_url_filename_pairs:
                executor.submit(download_image_to_disk, url, filename)
        logger.info("Images download finished!")
        ### END IMAGE DOWNLOAD ###

        ### BEGIN DATA PROCESSING ###
        logger.info("Starting data processing...")
        words = query.split()
        # quick and easy, regular expression approach matching full words
        # more appropriate for this task would be a proper tokenization and stemming approach using spacy or nltk
        for row in rows:
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
            rows, filepath=f"output/reuters_query-{query_simple}_cat-{category}_months-{months}.xlsx")
        output_payload['filename'] = (
            f"reuters_query-{query_simple}_",
            f"cat-{category}_months-{months}.xlsx"
        )
        output_payload['results_found'] = len(rows)
        logger.info("Output writing finished!")
        ### END OUTPUT WRITING ###

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        output_payload['error'] = str(e)
        raise

    finally:
        # A place for teardown and cleanups. (Playwright handles browser closing)
        logger.info("Automation finished!")
        workitems.outputs.create(payload=output_payload)


def open_page() -> browser.Page:
    """ Open the Reuters website and wait for it to load """
    page = browser.goto("https://www.reuters.com")
    page.wait_for_load_state()
    return page


def reject_cookies_popup_if_available(page: browser.Page) -> None:
    """ Reject the cookies popup if it is displayed """
    try:
        page.click("button#onetrust-reject-all-handler", timeout=30000)
    except Exception:
        logger.info("Could not find the cookies popup.")
        pass


def search(page: browser.Page, query: str) -> None:
    """ Perform a search on the Reuters website """
    page.click("//button[@aria-label='Open search bar']")
    page.get_by_test_id('FormField:input').fill(query)
    page.click("//button[@aria-label='Search']")


def select_category(page: browser.Page, category: str) -> None:
    """ Select a category on the Reuters website """
    page.click("button#sectionfilter")
    page.click(f"//li[@data-key='{category}']")


def sortby(page: browser.Page, sortby: Literal['Newest', 'Oldest', 'Relevance'] = 'Newest') -> None:
    """ Sort the search results on the Reuters website (Newest, Oldest, Relevance) """
    page.click("button#sortby")
    page.click(f"//li[@data-key='{sortby}']")


def download_image_to_disk(url: str, filename: str) -> None:
    """ Download an image from the given URL to the given filename """
    logger.debug(f"Downloading {url} to {filename}")
    urlretrieve(url, filename)


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


def parse_search_results(page: browser.Page) -> BeautifulSoup:
    """ Parse the search results using BeautifulSoup """
    locator = page.locator("//ul[contains(@class, 'search-results')]")
    locator.wait_for()
    html = locator.evaluate("(e) => e.outerHTML")
    soup = BeautifulSoup(html, "html.parser")
    return soup


def extract_information_from_list_item(li: BeautifulSoup) -> dict:
    """ Extract information from a list item in the search results """
    title = li.find('span', attrs={'data-testid': 'Heading'}).text
    datetime_string = li.find('time').get('datetime')
    # Observation: Label my not exist in all cases
    try:
        label = li.find('span', attrs={'data-testid': 'Label'}).text
    except:
        label = ''

    # Parse the datetime string into a date object
    try:
        post_datetime = datetime.strptime(
            datetime_string, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        post_datetime = datetime.strptime(
            datetime_string, '%Y-%m-%dT%H:%M:%SZ')
    post_date = post_datetime.date()

    hash_object = shake_128()
    hash_object.update(datetime_string.encode('utf-8'))
    hash_object.update(title.encode('utf-8'))

    hex_dig = hash_object.hexdigest(8)

    image_link = li.find('img').get('src')
    file_type = image_link.split('.')[-1]
    description = li.find('img').get('alt', '')
    img_filename = f"output/{hex_dig}.{file_type}"

    row = {
        'date': post_date,
        'title': title,
        'description': description,
        "img_link": image_link,
        "img_filename": img_filename
    }
    return row


def write_rows_to_excel(rows: dict, filepath: str = "output/reuters_results.xlsx") -> None:
    """ Write the extracted rows to an Excel file """
    excel = Excel()
    excel.create_workbook(filepath)
    excel.create_worksheet(name="Results", content=rows, header=True)
    excel.save_workbook()
