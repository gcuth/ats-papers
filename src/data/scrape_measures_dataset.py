# -*- coding: utf-8 -*-
import os
import click
import logging
import json
import requests
from bs4 import BeautifulSoup
from requests.exceptions import Timeout
import random
from datetime import datetime
from pathlib import Path, PurePath
from dotenv import find_dotenv, load_dotenv

"""
This script aims to collect all measures found at:
    https://www.ats.aq/devAS/Meetings/Measure/{some_n}

As with scrapes of the other docs, queries to ats.aq's Doc Database are slow.

Note that this script does not currently collect 'Final Reports', which are at
    https://www.ats.aq/devAS/Info/FinalReports

Note also that this script does not currently collect 'attachments'.

We'll get there.
"""

def measure_url(n: int) -> str:
    """Construct the url suitable for scraping a given measure."""
    return f'https://www.ats.aq/devAS/Meetings/Measure/{n}'


def get_approvals_from_scrape(soup):
    """Take a BeautifulSouped page and return a clean list of all approval dates."""
    table = soup.find('table', attrs={'class':'approvals'})
    approvals = []
    if table:
        for tr in table.find_all('tr'):
            country = tr.find('th')
            date = tr.find('td')
            if country and date:
                approval = {'country': country.get_text(strip=True),
                            'date': date.get_text(strip=True)}
                approvals.append(approval)
    return approvals

def get_body_text_from_scrape(soup):
    """Take a BeautifulSouped page and return a body text for the measure if one exists."""
    raw_text = soup.find_all('div', attrs={'class':'text-container'})
    if len(raw_text) > 0:
        # cludgy solution to retain line breaks in the first match:
        return str(raw_text[0]).replace('<br/>','\n').replace('</div>','').replace('<div class="text-container">','').strip()

def get_characteristics_from_scrape(soup):
    """Take a BeautifulSouped page and return a dictionary of characteristics."""
    raw_characteristics = soup.find_all('ul', attrs={'class':'characteristics__list'})[0].find_all('li', attrs={'class':'characteristics__item'})
    clean_characteristics = []
    for characteristic in raw_characteristics:
        pair = {
            'title': characteristic.find('h2', attrs={'class':'characteristics__item__title'}).text,
        }
        item_text = characteristic.find('p', attrs={'class':'characteristics__item__text'})
        if item_text is not None:
            pair['text'] = item_text.get_text(strip=True)
        else:
            pair['text'] = None
        clean_characteristics.append(pair)
    characteristic_dict = {}
    for characteristic in clean_characteristics:
        k = characteristic['title'].lower().replace(' ','_')
        v = characteristic['text']
        if v:
            characteristic_dict[k] = v
    return characteristic_dict

def scrape_measure(n, logger):
    """Return a dictionary for a measure's info (or none if the scrape fails)."""
    url = measure_url(n)
    logger.info(f'beginning scrape for measure {n} at {url}')
    r = requests.get(url)
    logger.info(f'scrape of {n} at {url} returned {r.status_code}')
    if r.ok:
        soup = BeautifulSoup(r.content, features='lxml')
        raw_title = soup.find_all('h1', attrs={'class':'title'})[0].text
        info = {
            'raw_title': raw_title,
            'raw_text': get_body_text_from_scrape(soup),
            'characteristics': get_characteristics_from_scrape(soup),
            'approvals': get_approvals_from_scrape(soup)}
        return info

def construct_measure_path(data_dir, n):
    """ Construct a filepath suitable for dumping measure info as a json file. """
    fname = ''.join([datetime.now().strftime('%Y-%m-%d-%H-%M-%S'), '_measure_', str(n), '.json'])
    return os.path.join(data_dir, fname)

@click.command()
@click.argument('output_dir', type=click.Path(exists=True))
def main(output_dir):
    """ Runs data scraping scripts to populate raw data (../raw/measures).
    """
    logger = logging.getLogger(__name__)
    logger.info('using environment variables to generate scrape outpath')

    absolute_output_dir = PurePath(project_dir).joinpath(output_dir)

    for measure_n in range(1,1000):
        measure_info = scrape_measure(measure_n, logger)
        if isinstance(measure_info, dict):
            logger.info(f'scrape of measure {measure_n} successful')
            measure_info['measure_number'] = measure_n
            measure_info['scraped_at'] = datetime.now().isoformat()
            outpath = construct_measure_path(absolute_output_dir, measure_n)
            logger.info(f'attempting to save scrape of measure {measure_n} to {outpath}')
            with open(outpath, 'w+') as f:
                json.dump(measure_info, f)
            if os.path.exists(outpath):
                logger.info(f'scrape file now exists at {outpath}')


if __name__ == '__main__':
    # the base directory from which we'll resolve the 'data/raw' path etc.
    project_dir = Path(__file__).resolve().parents[2]

    # setting the log
    os.makedirs(PurePath(project_dir).joinpath('logs'), exist_ok=True)
    log_path = PurePath(project_dir).joinpath('logs/scrape_measures.log')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=log_path, level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
