# -*- coding: utf-8 -*-
import os
import click
import logging
import json
import requests
from requests.exceptions import Timeout
import random
from datetime import datetime
from pathlib import Path, PurePath
from dotenv import find_dotenv, load_dotenv

"""
This script aims to collect all meeting documents found at:
    https://www.ats.aq/devAS/Meetings/DocDatabase

We do so by scraping iteratively querying the underlying doc database to collect
paper metadata, then resolving that paper metadata into document links. Queries
to ats.aq's Doc Database are slow, so even the metadata portion of this scrape
currently takes over an hour.

Note that this script does not currently collect 'Final Reports', which are at
    https://www.ats.aq/devAS/Info/FinalReports

Note also that this script does not currently collect 'attachments'.

We'll get there.
"""


def construct_document_links(wp_info: dict) -> list:
    """Take a dict of paper metadata; return a list of associated document urls.

    Note: this excludes 'attachment' documents, which are resolved separately.

    Ideally, this will be a set of four links (E/S/F/R), but may be less.

    :wp_info: A dict of some metadata about a working paper.
    :returns: A list of all resolved document links

    """
    document_links = []
    meeting = wp_info['Meeting_type'] + wp_info['Meeting_number']
    base = 'https://documents.ats.aq/' + meeting + '/' + wp_info['Abbreviation']
    pnum = wp_info['Abbreviation'] + str(wp_info['Number']).zfill(3)  # zero pad
    if wp_info['Revision'] > 0:  # a 'rev#' included in filename iff revisions
        revision = f"rev{wp_info['Revision']}"
    else:
        revision = None
    for country in ['e','s','f','r']:
        fname = '_'.join([x for x in [meeting, pnum, revision, country] if x])
        fname += '.' + wp_info['Type']
        full_path = base + '/' + fname
        document_links.append(full_path)
    return document_links


def scrape_document_from_link(doc_link, logger, timeout=(2,5)):
    """Take a link and download the associated file as bytes.

    :doc_link: TODO
    :logger: TODO
    :returns: TODO

    """
    logger.info(f"requesting {doc_link}")
    try:
        r = requests.get(doc_link, timeout=timeout)
        logger.info(f"{doc_link} returned status {r.status_code}")
        if r.ok:
            return r.content
    except Timeout as e:
        logger.info(f"{doc_link} scrape attempt timed-out after {timeout} with exception: {e}")


def scrape_documents(wp_info: dict, out_dir, logger, ignore_existing=True):
    """Take raw info about a working paper, generate doc urls, scrape the files.

    :wp_info: A dict of some metadat about a working paper.
    :out_dir: The base directory for saving scraped documents.
    :returns: TODO

    """
    logger.info(f"constructing document links for {wp_info['Paper_id']}")
    doc_links = construct_document_links(wp_info)
    if ignore_existing:
        logger.info(f"filtering document links for already-scraped papers")
        logger.info(f"constructed document links before filter: {len(doc_links)}")
        outpaths = [(doc_link, construct_document_outpath(out_dir, wp_info, doc_link)) for doc_link in doc_links]
        logger.info(f"target outpaths: {[str(x[1]) for x in outpaths]}")
        existing = [os.path.join(out_dir, f) for f in os.listdir(out_dir)]
        logger.info(f"already collected outpaths: {[str(x[1]) for x in outpaths if str(x[1]) in existing]}")
        doc_links = [x[0] for x in outpaths if str(x[1]) not in existing]
        logger.info(f"constructed document links after filter: {len(doc_links)}")
    for doc_link in doc_links:
        logger.info(f"attempting to scrape file at {doc_link}")
        raw_doc = scrape_document_from_link(doc_link, logger)
        if raw_doc:
            logger.info(f"successful scrape of {doc_link}")
            outpath = construct_document_outpath(out_dir, wp_info, doc_link)
            logger.info(f"writing file scraped from {doc_link} to {outpath}")
            with open(outpath, 'wb+') as f:
                f.write(raw_doc)
        else:
            logger.info(f"failed scrape of {doc_link}")


def construct_document_outpath(out_dir, wp_info, doc_link):
    """Generate a best outpath for saving the actual scraped file.

    :wp_info: TODO
    :doc_link: TODO
    :returns: TODO

    """
    fname = doc_link.split('/')[-1]
    return PurePath(out_dir).joinpath(fname)


def construct_metadata_scrape_path(base_data_path):
    """Take a base data path and return a sensible filepath for metadata scrape.

    :base_data_path: TODO
    :returns: TODO

    """
    now = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
    default_fname = f"{now}_papers_metadata.json"
    return PurePath(base_data_path).joinpath(default_fname)


def construct_wp_url(page: int):
    """Generate an ats.aq doc database url from a page number (for querying).

    :page: TODO
    :returns: TODO

    """
    url = f"https://www.ats.aq/devAS/Meetings/SearchDocDatabase?page={page}"
    return url


def scrape_working_papers_listing(starting_page, logger):
    """Use ats.aq doc database endpoint to collect list of papers from a page.

    :page: Some num for of the page of results to start with.
    :logger: A logger object for reporting on results.
    :returns: TODO

    """
    logger.info(f'starting with page {starting_page}')
    current_page = starting_page  # set the starting page as our 'current'
    url = construct_wp_url(current_page)  # construct the first url
    papers = []
    while url is not None:
        logger.info(f"attempting to scrape {url}")
        r = requests.get(url)
        logger.info(f"got {r.status_code} for {url}")
        data = json.loads(r.text)
        papers += data['payload']
        logger.info(f"total {len(papers)} listings collected so far")
        next_page = data['pager']['next']
        if next_page > current_page:
            current_page = next_page
            url = construct_wp_url(current_page)
        else:
            url = None
    return papers


@click.command()
@click.argument('output_path', type=click.Path(exists=True))
def main(output_path):
    """ Runs data scraping scripts to populate raw data (../raw).
    """
    logger = logging.getLogger(__name__)
    logger.info('using environment variables to generate scrape outpath')

    absolute_output_path = PurePath(project_dir).joinpath(output_path)

    existing_metadata_files = [os.path.join(absolute_output_path, f) for f in os.listdir(absolute_output_path) if f.endswith('.json')]
    if existing_metadata_files:
        logger.info(f'{len(existing_metadata_files)} existing metadata files found')
        logger.info(f'reading {existing_metadata_files[-1]}')
        with open(existing_metadata_files[-1], 'r') as f:
            papers = json.load(f)
    else:
        logger.info(f'no existing metadata scrape; beginning')
        metadata_outpath = construct_metadata_scrape_path(absolute_output_path)
        logger.info(f'best metadata outpath: {metadata_outpath}')
        logger.info('beginning scrape of working papers listing')
        papers = scrape_working_papers_listing(starting_page=1, logger=logger)
        logger.info(f'saving papers metadata to file at {metadata_outpath}')
        with open(metadata_outpath, 'w+') as f:
            json.dump(papers, f, indent=2)
        if os.path.exists(metadata_outpath):
            logger.info(f'metadata file now exists at {metadata_outpath}')
    logger.info(f'beginning collection of underlying paper documents')
    random.shuffle(papers) # shuffle in place
    for i, paper in enumerate(papers):
        try:
            logger.info(f"attempting scrape of paper {paper['Paper_id']} ({i+1}/{len(papers)})")
            scrape_documents(paper, absolute_output_path, logger=logger)
        except Exception as e:
            logger.info(f"attempted scrape of paper {paper['Paper_id']} failed with {e}")

if __name__ == '__main__':
    # the base directory from which we'll resolve the 'data/raw' path etc.
    project_dir = Path(__file__).resolve().parents[2]

    # setting the log
    os.makedirs(PurePath(project_dir).joinpath('logs'), exist_ok=True)
    log_path = PurePath(project_dir).joinpath('logs/scrape.log')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=log_path, level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
