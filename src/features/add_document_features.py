# -*- coding: utf-8 -*-
import os
import re
import sys
import json
import time
import click
import logging
import pypandoc
import pdftotext
import numpy as np
import pandas as pd
import subprocess
from pathlib import Path, PurePath
from dotenv import find_dotenv, load_dotenv


def load_all_metadata(data_dir, logger) -> list:
    """Load all available paper metadata from files in data_dir.

    TODO: SWITCH TO IMPORTING THIS FROM scrape_dataset.py

    :data_dir: TODO
    :returns: TODO

    """
    papers = []
    files = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]
    logger.info(f'{len(files)} total files found in {data_dir}')
    all_json = [f for f in files if f.endswith('.json')]
    logger.info(f'{len(all_json)} json files found in {data_dir}')
    meta = [f for f in all_json if 'papers_metadata' in f]
    logger.info(f'{len(meta)} existing metadata files found in {data_dir}')
    if meta:
        for meta_path in meta:
            with open(meta_path, 'r') as f:
                logger.info(f'reading metadata from {meta_path}')
                metadata = json.load(f)
                logger.info(f'{len(metadata)} papers found in {meta_path}')
                papers += metadata
    # filter for unique entries (using workaround because dicts aren't hashable)
    dedupe = [json.loads(i) for i in set(json.dumps(p, sort_keys=True) for p in papers)]
    logger.info(f'metadata for {len(dedupe)} unique papers found in {data_dir}')
    return papers


def join_metadata_to_document_df(document_df, metadata):
    """Take a raw document df and join metadata for each document row.

    :document_df: TODO
    :metadata: TODO
    :returns: TODO

    """
    pass


def get_best_matching_metadata(document_row, metadata):
    """TODO: Docstring for get_matching_metadata.

    :document_row: TODO
    :metadata: TODO
    :returns: TODO

    """
    meta = [m for m in metadata if m['Abbreviation'] == document_row['paper_type_abbreviation']]
    meta = [m for m in meta if m['Meeting_type'] in document_row['meeting']]
    meta = [m for m in meta if m['Meeting_type'] in document_row['meeting']]
    meta = [m for m in meta if m['Number'] == int(document_row['paper_number'].lstrip("0"))]
    if len(meta) == 1:
        return meta[0]
    else:
        return None

@click.command()
@click.argument('raw_data_dir', # directory containing raw documents (and metadata)
                type=click.Path(exists=True,
                                file_okay=False,
                                dir_okay=True,
                                readable=True,
                                resolve_path=True),)
@click.argument('interim_data_dir',) # directory containing pickled dataframe
@click.argument('output_dir',) # directory containing pickled dataframe
def main(raw_data_dir, interim_data_dir, output_dir):
    """ Finds metadata in the raw documents directory, adding it to records in
        the interim data where possible, then outputing an expanded dataframe to ../processed
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from interim dataframe')

    final_df_outpath = PurePath(project_dir).joinpath(output_dir).joinpath('ats_documents.pkl')
    logger.info(f'planned outpath for final dataframe: {final_df_outpath}')
    
    if os.path.isdir(interim_data_dir):
        logger.info(f'{interim_data_dir} appears to be a directory; checking default filenames')
        raw_documents_df_path = PurePath(project_dir).joinpath(interim_data_dir).joinpath('raw_documents.pkl')
    else:
        raw_documents_df_path = interim_data_dir # this shouldn't ever be called, but is a sensible fallback
    logger.info(f'loading dataframe from {raw_documents_df_path}')
    raw_doc_df = pd.read_pickle(raw_documents_df_path)
    logger.info(f'loaded dataframe of raw documents has shape: {raw_doc_df.shape}')

    metadata = load_all_metadata(raw_data_dir, logger)

    print(raw_doc_df.shape)
    print(raw_doc_df.head())

    # print(raw_doc_df.iloc[0])
    get_best_matching_metadata(raw_doc_df.iloc[0], metadata)


if __name__ == '__main__':
    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # setting the logs
    os.makedirs(PurePath(project_dir).joinpath('logs'), exist_ok=True)
    log_path = PurePath(project_dir).joinpath('logs/add_document_features.log')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=log_path, level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
