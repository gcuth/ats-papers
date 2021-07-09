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


def get_fnames_from_meta_dict(wp_info):
    """
    docstring
    """
    fnames = []
    meeting = wp_info['Meeting_type'] + wp_info['Meeting_number']
    pnum = wp_info['Abbreviation'] + str(wp_info['Number']).zfill(3)  # zero pad
    if wp_info['Revision'] > 0:  # a 'rev#' included in filename iff revisions
        revision = f"rev{wp_info['Revision']}"
    else:
        revision = None
    for country in ['e','s','f','r']:
        fname = '_'.join([x for x in [meeting, pnum, revision, country] if x])
        fname += '.' + wp_info['Type']
        fnames.append(fname)
    return fnames


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


def join_metadata_to_document_df(df, metadata, logger):
    """Take a raw document df and join metadata for each document row.

    :document_df: TODO
    :metadata: TODO
    :returns: TODO

    """
    logger.info(f'Matching metadata for all {df.shape[0]} lines in dataframe')
    matching_metadata = []
    for i, row in df.iterrows():
        logger.info(f'Collecting metadata for row ({i+1}/{df.shape[0]})')
        meta = get_best_matching_metadata(row, metadata, logger)
        logger.info(f'Successfully collected metadata for row ({i+1}/{df.shape[0]})')
        matching_metadata.append(meta)
        logger.info(f'{len([m for m in matching_metadata if m != {}])} now collected.')
    logger.info(f'Adding meeting years')
    df['meeting_year'] = [m.get('Meeting_year') for m in matching_metadata]
    logger.info(f'Adding paper names')
    df['paper_name'] = [m.get('Name') for m in matching_metadata]
    logger.info(f'Adding paper IDs')
    df['paper_id'] = [m.get('Paper_id') for m in matching_metadata]
    logger.info(f'Adding paper type IDs')
    df['paper_type_id'] = [m.get('Pap_type_id') for m in matching_metadata]
    logger.info(f'Adding meeting types')
    df['meeting_type'] = [m.get('Meeting_type') for m in matching_metadata]
    logger.info(f'Adding meeting IDs')
    df['meeting_id'] = [m.get('Meeting_id') for m in matching_metadata]
    logger.info(f'Adding meeting numbers')
    df['meeting_number'] = [m.get('Meeting_number') for m in matching_metadata]
    logger.info(f'Adding meeting names')
    df['meeting_name'] = [m.get('Meeting_name') for m in matching_metadata]
    logger.info(f'Adding party names')
    df['parties'] = [', '.join([name for name in [p.get('Name','') for p in m.get('Parties',[])] if len(name)>0]) for m in matching_metadata]
    return df


def get_best_matching_metadata(document_row, metadata, logger):
    """TODO: Docstring for get_matching_metadata.

    :document_row: TODO
    :metadata: TODO
    :returns: TODO

    """
    logger.info(f'Starting with a metadata list totalling {len(metadata)} items')
    meta = [m for m in metadata if str(m['Type']) == document_row['extension']]
    logger.info(f'After filtering metadata list for doc extension: {len(meta)} items')
    meta = [m for m in meta if m['Abbreviation'] == document_row['paper_type_abbreviation']]
    logger.info(f'After filtering metadata list for paper type abbreviation: {len(meta)} items')
    meta = [m for m in meta if str(m['Number']) == document_row['paper_number'].lstrip("0")]
    logger.info(f'After filtering metadata list for paper number: {len(meta)} items')
    meta = [m for m in meta if str(m['Meeting_type']) in document_row['meeting']]
    logger.info(f'After filtering metadata list for meeting type: {len(meta)} items')
    meta = [m for m in meta if m['Revision'] == document_row['paper_revision']]
    logger.info(f'After filtering metadata list for paper revision number: {len(meta)} items')
    meta = [m for m in meta if document_row['filename'] in get_fnames_from_meta_dict(m)]
    logger.info(f'After filtering metadata list for matching filename: {len(meta)} items')
    if len(meta) == 1:
        logger.info(f'Exactly one matching metadata dictionary found')
        return meta[0]
    elif len(meta) > 1:
        logger.info(f'Error: Found too many matching metadata files: {len(meta)}')
        time.sleep(1)
        return {}
    else:
        logger.info(f'Error: Failed to find a matching metadata dictionary!')
        return {}

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

    logger.info(f'joining metadata to document dataframe')
    full_doc_df = join_metadata_to_document_df(raw_doc_df, metadata, logger)

    print(full_doc_df.shape)
    print(full_doc_df.head())

    logger.info(f'processed dataframe of documents has shape: {full_doc_df.shape}')
    logger.info(f'saving final dataframe to {final_df_outpath}')
    full_doc_df.to_pickle(final_df_outpath)

    final_df_outpath = str(final_df_outpath).replace('.pkl','.csv')
    logger.info(f'saving final dataframe to {final_df_outpath}')
    full_doc_df.to_csv(final_df_outpath)

    final_df_outpath = final_df_outpath.replace('.csv','.xlsx')
    logger.info(f'saving final dataframe to {final_df_outpath}')
    full_doc_df.to_excel(final_df_outpath)

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
