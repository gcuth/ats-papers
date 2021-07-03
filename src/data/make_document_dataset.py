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


def convert_doc_to_docx(data_path, logger):
    """TODO: Docstring for convert_doc_to_docx

    :data_path: TODO
    :returns: the path to the converted document
    """
    logger.info(f'calling external unoconv process to convert {data_path} to docx')
    call_status = subprocess.call(['unoconv', '-f', 'docx', data_path])
    logger.info(f'unoconv conversion attempt returned {call_status}')
    doc_dir, doc_name = os.path.split(data_path)
    expected_outpath = os.path.join(doc_dir, doc_name.replace('.doc','.docx'))
    logger.info(f'Expected new outpath: {expected_outpath}')
    if os.path.exists(expected_outpath):
        logger.info(f'os path reports that {expected_outpath} now exists')
    else:
        logger.info(f'Error in conversion: {expected_outpath} does not exist!')
        time.sleep(10)
    return expected_outpath

def get_doc_text(data_path, logger):
    """TODO: Docstring for get_doc_text.

    :data_path: TODO
    :returns: TODO

    """
    logger.info(f'Using pypandoc to convert {data_path} to txt')
    try:
        output = pypandoc.convert_file(data_path, 'plain', format='docx')
        return output
    except Exception as e:
        logger.info(f'Error when attempting to convert {data_path}: {e}')
        if data_path.endswith('doc'):
            logger.info(f'{data_path} appears to be a doc, rather than a docx')
            logger.info(f'trying to convert {data_path} to docx')
            new_path = convert_doc_to_docx(data_path, logger)
            return get_doc_text(new_path, logger)
        return None

def get_pdf_text(data_path, logger):
    """TODO: Docstring for get_pdf_text.

    :data_path: TODO
    :returns: TODO

    """
    logger.info(f'Using pdftotext to convert {data_path} to txt')
    logger.info(f'reading raw bytes from {data_path}')
    try:
        with open(data_path, "rb") as f:
            pdf = pdftotext.PDF(f)
    except Exception as e:
        logger.info(f'error when reading {data_path} to pdf: {e}')
        pdf = None
    if pdf:
        logger.info(f'pdftotext reports {len(pdf)} pages in {data_path}')
        logger.info(f'converting {data_path} to single string')
        output = "\n\n".join(pdf)
    else:
        output = None
    return output

def get_text_for_document(data_path, logger):
    """Read a file from path into a long string.

    :data_path: TODO
    :returns: TODO

    """
    logger.info(f'collecting text from {data_path}')
    if data_path.endswith('.doc'):
        logger.info(f'{data_path} appears to be a doc file')
        text = get_doc_text(data_path, logger)
    elif data_path.endswith('.pdf'):
        logger.info(f'{data_path} appears to be a pdf file')
        text = get_pdf_text(data_path, logger)
    else:
        text = None
    if text:
        logger.info(f'{data_path} returned text {len(text)} in length')
    else:
        logger.info(f'{data_path} returned no valid text!')
    return text

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


def list_raw_docs(data_dir):
    """List all raw docs/papers found in data dir.

    :data_dir: TODO
    :returns: TODO

    """
    in_dir = [os.path.join(data_dir, f) for f in os.listdir(data_dir)]
    files = [f for f in in_dir if os.path.isfile(f)]
    docs = [f for f in files if f.endswith('.doc') or f.endswith('.pdf')]
    return docs

def get_doc_df(raw_docs:list, metadata:list, logger):
    """TODO: Docstring for get_doc_df.

    :raw_docs: TODO
    :metadata: TODO
    :logger: TODO
    :returns: TODO

    """
    raw_documents = pd.DataFrame(columns = ['filename','extension','meeting', 'paper_type_abbreviation','paper_number', 'paper_revision','paper_language_abbreviation','raw_text'])
    for i, doc_path in enumerate(raw_docs):
        logger.info(f'Processing {doc_path} ({i+1}/{len(raw_docs)})')
        directory, filename = os.path.split(doc_path)
        extension = filename.split('.')[-1]
        meeting = filename.split('_')[0]
        paper_type_abbreviation = re.findall(r'\D+|$', filename.split('_')[1])[0]
        paper_number = re.findall(r'\d+|$', filename.split('_')[1])[0]
        paper_language_abbreviation = filename.split('_')[-1].split('.')[0]
        if '_rev' in filename:
            bits = [x for x in filename.split('_') if 'rev' in x]
            paper_revision = re.findall(r'\d+|$', bits[0])[0]
        else:
            paper_revision = 0
        raw_text = get_text_for_document(doc_path, logger)
        new_row = {'filename': filename,
                   'extension': extension,
                   'meeting': meeting,
                   'paper_type_abbreviation': paper_type_abbreviation,
                   'paper_number': paper_number,
                   'paper_revision': paper_revision,
                   'paper_language_abbreviation': paper_language_abbreviation,
                   'raw_text': raw_text}
        raw_documents = raw_documents.append(new_row, ignore_index = True)
        logger.info(f'documents database now has {raw_documents.shape}')
        n_empty_docs = (raw_documents['raw_text'].values == None).sum()
        logger.info(f'documents database now has {n_empty_docs} empty documents')
    return raw_documents

@click.command()
@click.argument('input_dir',
                type=click.Path(exists=True,
                                file_okay=False,
                                dir_okay=True,
                                readable=True,
                                resolve_path=True),)
                # help='Path to raw scrape data.')
@click.argument('output_dir',)
                # help='Path to save processed data.')
def main(input_dir, output_dir):
    """ Runs data processing scripts to turn raw data from (../raw) into
        a single interim dataset (../interim), for which later features
        will be engineered. The main process here is *conversion* via pypandoc
        and external calls to unoconv.
    """
    logger = logging.getLogger(__name__)
    logger.info('making document data set from raw data documents directory')

    raw_documents_outpath = PurePath(project_dir).joinpath(output_dir).joinpath('raw_documents.pkl')

    raw_docs = list_raw_docs(input_dir)
    metadata = load_all_metadata(input_dir, logger)

    raw_documents = get_doc_df(raw_docs, metadata, logger)
    raw_documents.to_pickle(raw_documents_outpath)


if __name__ == '__main__':
    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # setting the logs
    os.makedirs(PurePath(project_dir).joinpath('logs'), exist_ok=True)
    log_path = PurePath(project_dir).joinpath('logs/combine_documents.log')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=log_path, level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
