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


def get_measure_df(measure_files_list, logger):
    """
    Combine all measure files into a single dataframe.
    """
    for i, fp in enumerate(measure_files_list):
        logger.info(f'Processing fp ({i+1}/{len(measure_files_list)}')
        with open(fp, 'r') as f:
            measure_data = json.load(f)

def deduplicate_measures_list(measures_list):
    """
    Take a list of file paths to measures; remove duplicates (preferring newer).
    """
    pass

def list_raw_measures(data_directory, logger) -> list:
    """
    List all measure files in the data directory.
    """
    measure_files = [os.path.join(data_directory, f) for f in os.listdir(data_directory)]
    measure_files = [f for f in measure_files if f.endswith('.json')]
    measure_files = [f for f in measure_files if '_measure_' in f]
    return measure_files

@click.command()
@click.argument('input_dir',
                type=click.Path(exists=True,
                                file_okay=False,
                                dir_okay=True,
                                readable=True,
                                resolve_path=True),)
@click.argument('output_dir',)
def main(input_dir, output_dir):
    """ Runs data processing scripts to turn raw data from (../raw) into
        a single interim dataset (../interim), for which later features
        will be engineered.
    """
    logger = logging.getLogger(__name__)
    logger.info('making measures data set from raw data measures directory')

    raw_measures_outpath = PurePath(project_dir).joinpath(output_dir).joinpath('raw_measures.pkl')

    raw_measure_files = list_raw_measures(input_dir, logger)

    raw_measures = get_measure_df(raw_measure_files, logger)
    raw_measures.to_pickle(raw_measures_outpath)


if __name__ == '__main__':
    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # setting the logs
    os.makedirs(PurePath(project_dir).joinpath('logs'), exist_ok=True)
    log_path = PurePath(project_dir).joinpath('logs/combine_measures.log')
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename=log_path, level=logging.INFO, format=log_fmt)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
