# -*- coding: utf-8 -*-
import glob
import json
import logging
import os
import re

import click
import pandas as pd
import pandas.io.json as pd_json
from dotenv import find_dotenv, load_dotenv
from tqdm import tqdm


def get_observations(obs_folder, key):
    """ Takes the path and target key and returns a pandas dataframe with
        the observations for a full year and the time zone of the data.

        :param str obs_folder: Folder path for the yearly observations;
        :param str key: Dictionary key;
        :returns pandas.DataFrame: Weather observations;
        :returns str: Time zone of the local.
    """
    year_pattern = os.path.join(obs_folder, '*.json')
    year_files = glob.glob(year_pattern)

    # read observation json files
    observations = []
    for fn in year_files:
        with open(fn, 'r') as fp:
            raw_obs = fp.read()
            json_obs = json.loads(raw_obs)

            # import data to pandas
            key_data = json_obs[key]['data']
            observations.append(pd_json.json_normalize(key_data))

    observations = pd.concat(observations)
    time_zone = json_obs['timezone']

    return observations, time_zone


def get_datetime(observations, time_zone):
    """ Due to an old outstanding bug in Pandas there are some problems
        concatenating dataframes with datetime when some columns are
        missing in some of them. This could be overcomed by using the
        `merge` function, but it is much slower to process this way.

        :param pandas.DataFrame observations: All the local observations;
        :param str time_zone: Time Zone of the local;
        :returns pandas.DataFrame: Weather observations.
    """

    # transform all timestamps into datetime
    columns = []
    regex_expression = "(.+|)[tT]ime"
    for column in observations.columns:
        columns.append(str(column))
    time_columns = [m.group(0) for l in columns for m in [re.fullmatch(regex_expression, l)] if m]
    for column in time_columns:
        observations[column] = pd.to_datetime(observations[column],
                                              unit='s',
                                              infer_datetime_format=True).dt.tz_localize('UTC').dt.tz_convert(time_zone)

    # set the dataframe index to the time column
    observations.set_index('time', inplace=True)
    observations.sort_index(inplace=True)

    return observations


@click.command()
@click.argument('input_filepath', default='data/raw', type=click.Path(exists=True))
@click.argument('output_filepath', default='data/interim', type=click.Path(exists=True))
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw daily data from
        `data/raw/{location}/{year}/*.json`
        into interim CSV data saved in `data/interim/{location}.csv`
        and into interim XZ zipped PICKLE data saved in `data/interim/{location}.xz`.
    """
    logger = logging.getLogger(__name__)
    logger.info('making interim CSV and pickled data set from daily raw data')

    input_folder = os.path.normpath(os.path.join(project_dir, input_filepath))
    output_folder = os.path.normpath(os.path.join(project_dir, output_filepath))

    # get all the files/directories under `input_filepath` and filter in directories
    locations = glob.glob(os.path.join(input_folder, '*'))
    locations = filter(os.path.isdir, locations)

    for location_path in locations:
        _, location = os.path.split(location_path)
        click.echo("\nParsing " + location + ":")
        year_regex = re.compile(r'(.+\d+)')
        years = glob.glob(os.path.join(location_path, '*'))
        years = filter(os.path.isdir, years)
        years = sorted(list(filter(year_regex.search, years)))

        observations = pd.DataFrame()
        with tqdm(total=len(years)) as pbar:
            for year_path in years:
                pbar.set_postfix({'year': year_path[-4:]})
                observation, time_zone = get_observations(year_path, 'daily')
                observations = pd.concat([observations, observation])
                pbar.update(1)

        # set the datetime
        observations = get_datetime(observations, time_zone)

        # ERROR: location tem de ser apenas o sítio
        # write CSV file
        output_csv_file = os.path.join(output_folder, location + "_daily.csv")
        observations.to_csv(output_csv_file)

        # Write xz compressed pickle file
        output_pkl_file = os.path.join(output_folder, location + "_daily.xz")
        observations.to_pickle(output_pkl_file)        


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    load_dotenv(os.path.join(os.path.expanduser('~'), '.env'))

    main()
