# -*- coding: utf-8 -*-
import os
import click
import logging
from tqdm import tqdm
from dotenv import find_dotenv, load_dotenv

import requests
import json
import pandas as pd
from datetime import date
from geopy.geocoders import GoogleV3


def get_weather(latitude, longitude, obs_date, units='auto'):
    """ Takes a location coordinates and a date and returns the weather conditions.

        :param float latitude: Latitude
        :param float longitude: Longitude
        :param obs_date: Date for the observation
        :type obs_date: datetime.date
        :param units: Observation units. Default auto.
                      Possible values: auto, ca, uk2, us, si
        :returns: JSON object with the daily weather conditions or False
    """
    api_forecast_io = 'https://api.darksky.net/forecast/{}/{},{},{}?units={}'
    obs_date = '{}T00:00:00'.format(obs_date)
    lookup_url = api_forecast_io.format(os.environ.get('DARKSKY_KEY'),
                                        latitude,
                                        longitude,
                                        obs_date,
                                        units)
    response = requests.get(lookup_url)

    if response:
        return response.json()
    else:
        return


@click.command()
@click.argument('location', type=str)
@click.argument('year', type=int)
def main(location, year):
    """ Run data gathering scripts to fetch weather observation data from
        Dark Sky API for given year and location and save it as JSON at
        ../../data/raw/{location}/{year} in files doy.json, where doy is the
        day of the year.

        :param str location: Name of the location
        :param int year: Observations year to download
    """
    logger = logging.getLogger(__name__)
    logger.info('getting json data for every day of the year')

    # create folder path for saving the JSON data
    output_folder = os.path.join(project_dir, 'data', 'raw', location, str(year))
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # use `geopy` to get the coordinates of the location.
    geocode = GoogleV3().geocode(location)
    latitude = geocode.latitude
    longitude = geocode.longitude

    obs_dates = [d.date() for d in pd.date_range(start=date(year, 1, 1),
                                                 end=date(year, 12, 31),
                                                 normalize=True)]
    click.echo('Fetching the data from Dark Sky API:')
    with tqdm(total=len(obs_dates)) as pbar:
        for obs_date in obs_dates:
            doy = obs_date.timetuple().tm_yday
            obs_fn = os.path.normpath(os.path.join(output_folder, str(doy) + '.json'))

            if not os.path.exists(obs_fn):
                # get the json request for the weather observations for the day
                response = get_weather(latitude, longitude, obs_date)
                if response:
                    # check day of year in the response and assert is same in request
                    resp_date = date.fromtimestamp(response['daily']['data'][0]['time'])
                    resp_doy = resp_date.timetuple().tm_yday
                    try:
                        assert resp_doy == doy, 'Day of year should be equal in request and response.'
                    except AssertionError:
                        logger.error(u'Request day of year ({0:d}) diferent from in response ({1:d}).'
                                     .format(doy, resp_doy))
                    else:
                        logger.info('Request day of year same as in response.')

                    # write json file
                    with open(obs_fn, 'w') as fp:
                        json.dump(response, fp)
                else:
                    logger.warning(u'doy:{0:d} can\'t fetch data from API'.format(doy))
                    return
            else:
                logger.info('file {0} already exists, skipping'.format(obs_fn))
            pbar.update(1)


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.WARNING, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())
    load_dotenv(os.path.join(os.path.expanduser('~'), '.env'))

    main()
