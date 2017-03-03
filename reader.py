import os
import time
import logging
import requests
import csv
from time import strftime

from concurrent import futures
from datetime import datetime
from homura import download as fetch
from tempfile import mkdtemp
from collections import OrderedDict

logger = logging.getLogger('hyperion.meta')

class ReachedEndOfProcess(Exception):
    pass


def convert_date(value):
    #return datetime.strptime(value, '%Y-%m-%d').date()
    return datetime.strptime(value, '%m/%d/%y').date()


def download_meta(url, download_path):
    dpath = download_path if download_path else mkdtemp()
    dpath = os.path.join(dpath, 'Hyperion.csv')

    # don't download if the file is downloaded in the last 6 hours
    if os.path.isfile(dpath):
        mtime = os.path.getmtime(dpath)
        if time.time() - mtime < (6 * 60 * 60):
            return open(dpath, 'r')

    fetch(url, dpath)
    return open(dpath, 'r')


def row_processor(record, date, dst, writers):

    path = os.path.join(dst, str(date.year), date.strftime("%j"))

    logger.info('processing %s' % record['SceneName'])
    for w in writers:
        w(path, record)


def csv_reader(fname, dst, writers, start_date=None, end_date=None, url=None,
               download=False, download_path=None, num_worker_threads=1):
    """ Reads hyperion metadata from a csv file stored on USGS servers
    and applys writer functions on the data """

    #if not url:
    #    url = 'https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/LANDSAT_8.csv'
	
    # download the whole file
    #if download:
    #    logger.info('Downloading hyperion metadata file', url)

        # don't download if the file is downloaded in the last 6 hours
    #    f = download_meta(url, download_path)
    #    liner = f.readlines

    # or read line by line
    #else:
    #    logger.info('Streaming hyperion metadata file')
    #    r = requests.get(url, stream=True)
    #    liner = r.iter_lines
	
    liner = csv.reader(open(fname, 'rb'))
    
    print start_date, end_date
    
    if start_date:
        start_date = convert_date(start_date)

    if end_date:
        end_date = convert_date(end_date)

    header = None

    # read the header
    header = liner.next()
    
    def gen(line):
        row = line  #.split(',')
        for j, v in enumerate(row):
            try:
                row[j] = float(v)
            except ValueError:
                pass

        # generate the record
        record = OrderedDict(zip(header, row))
        date = convert_date(record['SceneDate'].split(' ')[0])

        # apply filter
        # if there is an enddate, stops the process when the end date is reached
        if end_date and date > end_date:
            return

        if start_date and date < start_date:
            return

        row_processor(record, date, dst, writers)

    for line in liner:
        gen(line)
        
    #with futures.ThreadPoolExecutor(max_workers=num_worker_threads) as executor:
    #    try:
    #        executor.map(gen, liner, timeout=30)
    #    except futures.TimeoutError:
    #        print('skipped')
