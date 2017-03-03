import os, sys, json, logging, boto3, click

from copy import copy
from collections import OrderedDict
from datetime import date, timedelta
from elasticsearch import Elasticsearch, RequestError
from reader import csv_reader

sys.path.append('../hyperion-l1u')
import config

logger      = logging.getLogger('hyperion.meta')
bucket_name = os.getenv('BUCKETNAME', 'hyperion-meta')
s3          = boto3.resource('s3')
aws_s3_dir	= os.path.join("https://s3.amazonaws.com", bucket_name)

es_index    = 'sat-api'
es_type     = 'hyperion'
host_url    = "http://hyperion-api.herokuapp.com"

def create_index(index_name, doc_type):
    
    body = {
        doc_type: {
            'properties': {
                'scene_id': {'type': 'string', 'index': 'not_analyzed'},
                'satellite_name': {'type': 'string'},
                'cloud_coverage': {'type': 'float'},
                'date': {'type': 'date'},
                'data_geometry': {
                    'type': 'geo_shape',
                    'tree': 'quadtree',
                    'precision': '5mi'}
            }
        }
    }
    
    es.indices.create(index=index_name, ignore=400)
    
    es.indices.put_mapping(
        doc_type=doc_type,
        body=body,
        index=index_name
    )

def convert_date(value):
    dt      = value.split(' ')[0]
    arr     = dt.split('/')
    year    = int(arr[2])+2000
    month   = int(arr[0])
    day     = int(arr[1])
    
    datestr = "%d-%02d-%02d" % (year, month, day)
    return datestr

def convert_SceneName(value):
    scene = value.split('_')
    return scene[0]

def meta_constructor(metadata):
    internal_meta = copy(metadata)
    
    data_geometry = {
        "type": "polygon",
        "crs": {
            "type": "name",
            "properties": {
                "name": "urn:ogc:def:crs:EPSG:8.9:4326"
            }
        },
        "coordinates": [[
            [ metadata.get('CornerLonUpperLeft'),  metadata.get('CornerLatUpperLeft')],
            [ metadata.get('CornerLonUpperRight'), metadata.get('CornerLatUpperRight')],
            [ metadata.get('CornerLonLowerRight'), metadata.get('CornerLatLowerRight')],
            [ metadata.get('CornerLonLowerLeft'),  metadata.get('CornerLatLowerLeft')],
            [ metadata.get('CornerLonUpperLeft'),  metadata.get('CornerLatUpperLeft')]
        ]]
    }

    dt 			= convert_date(metadata.get('SceneDate'))
    scene_id 	= convert_SceneName(metadata.get('SceneName'))
    year 		= scene_id[10:14]
    doy			= scene_id[14:17]

    l1u_geotiffs = []
    for b in range(242):
        if config.bbl[b]:
            filename = os.path.join(aws_s3_dir, "L1U", year, doy, scene_id, scene_id+"_B%03d_L1U.TIF" % (b+1))
            l1u_geotiffs.append( { "band_%03d"%(b+1) : { "href" : filename} } )
	
    actions = {
        "downloads": {
            "L1U": {
                "ENVI.BIL": {
                    "href": os.path.join(aws_s3_dir, "L1U", year, doy, scene_id, scene_id+"_L1U.tar.gz")
                },
                "GEOTIFF": l1u_geotiffs
            }
        },
        "process": {
            "rgb":  {"href": "%s/eo1/process?scene=%s&algorithm=rgb"%(host_url, scene_id+"_L1U")},
            "ndvi": {"href": "%s/eo1/process?scene=%s&algorithm=ndvi"%(host_url,scene_id+"_L1U")},
            "evi":  {"href": "%s/eo1/process?scene=%s&algorithm=evi"%(host_url, scene_id+"_L1U")}
        }
    }
    
    body = OrderedDict([
        ('scene_id', scene_id),
        ('satellite_name', 'eo1'),
        ('cloud_coverage', metadata.get('MaxCloudCover')),
        ('date', dt),
        ('thumbnail', "https://earthexplorer.usgs.gov/browse/eo-1/hyp/" + metadata.get('BrowseImageLocation')),
        ('data_geometry', data_geometry),
        ('actions', actions)
    ])
    
    body.update(internal_meta)
    return body


def elasticsearch_updater(product_dir, metadata):
    
    try:
        body = meta_constructor(metadata)
        
        logger.info('Pushing to Elasticsearch')
		
        try:
            es.index(index=es_index, doc_type=es_type, id=body['scene_id'], body=body)
        except RequestError as e:
            print "ES RequestError", e
            print  body['data_geometry']
            body['data_geometry'] = None
            es.index(index=es_index, doc_type=es_type, id=body['scene_id'], body=body)
    
    except Exception as e:
        logger.error('Unhandled error occured while writing to elasticsearch')
        logger.error('Details: %s' % e.__str__())
        logger.error('Error on line {}'.format(sys.exc_info()[-1].tb_lineno))


def file_writer(product_dir, metadata):
    print "file_writer", product_dir
    
    body = meta_constructor(metadata)
    
    if not os.path.exists(product_dir):
        os.makedirs(product_dir)
    
    f = open(os.path.join(product_dir, body['scene_id'] + '.json'), 'w')
    f.write(json.dumps(body, indent=4, separators=(',',': ')))
    logger.info('saving to disk at %s' % product_dir)
    f.close()


def s3_writer(product_dir, metadata):
    print "s3_writer", bucket_name
    # make sure product_dir doesn't start with slash (/) or dot (.)
    if product_dir.startswith('.'):
        product_dir = product_dir[1:]
    
    if product_dir.startswith('/'):
        product_dir = product_dir[1:]
    
    body = meta_constructor(metadata)
    
    key = os.path.join(product_dir, body['scene_id'] + '.json')
    s3.Object(bucket_name, key).put(Body=json.dumps(body), ACL='public-read', ContentType='application/json')
    
    logger.info('saving to s3 at %s', key)


def last_updated(today):
    """ Gets the latest time a product added to Elasticsearch """
    
    bucket = s3.Bucket(bucket_name)
    
    start_day = today.day
    start_month = today.month
    
    yr_counter = 0
    while True:
        m_counter = 0
        year = today.year - yr_counter
        if year < 2015:
            break
        while True:
            month = start_month - m_counter
            if month == 0:
                start_month = 12
                break
            d_counter = 0
            while True:
                day = start_day - d_counter
                if day == 0:
                    start_day = 31
                    break
                path = os.path.join(str(year), str(month), str(day))
                logger.info('checking %s' % path)
                objs = bucket.objects.filter(Prefix=path).limit(1)
                if list(objs):
                    return date(year, month, day)
                d_counter += 1
            m_counter += 1
        yr_counter += 1
    
    return None


@click.command()
@click.argument('ops', metavar='<operations: choices: s3 | es | disk>', nargs=-1)
@click.option('--start', default=None, help='Start Date. Format: MM/DD/YY')
@click.option('--end', default=None, help='End Date. Format: MM/DD/YY')
#@click.option('--es-host', default='localhost', help='Elasticsearch host address')
#@click.option('--es-port', default=9200, type=int, help='Elasticsearch port number')
@click.option('--csv', default='.', help='csv file to read')
@click.option('--folder', default='.', help='Destination folder if is written to disk')
@click.option('--download', is_flag=True,
              help='Sets the updater to download the metadata file first instead of streaming it')
@click.option('--download-folder', default=None,
              help='The folder to save the downloaded metadata to. Defaults to a temp folder')
@click.option('-v', '--verbose', is_flag=True)
@click.option('--concurrency', default=20, type=int, help='Process concurrency. Default=20')

# python main.py disk s3 es --csv Hyperion_3.csv --folder ../data/metadata --start 01/01/03 -v
# python main.py disk --csv Hyperion_3.csv --folder ../data/metadata --start 01/01/03 -v

def main(ops, csv, start, end, folder, download, download_folder, verbose, concurrency):
    
    if not ops:
        raise click.UsageError('No Argument provided. Use --help if you need help')
    
    accepted_args = {
        'es': elasticsearch_updater,
        's3': s3_writer,
        'disk': file_writer
    }
	
    writers = []
    for op in ops:
        if op in accepted_args.keys():
            writers.append(accepted_args[op])
        else:
            raise click.UsageError('Operation (%s) is not supported' % op)
    
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    
    if verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.ERROR)
    
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    print "folder", folder, "csv", csv
		
    if 'es' in ops:
        global es
        
        es_port = int(os.getenv('ES_PORT', 80))
        es_host = os.getenv('ES_HOST', "NOT_AVAILABLE")
        
        logger.info("Connecting to Elastic Search %s:%d", es_host, es_port)
        es = Elasticsearch([{
            'host': es_host,
            'port': es_port
        }])
        
        create_index(es_index, es_type)
    
    if not start and not end:
        delta = timedelta(days=3)
        start = date.today() - delta
        start = '{0}/{1}/{2}'.format(start.month, start.day,start.year-2000)
    
    csv_reader(csv, folder, writers, start_date=start, end_date=end, download=download, download_path=download_folder,
               num_worker_threads=concurrency)
    


if __name__ == '__main__':
    main()
