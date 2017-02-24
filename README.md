# Hyperion Metadata Generator

This small library helps with generating EO-1 Hyperion metadata and upload it to Amazon S3 and/or ElasticSearch

## Installation

    $ pip install -r requirements.txt

## Usage

```
    $ python main.py --help

    Usage: main.py [OPTIONS] <operations: choices: s3 | es | disk>

    Options:
      --start TEXT            Start Date. Format: YYYY-MM-DD
      --end TEXT              End Date. Format: YYYY-MM-DD
      --folder TEXT           Destination folder if is written to disk
      --download              Sets the updater to download the metadata file first
                              instead of streaming it
      --download-folder TEXT  The folder to save the downloaded metadata to.
                              Defaults to a temp folder
      -v, --verbose
      --help                  Show this message and exit.
```

Example:

    $ python main.py s3 es --start='2016-01-01' --verbose

## Enviroment Variables

Elastic Search Environment Variables

	ES_HOST
	ES_PORT

S3 bucket:

	BUCKETNAME

## CSV
Hyperion.csv

## About
The EO-1 Hyperion Metadata Generator was inspired by the Landsat8 Metadata Generator, made by [Development Seed](http://developmentseed.org).

## To Delete Index
```
    $ curl -XDELETE $ES_HOST/sat-api