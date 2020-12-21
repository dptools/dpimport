#!/usr/bin/env python

import os
import sys
import ssl
import glob
import yaml
import dppylib
import dpimport
import logging
import argparse as ap
import collections as col
import dpimport.importer as importer
from dpimport.database import Database

logger = logging.getLogger(__name__)

def main():
    parser = ap.ArgumentParser()
    parser.add_argument('-c', '--config')
    parser.add_argument('-d', '--dbname', default='dpdata')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('expr')
    args = parser.parse_args()

    level = logging.INFO
    if args.verbose:
        level = logging.DEBUG
    logging.basicConfig(level=level)

    with open(os.path.expanduser(args.config), 'r') as fo:
        config = yaml.load(fo, Loader=yaml.SafeLoader)

    db = Database(config, args.dbname).connect()

    # iterate over matching files on the filesystem
    for f in glob.iglob(args.expr):
        dirname = os.path.dirname(f)
        basename = os.path.basename(f)
        # probe for dpdash-compatibility and gather information
        probe = dpimport.probe(f)
        if not probe:
            logger.debug('document is unknown %s', basename)
            continue
        # nothing to be done
        if db.exists(probe):
            logger.info('document exists and is up to date %s', probe['path'])
            continue
        logger.info('document does not exist or is out of date %s', probe['path'])
        # mark matching documents as unsynced (probably unnecessary)
        logger.info('flipping sync to false for documents matching %s', probe['glob'])
        db.unsync(probe['glob'])
        # remove unsynced documents
        logger.info('removing all unsynced documents matching %s', probe['glob'])
        db.remove_unsynced(probe['glob'])
        # import the file
        logger.info('importing file %s', f)
        dppylib.import_file(db.db, probe)

    logger.info('cleaning metadata')
    lastday = get_lastday(db.db)
    if lastday:
        clean_metadata(db.db, lastday)

def clean_metadata(db, max_days):
    studies = col.defaultdict()
    subjects = list()

    for subject in max_days:
        if subject['_id']['study'] not in studies:
            studies[subject['_id']['study']] = {}
            studies[subject['_id']['study']]['subject'] = []
            studies[subject['_id']['study']]['max_day'] = 0

            # if there are more than 2, drop unsynced
            metadata = list(db.metadata.find(
                {
                    'study' : subject['_id']['study']
                },
                {
                    '_id' : True,
                    'collection' : True,
                    'synced' : True
                }
            ))

            if len(metadata) > 1:
                for doc in metadata:
                    if doc['synced'] is False and 'collection' in doc:
                        db[doc['collection']].drop()
                    if doc['synced'] is False:
                        db.metadata.delete_many(
                            {
                                '_id': doc['_id']
                            }
                        )

        subject_metadata = col.defaultdict()
        subject_metadata['subject'] = subject['_id']['subject']
        subject_metadata['synced'] = subject['synced']
        subject_metadata['days'] = subject['days']
        subject_metadata['study'] = subject['_id']['study']

        studies[subject['_id']['study']]['max_day'] = studies[subject['_id']['study']]['max_day'] if (studies[subject['_id']['study']]['max_day'] >= subject['days'] ) else subject['days']

        studies[subject['_id']['study']]['subject'].append(subject_metadata)

    for study, subject in studies.iteritems():
        bulk_metadata = db.metadata.initialize_ordered_bulk_op()
        bulk_metadata.find({'study' : study}).upsert().update({'$set' :
            {
                'synced' : True,
                'subjects' : studies[study]['subject'],
                'days' : studies[study]['max_day']
            }
        })

        bulk_metadata.find({'study' : study, 'synced' : False}).remove()
        bulk_metadata.find({'study' : study }).update({'$set' : {'synced' : False}})

        try:
            bulk_metadata.execute()
        except BulkWriteError as e:
            logger.error(e)

def get_lastday(db):
    return list(db.toc.aggregate([
        {
            '$group' : {
                '_id' : {
                    'study': '$study',
                    'subject' : '$subject'
                },
                'days' : {
                    '$max' : '$time_end'
                },
                'synced' : {
                    '$max' : '$updated'
                }
            }
        }
    ]))

def clean_toc(db):
    logger.info('cleaning table of contents')
    out_of_sync_tocs = db.toc.find(
        {
            'synced' : False
        },
        {
            '_id' : False,
            'collection' : True,
            'path' : True 
        }
    )
    
    for doc in out_of_sync_tocs:
        db[doc['collection']].delete_many(
            {
                'path' : doc['path']
            }
        )

    bulk = db.toc.initialize_ordered_bulk_op()
    bulk.find(
        {
            'synced' : False
        }
    ).remove()
    try:
        bulk.execute()
    except BulkWriteError as e:
        logger.error(e)

def clean_toc_study(db, study):
    logger.info('cleaning table of contents for {0}'.format(study))
    out_of_sync_tocs = db.toc.find(
        {
            'study' : study,
            'synced' : False
        },
        {
            '_id' : False,
            'collection' : True,
            'path' : True 
        }
    )
    for doc in out_of_sync_tocs:
        db[doc['collection']].delete_many(
            {
                'path' : doc['path']
            }
        )

    bulk = db.toc.initialize_ordered_bulk_op()
    bulk.find(
        {
            'study' : study,
            'synced' : False
        }
    ).remove()
    try:
        bulk.execute()
    except BulkWriteError as e:
        logger.error(e)

if __name__ == '__main__':
    main()

