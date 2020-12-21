import os
import re
import hashlib
import logging
import mimetypes as mt
from . import patterns

logger = logging.getLogger(__name__)

class Role:
    UNKNOWN     = 'unknown'
    METADATA    = 'metadata'
    DATAFILE    = 'data'

def probe(path):
    '''
    Check file for DPdash compatibility and return a file
    information object.

    :param path: File path
    :type path: str
    '''
    if not os.path.exists(path):
        logger.debug('file not found %s', f)
        return None
    dirname = os.path.dirname(path)
    basename = os.path.basename(path)
    # match file and get re match object and file role
    role,match = match_file(basename)
    if role == Role.UNKNOWN:
        return None
    # initialize info object
    info = match.groupdict()
    info['glob'] = path
    if role == Role.DATAFILE:
        info.update(init_datafile(info))
        info['glob'] = get_glob(path)
    # add other necessary information to info object
    mimetype,encoding = mt.guess_type(path)
    stat = os.stat(path)
    info.update({
        'path' : path,
        'filetype' : mimetype,
        'encoding' : encoding,
        'basename' : basename,
        'dirname' : dirname,
        'dirty' : True,
        'synced' : False,
        'mtime' : stat.st_mtime,
        'size' : stat.st_size,
        'uid' : stat.st_uid,
        'gid' : stat.st_gid,
        'mode' : stat.st_mode,
        'role': role
    })
    return info

def match_file(f):
    match = patterns.DATAFILE.match(f)
    if match:
        return Role.DATAFILE, match
    match = patterns.METADATA.match(f)
    if match:
        return Role.METADATA, match
    return Role.UNKNOWN, None

def init_datafile(info):
    string = '{STUDY}{SUBJECT}{ASSESSMENT}'.format(
        STUDY=info['study'],
        SUBJECT=info['subject'],
        ASSESSMENT=info['assessment']
    )
    hash = hashlib.sha256(string.encode('utf-8'))
    return {
        'collection': hash.hexdigest(),
        'subject' : info['subject'],
        'assessment' : info['assessment'],
        'time_units' : str(info['units']),
        'time_start' : int(info['start']),
        'time_end' : int(info['end'])
    }

def get_glob(f):
    basename = os.path.basename(f)
    dirname = os.path.dirname(f)
    glob = patterns.GLOB_SUB.sub('\\1*\\2', basename)
    return os.path.join(dirname, glob)

def import_file(db, file_info):
    if file_info['role'] == 'data':
        collection = db['toc']
    elif file_info['role'] == 'metadata':
        collection = db['metadata']
    else:
        logger.error('incompatible file %s', file_info['path'])
        return
    diff_files(db, collection, file_info)
