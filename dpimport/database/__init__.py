import ssl
import fnmatch
import logging
from pymongo import MongoClient
from bson.json_util import dumps

logger = logging.getLogger(__name__)

class Database(object):
    def __init__(self, config, dbname):
        self.config = config
        self.dbname = dbname
        self.client = None
        self.db = None

    def connect(self):
        uri = 'mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{AUTH_SOURCE}'
        uri = uri.format(
            USERNAME=self.config['username'],
            PASSWORD=self.config['password'],
            HOST=self.config['hostname'],
            PORT=self.config['port'],
            AUTH_SOURCE=self.config['auth_source']
        )
        self.client = MongoClient(
            uri,
            ssl=True,
            ssl_cert_reqs=ssl.CERT_REQUIRED,
            ssl_certfile=self.config['ssl_certfile'],
            ssl_keyfile=self.config['ssl_keyfile'],
            ssl_ca_certs=self.config['ssl_ca_certs']
        )
        self.db = self.client[self.dbname]
        return self

    def remove_unsynced(self, expr):
        '''
        Remove all documents with sync: false matching the 
        input shell-style expression.

        :param expr: shell-style expression
        :type expr: str
        '''
        regex = fnmatch.translate(expr)
        cursor = self.db.toc.find({
            'path': {
                '$regex': regex,
            },
            'synced': False
        }, {
            'collection': True
        })
        for doc in cursor:
            _id = doc['_id']
            collection = doc['collection']
            # todo: wrap in a transaction, requires MongoDB 4.x
            logger.debug('dropping collection %s', collection)
            self.db[collection].drop()
            logger.debug('deleting toc document %s', _id)
            self.db.toc.remove({ '_id': _id })

    def exists(self, probe):
        '''
        Check if file exists in the database

        :param probe: File probe
        :type probe: dict
        '''
        doc = self.db.toc.find_one({
            'path': probe['path'],
            'size': probe['size']
        })
        if doc:
            return True
        return False

    def unsync(self, expr):
        '''
        Convert shell-style expression to a regular expression and 
        use that to match TOC entries for files stored in the 
        database and mark them as un-synced.

        :param expr: shell-style regular expression
        :type expr: str
        '''
        regex = fnmatch.translate(expr)
        docs = self.db.toc.update_many({
            'path': {
                '$regex': regex
            }
        }, {
            '$set': {
                'synced': False
            }
        })

