import uuid
import logging

logger = logging.getLogger(__name__)

def import_file(db, file_info):
    if file_info['role'] == 'data':
        collection = db['toc']
    elif file_info['role'] == 'metadata':
        collection = db['metadata']
    else:
        logger.error('{FILE} is not compatible with DPdash. Exiting import.'.format(FILE=file_info['path']))
        return
    diff_files(db, collection, file_info)

# Match the file info with the record stored in the database
def diff_files(db, collection, file_info):
    file_path = file_info['path']
    db_data = collection.find_one({ 'path' : file_path })
    if not db_data:
        logger.info('{FILE} does not exist in the database. Importing.'.format(FILE=file_path))
        import_data(db, collection, file_info)
    else:
        if db_data['mtime'] != file_info['mtime'] or db_data['size'] != file_info['size']:
            logger.info('{FILE} has been modified. Re-importing.'.format(FILE=file_path))
            dbtools.remove_doc(db, collection, db_data, file_info['role'])
            import_data(db, collection, file_info)
        else:
            logger.info('Database already has {FILE}. Skipping.'.format(FILE=file_path))
            logged = log_success(collection, db_data['_id'])
            if logged == 0:
                logger.info('Journaling complete for {FILE}'.format(FILE=file_info['path']))

# Import data into the database
def import_data(db, ref_collection, file_info):
    if file_info['role'] == 'metadata':
        file_info.update({'collection': str(uuid.uuid4())})
    ref_id = insert_reference(ref_collection, file_info)
    if ref_id is None:
        logger.error('Unable to import {FILE}'.format(FILE=file_info['path']))
        return

    inserted = insert_data(db, file_info)
    if inserted == 0:
        logger.info('Import success for {FILE}'.format(FILE=file_info['path']))

        logged = log_success(ref_collection, ref_id)
        if logged == 0:
            logger.info('Journaling complete for {FILE}'.format(FILE=file_info['path']))

# Mark the sync as successful
def log_success(ref_collection, ref_id):
    update_ref = {
        '$set' : {
            'dirty': False,
            'synced' : True,
            'updated' : datetime.utcnow()
        }
    }

    try:
        ref_collection.update({
            '_id' : ref_id
        }, update_ref)
        return 0
    except Exception as e:
        logger.error(e)
        return 1

# Insert the reference doc, returns the inserted id
def insert_reference(collection, reference):
    try:
        ref_id = collection.insert_one(reference).inserted_id
        return ref_id
    except Exception as e:
        logger.error(e)
        return None

# Insert the data
def insert_data(db, file_info):
    try:
        # Import data
        data_blob = []
        import_collection = db[file_info['collection']]
        for chunk in reader.read_csv(file_info['path']):
            if len(chunk) > 0:
                if file_info['role'] != 'metadata':
                    chunk_columns = sanitize_columns(chunk.columns.values.tolist())
                    chunk.columns = chunk_columns
                chunk['path'] = file_info['path']
                data_blob.extend(chunk.round(4).to_dict('records'))

                if len(data_blob) >= 100000:
                    import_collection.insert_many(data_blob, False)
                    data_blob = []
        if data_blob:
            import_collection.insert_many(data_blob, False)
        return 0
    except Exception as e:
        logger.error(e)
        logger.error('Unable to import {FILE}'.format(FILE=file_info['path']))

# Rename columns to encode special characters
def sanitize_columns(columns):
    new_columns = []
    for column in columns:
        new_column = quote(unicode(column).encode('utf-8'), safe='~()*!.\'').replace('.', '%2E')
        new_columns.append(new_column)

    return new_columns

