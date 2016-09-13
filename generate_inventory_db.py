import sqlite3
import sys
import os
import mimetypes

path = sys.argv[1]
excluded_files = ['.DS_Store']
mappings = {
    'thumbnails': 'shipshack-thumbnails',
    'exports': 'shipshack-processed'
}

db_path = os.path.expanduser('~/inventory.sqlite')
if os.path.exists(db_path):
    os.remove(db_path)
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE files (
    prowid INTEGER PRIMARY KEY AUTOINCREMENT, path, bucket,
    key, content_type, uploaded INTEGER)''')


for root, dirnames, filenames in os.walk(path):
    for filename in filenames:
        if filename not in excluded_files:
            filepath = os.path.join(root, filename)
            print('processing', filepath)
            key = os.path.relpath(filepath, path)
            bucket = 'shipshack'
            mapping_key = key.split('/')
            if mapping_key:
                mapping_key = mapping_key[0]
                mapping = mappings.get(mapping_key)
                if mapping:
                    key = os.path.relpath(key, mapping_key)
                    bucket = mapping
            content_type = mimetypes.guess_type(filepath)[0]
            if not content_type:
                content_type = ''
            with open(filepath) as f:
                print(filepath, bucket, key)
                cursor.execute(
                    "INSERT INTO files VALUES (NULL, ?, ?, ?, ?, 0)",
                    [filepath, bucket, key, content_type])
conn.commit()
conn.close()
