"""import os
import sys
import mimetypes
import boto3
from botocore.client import Config


path = os.path.expanduser(sys.argv[1])
excluded_files = ['.DS_Store']
s3 = boto3.client('s3', config=Config(signature_version='s3v4'))

mappings = {
    'thumbnails': 'shipshack-thumbnails',
    'exports': 'shipshack-processed'
}

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
                s3.put_object(
                    Bucket=bucket, Key=key, Body=f.read(),
                    ContentType=content_type)
                print(filepath, bucket, key)
"""
import sqlite3
import os
import boto3
from botocore.client import Config

s3 = boto3.client('s3', config=Config(signature_version='s3v4'))

conn = sqlite3.connect(os.path.expanduser('~/inventory.sqlite'))
cursor = conn.cursor()

cursor.execute('''SELECT * FROM files WHERE uploaded=0''')
results = cursor.fetchall()

for result in results:
    print('processing', result)
    row_id, path, bucket, key, content_type, uploaded = result
    with open(path) as f:
        s3.put_object(
            Bucket=bucket, Key=key, Body=f.read(), ContentType=content_type)
    cursor.execute('''UPDATE files SET uploaded=1 WHERE rowid=?''', [row_id])
    conn.commit()
    print('uploaded')
conn.close()
