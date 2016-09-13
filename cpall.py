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
