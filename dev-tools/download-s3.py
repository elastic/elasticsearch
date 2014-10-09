# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance  with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on 
# an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

import os
import sys
import argparse
try:
  import boto.s3
except:
  raise RuntimeError("""
  S3 download requires boto to be installed
    Use one of:
      'pip install -U boto'
      'apt-get install python-boto'
      'easy_install boto'
  """)

import boto.s3


def list_buckets(conn):
  return conn.get_all_buckets()


def download_s3(conn, path, key, file, bucket):
  print 'Downloading %s from Amazon S3 bucket %s/%s' % \
        (file, bucket,  os.path.join(path, key))
  def percent_cb(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()
  bucket = conn.get_bucket(bucket)
  k = bucket.get_key(os.path.join(path, key))
  k.get_contents_to_filename(file, cb=percent_cb, num_cb=100)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Downloads a bucket from Amazon S3')
  parser.add_argument('--file', '-f', metavar='path to file',
                      help='path to store the bucket to', required=True)
  parser.add_argument('--bucket', '-b', default='downloads.elasticsearch.org',
                      help='The S3 Bucket to download from')
  parser.add_argument('--path', '-p', default='',
                      help='The key path to use')
  parser.add_argument('--key', '-k', default=None,
                      help='The key - uses the file name as default key')
  args = parser.parse_args()
  if args.key:
    key = args.key
  else:
    key = os.path.basename(args.file)
  connection = boto.connect_s3()
  download_s3(connection, args.path, key, args.file, args.bucket);
