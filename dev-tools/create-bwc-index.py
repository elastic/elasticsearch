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

import random
import os
import tempfile
import shutil
import subprocess
import time
import argparse
import logging
import sys
import re

if sys.version_info[0] > 2:
  print('%s must use python 2.x (for the ES python client)' % sys.argv[0])

from datetime import datetime
try:
  from elasticsearch import Elasticsearch
  from elasticsearch.exceptions import ConnectionError
  from elasticsearch.exceptions import TransportError
except ImportError as e:
  print('Can\'t import elasticsearch please install `sudo pip install elasticsearch`')
  sys.exit(1)

# sometimes returns True
def rarely():
  return random.randint(0, 10) == 0

# usually returns True
def frequently():
  return not rarely()

# asserts the correctness of the given hits given they are sorted asc
def assert_sort(hits):
  values = [hit['sort'] for hit in hits['hits']['hits']]
  assert len(values) > 0, 'expected non emtpy result'
  val = min(values)
  for x in values:
    assert x >= val, '%s >= %s' % (x, val)
    val = x

# Indexes the given number of document into the given index
# and randomly runs refresh, optimize and flush commands
def index_documents(es, index_name, type, num_docs):
  logging.info('Indexing %s docs' % num_docs)
  for id in range(0, num_docs):
    es.index(index=index_name, doc_type=type, id=id, body={'string': str(random.randint(0, 100)),
                                                           'long_sort': random.randint(0, 100),
                                                           'double_sort' : float(random.randint(0, 100))})
    if rarely():
      es.indices.refresh(index=index_name)
    if rarely():
      es.indices.flush(index=index_name, force=frequently())
  if rarely():
      es.indices.optimize(index=index_name)
  logging.info('Flushing index')
  es.indices.flush(index=index_name)

def run_basic_asserts(es, index_name, type, num_docs):
  count = es.count(index=index_name)['count']
  assert count == num_docs, 'Expected %r but got %r documents' % (num_docs, count)
  for _ in range(0, num_docs):
    random_doc_id = random.randint(0, num_docs-1)
    doc = es.get(index=index_name, doc_type=type, id=random_doc_id)
    assert doc, 'Expected document for id %s but got %s' % (random_doc_id, doc)

  assert_sort(es.search(index=index_name,
                  body={
                    'sort': [
                      {'double_sort': {'order': 'asc'}}
                    ]
                  }))

  assert_sort(es.search(index=index_name,
                  body={
                    'sort': [
                      {'long_sort': {'order': 'asc'}}
                    ]
                  }))


def build_version(version_tuple):
  return '.'.join([str(x) for x in version_tuple])

def build_tuple(version_string):
  return [int(x) for x in version_string.split('.')]

def start_node(version, release_dir, data_dir, tcp_port, http_port):
  logging.info('Starting node from %s on port %s/%s' % (release_dir, tcp_port, http_port))
  cmd = [
    os.path.join(release_dir, 'bin/elasticsearch'),
    '-Des.path.data=%s' % data_dir,
    '-Des.path.logs=logs',
    '-Des.cluster.name=bwc_index_' + version,  
    '-Des.network.host=localhost', 
    '-Des.discovery.zen.ping.multicast.enabled=false',
    '-Des.script.disable_dynamic=true',
    '-Des.transport.tcp.port=%s' % tcp_port,
    '-Des.http.port=%s' % http_port
  ]
  if version.startswith('0.') or version == '1.0.0.Beta1':
    cmd.append('-f') # version before 1.0 start in background automatically
  return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def create_client(http_port, timeout=30):
  logging.info('Waiting for node to startup')
  for _ in range(0, timeout):
    # TODO: ask Honza if there is a better way to do this?
    try:
      client = Elasticsearch([{'host': '127.0.0.1', 'port': http_port}])
      client.cluster.health(wait_for_nodes=1)
      client.count() # can we actually search or do we get a 503? -- anyway retry
      return client
    except (ConnectionError, TransportError):
      pass
    time.sleep(1)
  assert False, 'Timed out waiting for node for %s seconds' % timeout

def generate_index(client, cfg):
  client.indices.delete(index='test', ignore=404)
  num_shards = random.randint(1, 10)
  num_replicas = random.randint(0, 1)
  logging.info('Create single shard test index')

  mappings = {}
  version = cfg.version
  if not version.startswith('2.'):
    # TODO: we need better "before/onOr/after" logic in python

    # backcompat test for legacy type level analyzer settings, see #8874
    mappings['analyzer_type1'] = {
      'analyzer': 'standard',
      'properties': {
        'string_with_index_analyzer': {
          'type': 'string',
          'index_analyzer': 'standard'
        },
      }
    }
    if not version.startswith('0.20') or version == '0.20.6':
      mappings['analyzer_type1']['properties']['completion_with_index_analyzer'] = {
        'type': 'completion',
        'index_analyzer': 'standard'
      }

    mappings['analyzer_type2'] = {
      'index_analyzer': 'standard',
      'search_analyzer': 'keyword',
      'search_quote_analyzer': 'english',
    }

  client.indices.create(index='test', body={
      'settings': {
          'number_of_shards': 1,
          'number_of_replicas': 0
      },
      'mappings': mappings
  })
  health = client.cluster.health(wait_for_status='green', wait_for_relocating_shards=0)
  assert health['timed_out'] == False, 'cluster health timed out %s' % health

  num_docs = random.randint(10, 100)
  index_documents(client, 'test', 'doc', num_docs)
  logging.info('Running basic asserts on the data added')
  run_basic_asserts(client, 'test', 'doc', num_docs)

def compress_index(version, tmp_dir, output_dir):
  abs_output_dir = os.path.abspath(output_dir)
  zipfile = os.path.join(abs_output_dir, 'index-%s.zip' % version)
  if os.path.exists(zipfile):
    os.remove(zipfile)
  logging.info('Compressing index into %s', zipfile)
  olddir = os.getcwd()
  os.chdir(tmp_dir)
  subprocess.check_call('zip -r %s *' % zipfile, shell=True)
  os.chdir(olddir)

def parse_config():
  parser = argparse.ArgumentParser(description='Builds an elasticsearch index for backwards compatibility tests')
  parser.add_argument('version', metavar='X.Y.Z',
                      help='The elasticsearch version to build an index for')
  parser.add_argument('--releases-dir', '-d', default='backwards', metavar='DIR',
                      help='The directory containing elasticsearch releases')
  parser.add_argument('--output-dir', '-o', default='src/test/resources/org/elasticsearch/bwcompat',
                      help='The directory to write the zipped index into')
  parser.add_argument('--tcp-port', default=9300, type=int,
                      help='The port to use as the minimum port for TCP communication')
  parser.add_argument('--http-port', default=9200, type=int,
                      help='The port to use as the minimum port for HTTP communication')
  cfg = parser.parse_args()

  cfg.release_dir = os.path.join(cfg.releases_dir, 'elasticsearch-%s' % cfg.version)
  if not os.path.exists(cfg.release_dir):
    parser.error('ES version %s does not exist in %s' % (cfg.version, cfg.releases_dir)) 

  if not os.path.exists(cfg.output_dir):
    parser.error('Output directory does not exist: %s' % cfg.output_dir)

  cfg.tmp_dir = tempfile.mkdtemp()
  cfg.data_dir = os.path.join(cfg.tmp_dir, 'data')
  logging.info('Temp data dir: %s' % cfg.data_dir)

  return cfg

def main():
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)

  cfg = parse_config()
  try:
    node = start_node(cfg.version, cfg.release_dir, cfg.data_dir, cfg.tcp_port, cfg.http_port)
    client = create_client(cfg.http_port)
    generate_index(client, cfg)
  finally:
    if 'node' in vars():
      logging.info('Shutting down node with pid %d', node.pid)
      node.terminate()
  compress_index(cfg.version, cfg.tmp_dir, cfg.output_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('Caught keyboard interrupt, exiting...')
