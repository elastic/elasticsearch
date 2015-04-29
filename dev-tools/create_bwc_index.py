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

import argparse
import glob
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time

DEFAULT_TRANSPORT_TCP_PORT = 9300
DEFAULT_HTTP_TCP_PORT = 9200

if sys.version_info[0] < 3:
  print('%s must use python 3.x (for the ES python client)' % sys.argv[0])

from datetime import datetime
try:
  from elasticsearch import Elasticsearch
  from elasticsearch.exceptions import ConnectionError
  from elasticsearch.exceptions import TransportError
except ImportError as e:
  print('Can\'t import elasticsearch please install `sudo pip3 install elasticsearch`')
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
                                                           'double_sort' : float(random.randint(0, 100)),
                                                           'bool' : random.choice([True, False])})
    if rarely():
      es.indices.refresh(index=index_name)
    if rarely():
      es.indices.flush(index=index_name, force=frequently())
  logging.info('Flushing index')
  es.indices.flush(index=index_name)

def delete_by_query(es, version, index_name, doc_type):

  logging.info('Deleting long_sort:[10..20] docs')

  query = {'query':
           {'range':
            {'long_sort':
             {'gte': 10,
              'lte': 20}}}}

  if version.startswith('0.') or version in ('1.0.0.Beta1', '1.0.0.Beta2'):
    # TODO #10262: we can't write DBQ into the translog for these old versions until we fix this back-compat bug:

    # #4074: these versions don't expect to see the top-level 'query' to count/delete_by_query:
    query = query['query']
    return

  deleted_count = es.count(index=index_name, doc_type=doc_type, body=query)['count']
    
  result = es.delete_by_query(index=index_name,
                              doc_type=doc_type,
                              body=query)

  # make sure no shards failed:
  assert result['_indices'][index_name]['_shards']['failed'] == 0, 'delete by query failed: %s' % result

  logging.info('Deleted %d docs' % deleted_count)

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

def start_node(version, release_dir, data_dir, tcp_port=DEFAULT_TRANSPORT_TCP_PORT, http_port=DEFAULT_HTTP_TCP_PORT, cluster_name=None):
  logging.info('Starting node from %s on port %s/%s, data_dir %s' % (release_dir, tcp_port, http_port, data_dir))
  if cluster_name is None:
    cluster_name = 'bwc_index_' + version
    
  cmd = [
    os.path.join(release_dir, 'bin/elasticsearch'),
    '-Des.path.data=%s' % data_dir,
    '-Des.path.logs=logs',
    '-Des.cluster.name=%s' % cluster_name,
    '-Des.network.host=localhost',
    '-Des.discovery.zen.ping.multicast.enabled=false',
    '-Des.transport.tcp.port=%s' % tcp_port,
    '-Des.http.port=%s' % http_port
  ]
  if version.startswith('0.') or version.startswith('1.0.0.Beta') :
    cmd.append('-f') # version before 1.0 start in background automatically
  return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def create_client(http_port=DEFAULT_HTTP_TCP_PORT, timeout=30):
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

def generate_index(client, version, index_name):
  client.indices.delete(index=index_name, ignore=404)
  logging.info('Create single shard test index')

  mappings = {}
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
    # completion type was added in 0.90.3
    if version not in ['0.90.0.Beta1', '0.90.0.RC1', '0.90.0.RC2', '0.90.0', '0.90.1', '0.90.2']:
      mappings['analyzer_type1']['properties']['completion_with_index_analyzer'] = {
        'type': 'completion',
        'index_analyzer': 'standard'
      }

    mappings['analyzer_type2'] = {
      'index_analyzer': 'standard',
      'search_analyzer': 'keyword',
      'search_quote_analyzer': 'english',
    }
    mappings['index_name_and_path'] = {
      'properties': {
        'parent_multi_field': {
          'type': 'string',
          'path': 'just_name',
          'fields': {
            'raw': {'type': 'string', 'index': 'not_analyzed', 'index_name': 'raw_multi_field'}
          }
        },
        'field_with_index_name': {
          'type': 'string',
          'index_name': 'custom_index_name_for_field'
        }
      }
    }
    mappings['meta_fields'] = {
      '_id': {
        'path': 'myid'
      },
      '_routing': {
        'path': 'myrouting'
      },
      '_boost': {
        'null_value': 2.0
      }     
    }
    mappings['custom_formats'] = {
      'properties': {
        'string_with_custom_postings': {
          'type': 'string',
          'postings_format': 'Lucene41'
        },
        'long_with_custom_doc_values': {
          'type': 'long',
          'doc_values_format': 'Lucene42'
        }
      }
    }
    mappings['auto_boost'] = {
      '_all': {
        'auto_boost': True
      }
    }

  client.indices.create(index=index_name, body={
      'settings': {
          'number_of_shards': 1,
          'number_of_replicas': 0
      },
      'mappings': mappings
  })
  health = client.cluster.health(wait_for_status='green', wait_for_relocating_shards=0)
  assert health['timed_out'] == False, 'cluster health timed out %s' % health

  num_docs = random.randint(2000, 3000)
  if version == "1.1.0":
    # 1.1.0 is buggy and creates lots and lots of segments, so we create a
    # lighter index for it to keep bw tests reasonable
    # see https://github.com/elastic/elasticsearch/issues/5817
    num_docs = int(num_docs / 10)
  index_documents(client, index_name, 'doc', num_docs)
  logging.info('Running basic asserts on the data added')
  run_basic_asserts(client, index_name, 'doc', num_docs)

def snapshot_index(client, cfg, version, repo_dir):
  # Add bogus persistent settings to make sure they can be restored
  client.cluster.put_settings(body={
    'persistent': {
      'cluster.routing.allocation.exclude.version_attr': version
    }
  })
  client.indices.put_template(name='template_' + version.lower(), order=0, body={
    "template": "te*",
    "settings": {
      "number_of_shards" : 1
    },
    "mappings": {
      "type1": {
        "_source": { "enabled" : False }
      }
    },
    "aliases": {
      "alias1": {},
      "alias2": {
        "filter": {
          "term": {"version" : version }
        },
        "routing": "kimchy"
      },
      "{index}-alias": {}
    }
  })
  client.snapshot.create_repository(repository='test_repo', body={
    'type': 'fs',
    'settings': {
      'location': repo_dir
    }
  })
  client.snapshot.create(repository='test_repo', snapshot='test_1', wait_for_completion=True)
  client.snapshot.delete_repository(repository='test_repo')

def compress_index(version, tmp_dir, output_dir):
  compress(tmp_dir, output_dir, 'index-%s.zip' % version, 'data')

def compress_repo(version, tmp_dir, output_dir):
  compress(tmp_dir, output_dir, 'repo-%s.zip' % version, 'repo')

def compress(tmp_dir, output_dir, zipfile, directory):
  abs_output_dir = os.path.abspath(output_dir)
  zipfile = os.path.join(abs_output_dir, zipfile)
  if os.path.exists(zipfile):
    os.remove(zipfile)
  logging.info('Compressing index into %s, tmpDir %s', zipfile, tmp_dir)
  olddir = os.getcwd()
  os.chdir(tmp_dir)
  subprocess.check_call('zip -r %s %s' % (zipfile, directory), shell=True)
  os.chdir(olddir)


def parse_config():
  parser = argparse.ArgumentParser(description='Builds an elasticsearch index for backwards compatibility tests')
  required = parser.add_mutually_exclusive_group(required=True)
  required.add_argument('versions', metavar='X.Y.Z', nargs='*', default=[],
                        help='The elasticsearch version to build an index for')
  required.add_argument('--all', action='store_true', default=False,
                        help='Recreate all existing backwards compatibility indexes')
  parser.add_argument('--releases-dir', '-d', default='backwards', metavar='DIR',
                      help='The directory containing elasticsearch releases')
  parser.add_argument('--output-dir', '-o', default='src/test/resources/org/elasticsearch/bwcompat',
                      help='The directory to write the zipped index into')
  parser.add_argument('--tcp-port', default=DEFAULT_TRANSPORT_TCP_PORT, type=int,
                      help='The port to use as the minimum port for TCP communication')
  parser.add_argument('--http-port', default=DEFAULT_HTTP_TCP_PORT, type=int,
                      help='The port to use as the minimum port for HTTP communication')
  cfg = parser.parse_args()

  if not os.path.exists(cfg.output_dir):
    parser.error('Output directory does not exist: %s' % cfg.output_dir)

  if not cfg.versions:
    # --all
    for bwc_index in glob.glob(os.path.join(cfg.output_dir, 'index-*.zip')):
      version = os.path.basename(bwc_index)[len('index-'):-len('.zip')]
      cfg.versions.append(version)

  return cfg

def create_bwc_index(cfg, version):
  logging.info('--> Creating bwc index for %s' % version)
  release_dir = os.path.join(cfg.releases_dir, 'elasticsearch-%s' % version)
  if not os.path.exists(release_dir):
    raise RuntimeError('ES version %s does not exist in %s' % (version, cfg.releases_dir))
  snapshot_supported = not (version.startswith('0.') or version == '1.0.0.Beta1')
  tmp_dir = tempfile.mkdtemp()

  data_dir = os.path.join(tmp_dir, 'data')
  repo_dir = os.path.join(tmp_dir, 'repo')
  logging.info('Temp data dir: %s' % data_dir)
  logging.info('Temp repo dir: %s' % repo_dir)

  node = None

  try:
    node = start_node(version, release_dir, data_dir, cfg.tcp_port, cfg.http_port)
    client = create_client(cfg.http_port)
    index_name = 'index-%s' % version.lower()
    generate_index(client, version, index_name)
    if snapshot_supported:
      snapshot_index(client, cfg, version, repo_dir)

    # 10067: get a delete-by-query into the translog on upgrade.  We must do
    # this after the snapshot, because it calls flush.  Otherwise the index
    # will already have the deletions applied on upgrade.
    delete_by_query(client, version, index_name, 'doc')

    shutdown_node(node)
    node = None

    compress_index(version, tmp_dir, cfg.output_dir)
    if snapshot_supported:
      compress_repo(version, tmp_dir, cfg.output_dir)
  finally:

    if node is not None:
      # This only happens if we've hit an exception:
      shutdown_node(node)
      
    shutil.rmtree(tmp_dir)

def shutdown_node(node):
  logging.info('Shutting down node with pid %d', node.pid)
  node.terminate()
  node.wait()
    
def main():
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)
  cfg = parse_config()
  for version in cfg.versions:
    create_bwc_index(cfg, version)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    print('Caught keyboard interrupt, exiting...')
