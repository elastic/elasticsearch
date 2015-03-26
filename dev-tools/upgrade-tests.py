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

from datetime import datetime
try:
  from elasticsearch import Elasticsearch
  from elasticsearch.exceptions import ConnectionError
  from elasticsearch.exceptions import TransportError
except ImportError as e:
  print('Can\'t import elasticsearch please install `sudo pip install elasticsearch`')
  raise e


'''This file executes a basic upgrade test by running a full cluster restart.

The upgrade test starts 2 or more nodes of an old elasticserach version, indexes
a random number of documents into the running nodes and executes a full cluster restart.
After the nodes are recovered a small set of basic checks are executed to ensure all
documents are still searchable and field data can be loaded etc.

NOTE: This script requires the elasticsearch python client `elasticsearch-py` run the following command to install:

  `sudo pip install elasticsearch`

if you are running python3 you need to install the client using pip3. On OSX `pip3` will be included in the Python 3.4
release available on `https://www.python.org/download/`:

  `sudo pip3 install elasticsearch`

See `https://github.com/elasticsearch/elasticsearch-py` for details

In order to run this test two different version of elasticsearch are required. Both need to be unpacked into
the same directory:

```
   $ cd /path/to/elasticsearch/clone
   $ mkdir backwards && cd backwards
   $ wget  https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.3.1.tar.gz
   $ wget  https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.13.tar.gz
   $ tar -zxvf elasticsearch-1.3.1.tar.gz && tar -zxvf elasticsearch-0.90.13.tar.gz
   $ cd ..
   $ python dev-tools/upgrade-tests.py --version.backwards 0.90.13 --version.current 1.3.1
```
'''

BLACK_LIST = {'1.2.0' : { 'reason': 'Contains a major bug where routing hashes are not consistent with previous version',
                          'issue': 'https://github.com/elasticsearch/elasticsearch/pull/6393'},
              '1.3.0' : { 'reason': 'Lucene Related bug prevents upgrades from 0.90.7 and some earlier versions ',
                          'issue' : 'https://github.com/elasticsearch/elasticsearch/pull/7055'}}
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

# asserts that the cluster health didn't timeout etc.
def assert_health(cluster_health, num_shards, num_replicas):
  assert cluster_health['timed_out'] == False, 'cluster health timed out %s' % cluster_health


# Starts a new elasticsearch node from a released & untared version.
# This node uses unicast discovery with the provided unicast host list and starts
# the nodes with the given data directory. This allows shutting down and starting up
# nodes on the same data dir simulating a full cluster restart.
def start_node(version, data_dir, node_dir, unicast_host_list, tcp_port, http_port):
  es_run_path = os.path.join(node_dir, 'elasticsearch-%s' % (version), 'bin/elasticsearch')
  if version.startswith('0.90.'):
    foreground = '-f' # 0.90.x starts in background automatically
  else:
    foreground = ''
  return subprocess.Popen([es_run_path,
    '-Des.path.data=%s' % data_dir, '-Des.cluster.name=upgrade_test',  
    '-Des.discovery.zen.ping.unicast.hosts=%s' % unicast_host_list, 
    '-Des.discovery.zen.ping.multicast.enabled=false',
    '-Des.transport.tcp.port=%s' % tcp_port,
    '-Des.http.port=%s' % http_port,
    foreground], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


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
  es.indices.refresh(index=index_name)

# Runs a basic number of assertions including:
#  - document counts
#  - match all search with sort on double / long
#  - Realtime GET operations
# TODO(simonw): we should add stuff like:
#  - dates including sorting
#  - string sorting
#  - docvalues if available
#  - global ordinal if available
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


# picks a random version or and entire random version tuple from the directory
# to run the backwards tests against.
def pick_random_upgrade_version(directory, lower_version=None, upper_version=None):
  if lower_version and upper_version:
    return lower_version, upper_version
  assert os.path.isdir(directory), 'No such directory %s' % directory
  versions = []
  for version in map(lambda x : x[len('elasticsearch-'):], filter(lambda x : re.match(r'^elasticsearch-\d+[.]\d+[.]\d+$', x), os.listdir(directory))):
    if not version in BLACK_LIST:
      versions.append(build_tuple(version))
  versions.sort()

  if lower_version: # lower version is set - picking a higher one
    versions = filter(lambda x : x > build_tuple(lower_version), versions)
    assert len(versions) >= 1, 'Expected at least 1 higher version than %s version in %s ' % (lower_version, directory)
    random.shuffle(versions)
    return lower_version, build_version(versions[0])
  if upper_version:
    versions = filter(lambda x : x < build_tuple(upper_version), versions)
    assert len(versions) >= 1, 'Expected at least 1 lower version than %s version in %s ' % (upper_version, directory)
    random.shuffle(versions)
    return build_version(versions[0]), upper_version
  assert len(versions) >= 2, 'Expected at least 2 different version in %s but found %s' % (directory, len(versions))
  random.shuffle(versions)
  versions = versions[0:2]
  versions.sort()
  return build_version(versions[0]), build_version(versions[1])

def build_version(version_tuple):
  return '.'.join([str(x) for x in version_tuple])

def build_tuple(version_string):
  return [int(x) for x in version_string.split('.')]

# returns a new elasticsearch client and ensures the all nodes have joined the cluster
# this method waits at most 30 seconds for all nodes to join
def new_es_instance(num_nodes, http_port, timeout = 30):
  logging.info('Waiting for %s nodes to join the cluster' % num_nodes)
  for _ in range(0, timeout):
    # TODO(simonw): ask Honza if there is a better way to do this?
    try:
      es = Elasticsearch([
      {'host': '127.0.0.1', 'port': http_port + x}
        for x in range(0, num_nodes)])
      es.cluster.health(wait_for_nodes=num_nodes)
      es.count() # can we actually search or do we get a 503? -- anyway retry
      return es
    except (ConnectionError, TransportError):
      pass
    time.sleep(1)
  assert False, 'Timed out waiting for %s nodes for %s seconds' % (num_nodes, timeout)

def assert_versions(bwc_version, current_version, node_dir):
  assert [int(x) for x in bwc_version.split('.')] < [int(x) for x in current_version.split('.')],\
      '[%s] must be < than [%s]' % (bwc_version, current_version)
  for version in [bwc_version, current_version]:
    assert not version in BLACK_LIST, 'Version %s is blacklisted - %s, see %s' \
                                          % (version, BLACK_LIST[version]['reason'],
                                             BLACK_LIST[version]['issue'])
    dir = os.path.join(node_dir, 'elasticsearch-%s' % current_version)
    assert os.path.isdir(dir), 'Expected elasticsearch-%s install directory does not exists: %s' % (version, dir)

def full_cluster_restart(node_dir, current_version, bwc_version, tcp_port, http_port):
  assert_versions(bwc_version, current_version, node_dir)
  num_nodes = random.randint(2, 3)
  nodes = []
  data_dir = tempfile.mkdtemp()
  logging.info('Running upgrade test from [%s] to [%s] seed: [%s] es.path.data: [%s] es.http.port [%s] es.tcp.port [%s]'
        % (bwc_version, current_version, seed, data_dir, http_port, tcp_port))
  try:
    logging.info('Starting %s BWC nodes of version %s' % (num_nodes, bwc_version))
    unicast_addresses = ','.join(['127.0.0.1:%s' % (tcp_port+x) for x in range(0, num_nodes)])
    for id in range(0, num_nodes):
      nodes.append(start_node(bwc_version, data_dir, node_dir, unicast_addresses, tcp_port+id, http_port+id))
    es = new_es_instance(num_nodes, http_port)
    es.indices.delete(index='test_index', ignore=404)
    num_shards = random.randint(1, 10)
    num_replicas = random.randint(0, 1)
    logging.info('Create index with [%s] shards and [%s] replicas' % (num_shards, num_replicas))
    es.indices.create(index='test_index', body={
        # TODO(simonw): can we do more here in terms of randomization - seems hard due to all the different version
        'settings': {
            'number_of_shards': num_shards,
            'number_of_replicas': num_replicas
        }
    })
    logging.info('Nodes joined, waiting for green status')
    health = es.cluster.health(wait_for_status='green', wait_for_relocating_shards=0)
    assert_health(health, num_shards, num_replicas)
    num_docs = random.randint(10, 100)
    index_documents(es, 'test_index', 'test_type', num_docs)
    logging.info('Run basic asserts before full cluster restart')
    run_basic_asserts(es, 'test_index', 'test_type', num_docs)
    logging.info('kill bwc nodes -- prepare upgrade')
    for node in nodes:
      node.terminate()

    # now upgrade the nodes and rerun the checks
    tcp_port = tcp_port + len(nodes) # bump up port to make sure we can claim them
    http_port = http_port + len(nodes)
    logging.info('Full Cluster restart starts upgrading to version [elasticsearch-%s] es.http.port [%s] es.tcp.port [%s]'
                 % (current_version, http_port, tcp_port))
    nodes = []
    unicast_addresses = ','.join(['127.0.0.1:%s' % (tcp_port+x) for x in range(0, num_nodes)])
    for id in range(0, num_nodes+1): # one more to trigger relocation
      nodes.append(start_node(current_version, data_dir, node_dir, unicast_addresses, tcp_port+id, http_port+id))
    es = new_es_instance(num_nodes+1, http_port)
    logging.info('Nodes joined, waiting for green status')
    health = es.cluster.health(wait_for_status='green', wait_for_relocating_shards=0)
    assert_health(health, num_shards, num_replicas)
    run_basic_asserts(es, 'test_index', 'test_type', num_docs)
    # by running the indexing again we try to catch possible mapping problems after the upgrade
    index_documents(es, 'test_index', 'test_type', num_docs)
    run_basic_asserts(es, 'test_index', 'test_type', num_docs)
    logging.info("[SUCCESS] - all test passed upgrading from version [%s] to version [%s]" % (bwc_version, current_version))
  finally:
    for node in nodes:
      node.terminate()
    time.sleep(1) # wait a second until removing the data dirs to give the nodes a chance to shutdown
    shutil.rmtree(data_dir) # remove the temp data dir

if __name__ == '__main__':
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)
  parser = argparse.ArgumentParser(description='Tests Full Cluster Restarts across major version')
  parser.add_argument('--version.backwards', '-b', dest='backwards_version', metavar='V',
                      help='The elasticsearch version to upgrade from')
  parser.add_argument('--version.current', '-c', dest='current_version', metavar='V',
                      help='The elasticsearch version to upgrade to')
  parser.add_argument('--seed', '-s', dest='seed', metavar='N', type=int,
                      help='The random seed to use')
  parser.add_argument('--backwards.dir', '-d', dest='bwc_directory', default='backwards', metavar='dir',
                      help='The directory to the backwards compatibility sources')

  parser.add_argument('--tcp.port', '-p', dest='tcp_port', default=9300, metavar='port', type=int,
                      help='The port to use as the minimum port for TCP communication')
  parser.add_argument('--http.port', '-t', dest='http_port', default=9200, metavar='port', type=int,
                      help='The port to use as the minimum port for HTTP communication')

  parser.set_defaults(bwc_directory='backwards')
  parser.set_defaults(seed=int(time.time()))
  args = parser.parse_args()
  node_dir = args.bwc_directory
  current_version = args.current_version
  bwc_version = args.backwards_version
  seed = args.seed
  random.seed(seed)
  bwc_version, current_version = pick_random_upgrade_version(node_dir, bwc_version, current_version)
  tcp_port = args.tcp_port
  http_port = args.http_port
  try:
    full_cluster_restart(node_dir, current_version, bwc_version, tcp_port, http_port)
  except:
    logging.warn('REPRODUCE WITH: \n\t`python %s --version.backwards %s --version.current %s --seed %s --tcp.port %s --http.port %s`'
                   % (sys.argv[0], bwc_version, current_version, seed, tcp_port, http_port))
    raise
