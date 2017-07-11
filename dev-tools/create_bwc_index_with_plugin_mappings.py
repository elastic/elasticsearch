import create_bwc_index
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile

def fetch_version(version):
  logging.info('fetching ES version %s' % version)
  if subprocess.call([sys.executable, os.path.join(os.path.split(sys.argv[0])[0], 'get-bwc-version.py'), version]) != 0:
    raise RuntimeError('failed to download ES version %s' % version)

def create_index(plugin, mapping, docs):
  '''
  Creates a static back compat index (.zip) with mappings using fields defined in plugins.
  '''
  
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)

  tmp_dir = tempfile.mkdtemp()
  plugin_installed = False
  node = None
  try:
    data_dir = os.path.join(tmp_dir, 'data')
    repo_dir = os.path.join(tmp_dir, 'repo')
    logging.info('Temp data dir: %s' % data_dir)
    logging.info('Temp repo dir: %s' % repo_dir)

    version = '2.0.0'
    classifier = '%s-%s' %(plugin, version)
    index_name = 'index-%s' % classifier

    # Download old ES releases if necessary:
    release_dir = os.path.join('backwards', 'elasticsearch-%s' % version)
    if not os.path.exists(release_dir):
      fetch_version(version)

    create_bwc_index.install_plugin(version, release_dir, plugin)
    plugin_installed = True
    node = create_bwc_index.start_node(version, release_dir, data_dir, repo_dir, cluster_name=index_name)
    client = create_bwc_index.create_client()
    put_plugin_mappings(client, index_name, mapping, docs)
    create_bwc_index.shutdown_node(node)

    print('%s server output:\n%s' % (version, node.stdout.read().decode('utf-8')))
    node = None
    create_bwc_index.compress_index(classifier, tmp_dir, 'plugins/%s/src/test/resources/indices/bwc' %plugin)
  finally:
    if node is not None:
      create_bwc_index.shutdown_node(node)
    if plugin_installed:
      create_bwc_index.remove_plugin(version, release_dir, plugin)
    shutil.rmtree(tmp_dir)

def put_plugin_mappings(client, index_name, mapping, docs):
  client.indices.delete(index=index_name, ignore=404)
  logging.info('Create single shard test index')

  client.indices.create(index=index_name, body={
    'settings': {
      'number_of_shards': 1,
      'number_of_replicas': 0
    },
    'mappings': {
      'type': mapping
    }
  })
  health = client.cluster.health(wait_for_status='green', wait_for_relocating_shards=0)
  assert health['timed_out'] == False, 'cluster health timed out %s' % health

  logging.info('Indexing documents')
  for i in range(len(docs)):
    client.index(index=index_name, doc_type="type", id=str(i), body=docs[i])
  logging.info('Flushing index')
  client.indices.flush(index=index_name)

  logging.info('Running basic checks')
  count = client.count(index=index_name)['count']
  assert count == len(docs), "expected %d docs, got %d" %(len(docs), count)

def main():
  docs = [
    {
      "foo": "abc"
    },
    {
      "foo": "abcdef"
    },
    {
      "foo": "a"
    }
  ]

  murmur3_mapping = {
    'properties': {
      'foo': {
        'type': 'string',
        'fields': {
          'hash': {
            'type': 'murmur3'
          }
        }
      }
    }
  }

  create_index("mapper-murmur3", murmur3_mapping, docs)

  size_mapping = {
    '_size': {
      'enabled': True
    }
  }

  create_index("mapper-size", size_mapping, docs)

if __name__ == '__main__':
  main()

