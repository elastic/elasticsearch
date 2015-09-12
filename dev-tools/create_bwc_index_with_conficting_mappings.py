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

def main():
  '''
  Creates a static back compat index (.zip) with conflicting mappings.
  '''
  
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)

  tmp_dir = tempfile.mkdtemp()
  try:
    data_dir = os.path.join(tmp_dir, 'data')
    repo_dir = os.path.join(tmp_dir, 'repo')
    logging.info('Temp data dir: %s' % data_dir)
    logging.info('Temp repo dir: %s' % repo_dir)

    version = '1.7.0'
    classifier = 'conflicting-mappings-%s' % version
    index_name = 'index-%s' % classifier

    # Download old ES releases if necessary:
    release_dir = os.path.join('backwards', 'elasticsearch-%s' % version)
    if not os.path.exists(release_dir):
      fetch_version(version)

    node = create_bwc_index.start_node(version, release_dir, data_dir, repo_dir, cluster_name=index_name)
    client = create_bwc_index.create_client()

    put_conflicting_mappings(client, index_name)
    create_bwc_index.shutdown_node(node)
    print('%s server output:\n%s' % (version, node.stdout.read().decode('utf-8')))
    node = None
    create_bwc_index.compress_index(classifier, tmp_dir, 'core/src/test/resources/org/elasticsearch/action/admin/indices/upgrade')
  finally:
    if node is not None:
      create_bwc_index.shutdown_node(node)
    shutil.rmtree(tmp_dir)

def put_conflicting_mappings(client, index_name):
  client.indices.delete(index=index_name, ignore=404)
  logging.info('Create single shard test index')

  mappings = {}
  # backwardcompat test for conflicting mappings, see #11857
  mappings['x'] = {
    'analyzer': 'standard',
    "properties": {
      "foo": {
        "type": "string"
      }
    }
  }
  mappings['y'] = {
    'analyzer': 'standard',
    "properties": {
      "foo": {
        "type": "date"
      }
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
  create_bwc_index.index_documents(client, index_name, 'doc', num_docs)
  logging.info('Running basic asserts on the data added')
  create_bwc_index.run_basic_asserts(client, index_name, 'doc', num_docs)

if __name__ == '__main__':
  main()
  
