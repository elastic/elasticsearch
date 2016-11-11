import create_bwc_index
import logging
import os
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
  Creates a static back compat data directory (.zip) with a repository, stored script and a pipeline.
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

    version = '5.0.0'
    name = 'custom-cluster-state-parts-%s' % version

    # Download old ES releases if necessary:
    release_dir = os.path.join('backwards', 'elasticsearch-%s' % version)
    if not os.path.exists(release_dir):
      fetch_version(version)

    node = create_bwc_index.start_node(version, release_dir, data_dir, repo_dir, cluster_name=name)
    client = create_bwc_index.create_client()

    create_cluster_state(client)
    create_bwc_index.shutdown_node(node)
    print('%s server output:\n%s' % (version, node.stdout.read().decode('utf-8')))
    node = None
    create_bwc_index.compress(tmp_dir, '../core/src/test/resources/indices/bwc', '%s.zip' % name, 'data')
  finally:
    if node is not None:
      create_bwc_index.shutdown_node(node)
    shutil.rmtree(tmp_dir)

def create_cluster_state(client):
  client.snapshot.create_repository(repository= 'repo1', body= {'type': 'url', 'settings' : {'url': 'http://snapshot.test'}})
  client.snapshot.create_repository(repository= 'repo2', body= {'type': 'url', 'settings' : {'url': 'http://snapshot.test'}})
  client.ingest.put_pipeline(id = 'pipeline1', body = {"processors":[]})
  client.ingest.put_pipeline(id = 'pipeline2', body = {"processors":[]})
  client.transport.perform_request('PUT', '/_scripts/painless/script1', body={"script": "1 + 1"})
  client.transport.perform_request('PUT', '/_scripts/painless/script2', body={"script": "1 + 2"})

if __name__ == '__main__':
  main()

