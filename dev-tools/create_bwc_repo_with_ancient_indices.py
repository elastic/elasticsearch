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
  Creates a back compat index (.zip) using v0.20 and then creates a snapshot of it using v1.1
  '''
  
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)

  tmp_dir = tempfile.mkdtemp()
  try:
    data_dir = os.path.join(tmp_dir, 'data')
    logging.info('Temp data dir: %s' % data_dir)

    first_version = '0.20.6'
    second_version = '1.1.2'
    index_name = 'index-%s-and-%s' % (first_version, second_version)

    # Download old ES releases if necessary:
    release_dir = os.path.join('backwards', 'elasticsearch-%s' % first_version)
    if not os.path.exists(release_dir):
      fetch_version(first_version)

    node = create_bwc_index.start_node(first_version, release_dir, data_dir, cluster_name=index_name)
    client = create_bwc_index.create_client()

    # Creates the index & indexes docs w/ first_version:
    create_bwc_index.generate_index(client, first_version, index_name)

    # Make sure we write segments:
    flush_result = client.indices.flush(index=index_name)
    if not flush_result['ok']:
      raise RuntimeError('flush failed: %s' % str(flush_result))

    create_bwc_index.shutdown_node(node)
    print('%s server output:\n%s' % (first_version, node.stdout.read().decode('utf-8')))
    node = None

    release_dir = os.path.join('backwards', 'elasticsearch-%s' % second_version)
    if not os.path.exists(release_dir):
      fetch_version(second_version)

    # Now use second_version to snapshot the index:
    node = create_bwc_index.start_node(second_version, release_dir, data_dir, cluster_name=index_name)
    client = create_bwc_index.create_client()

    repo_dir = os.path.join(tmp_dir, 'repo')
    create_bwc_index.snapshot_index(client, second_version, repo_dir)
    create_bwc_index.shutdown_node(node)
    print('%s server output:\n%s' % (second_version, node.stdout.read().decode('utf-8')))

    create_bwc_index.compress(tmp_dir, "src/test/resources/indices/bwc", 'unsupportedrepo-%s.zip' % first_version, 'repo')

    node = None
  finally:
    if node is not None:
      create_bwc_index.shutdown_node(node)
    shutil.rmtree(tmp_dir)
    
if __name__ == '__main__':
  main()
  
