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
  Creates a static back compat index (.zip) with mixed 0.20 (Lucene 3.x) and 0.90 (Lucene 4.x) segments. 
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

    first_version = '0.20.6'
    second_version = '0.90.6'
    index_name = 'index-%s-and-%s' % (first_version, second_version)

    # Download old ES releases if necessary:
    release_dir = os.path.join('backwards', 'elasticsearch-%s' % first_version)
    if not os.path.exists(release_dir):
      fetch_version(first_version)

    node = create_bwc_index.start_node(first_version, release_dir, data_dir, repo_dir, cluster_name=index_name)
    client = create_bwc_index.create_client()

    # Creates the index & indexes docs w/ first_version:
    create_bwc_index.generate_index(client, first_version, index_name)

    # Make sure we write segments:
    flush_result = client.indices.flush(index=index_name)
    if not flush_result['ok']:
      raise RuntimeError('flush failed: %s' % str(flush_result))

    segs = client.indices.segments(index=index_name)
    shards = segs['indices'][index_name]['shards']
    if len(shards) != 1:
      raise RuntimeError('index should have 1 shard but got %s' % len(shards))

    first_version_segs = shards['0'][0]['segments'].keys()

    create_bwc_index.shutdown_node(node)
    print('%s server output:\n%s' % (first_version, node.stdout.read().decode('utf-8')))
    node = None

    release_dir = os.path.join('backwards', 'elasticsearch-%s' % second_version)
    if not os.path.exists(release_dir):
      fetch_version(second_version)

    # Now also index docs with second_version:
    node = create_bwc_index.start_node(second_version, release_dir, data_dir, repo_dir, cluster_name=index_name)
    client = create_bwc_index.create_client()

    # If we index too many docs, the random refresh/flush causes the ancient segments to be merged away:
    num_docs = 10
    create_bwc_index.index_documents(client, index_name, 'doc', num_docs)

    # Make sure we get a segment:
    flush_result = client.indices.flush(index=index_name)
    if not flush_result['ok']:
      raise RuntimeError('flush failed: %s' % str(flush_result))

    # Make sure we see mixed segments (it's possible Lucene could have "accidentally" merged away the first_version segments):
    segs = client.indices.segments(index=index_name)
    shards = segs['indices'][index_name]['shards']
    if len(shards) != 1:
      raise RuntimeError('index should have 1 shard but got %s' % len(shards))

    second_version_segs = shards['0'][0]['segments'].keys()
    #print("first: %s" % first_version_segs)
    #print("second: %s" % second_version_segs)

    for segment_name in first_version_segs:
      if segment_name in second_version_segs:
        # Good: an ancient version seg "survived":
        break
    else:
      raise RuntimeError('index has no first_version segs left')

    for segment_name in second_version_segs:
      if segment_name not in first_version_segs:
        # Good: a second_version segment was written
        break
    else:
      raise RuntimeError('index has no second_version segs left')

    create_bwc_index.shutdown_node(node)
    print('%s server output:\n%s' % (second_version, node.stdout.read().decode('utf-8')))
    node = None
    create_bwc_index.compress_index('%s-and-%s' % (first_version, second_version), tmp_dir, 'core/src/test/resources/org/elasticsearch/action/admin/indices/upgrade')
  finally:
    if node is not None:
      create_bwc_index.shutdown_node(node)
    shutil.rmtree(tmp_dir)
    
if __name__ == '__main__':
  main()
  
