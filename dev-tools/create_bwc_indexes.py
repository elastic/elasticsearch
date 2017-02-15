# ELASTICSEARCH CONFIDENTIAL
# __________________
#
#  [2014] Elasticsearch Incorporated. All Rights Reserved.
#
# NOTICE:  All information contained herein is, and remains
# the property of Elasticsearch Incorporated and its suppliers,
# if any.  The intellectual and technical concepts contained
# herein are proprietary to Elasticsearch Incorporated
# and its suppliers and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination of this information or reproduction of this material
# is strictly forbidden unless prior written permission is obtained
# from Elasticsearch Incorporated.

# Creates indices with old versions of elasticsearch. These indices are used by x-pack plugins like security
# to test if the import of metadata that is stored in elasticsearch indexes works correctly.
# This tool will start a node on port 9200/9300. If a node is already running on that port then the script will fail.
# Currently this script can only deal with versions >=2.0.0 and < 5.0. Needs more work for versions before or after.
#
# Run from x-plugins root directory like so:
# python3 ./dev-tools/create_bwc_indexes.py 2.3.4
# You can get any particular version of elasticsearch with:
# python3 ../elasticsearch/dev-tools/get-bwc-version.py 2.3.4
#
#
import argparse
import glob
import json
import logging
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import requests
import socket
import signal
from cmd import Cmd

DEFAULT_TRANSPORT_TCP_PORT = 9300
DEFAULT_HTTP_TCP_PORT = 9200

if sys.version_info[0] < 3:
  print('%s must use python 3.x (for the ES python client)' % sys.argv[0])

try:
  from elasticsearch import Elasticsearch
  from elasticsearch.exceptions import ConnectionError
  from elasticsearch.exceptions import TransportError
  from elasticsearch.exceptions import NotFoundError
  from elasticsearch.client import IndicesClient
except ImportError as e:
  print('Can\'t import elasticsearch please install `sudo pip3 install elasticsearch`')
  sys.exit(1)

def start_node(version, release_dir, data_dir):
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  result = sock.connect_ex(('localhost',DEFAULT_HTTP_TCP_PORT))
  if result == 0:
    raise Exception('Elasticsearch instance already running on port ' + str(DEFAULT_HTTP_TCP_PORT))
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  result = sock.connect_ex(('localhost',DEFAULT_TRANSPORT_TCP_PORT))
  if result == 0:
    raise Exception('Elasticsearch instance already running on port ' + str(DEFAULT_TRANSPORT_TCP_PORT))
  logging.info('Starting node from %s on port %s/%s, data_dir %s' % (release_dir, DEFAULT_TRANSPORT_TCP_PORT
                                                                    , DEFAULT_HTTP_TCP_PORT, data_dir))
  cluster_name = 'bwc_index_' + version
  if parse_version(version) < parse_version("5.0.0-alpha1"):
    prefix = '-Des.'
  else:
    prefix = '-E'
  cmd = [
  os.path.join(release_dir, 'bin/elasticsearch'),
  '%spath.data=%s' % (prefix, data_dir),
  '%spath.logs=logs' % prefix,
  '%scluster.name=%s' % (prefix, cluster_name),
  '%snetwork.host=localhost' % prefix,
  '%stransport.tcp.port=%s' % (prefix, DEFAULT_TRANSPORT_TCP_PORT), # not sure if we need to customize ports
  '%shttp.port=%s' % (prefix, DEFAULT_HTTP_TCP_PORT)
   ]

  return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def install_plugin(version, release_dir, plugin_name):
  args = [plugin_name]
  if parse_version(version) >= parse_version('2.2.0'):
    args = [plugin_name, '--batch']
  run_plugin(version, release_dir, 'install', args)

def remove_plugin(version, release_dir, plugin_name):
  # 5.0 doesn't like trying to remove a plugin that isn't installed so we
  # shouldn't try.
  if os.path.exists(os.path.join(release_dir, 'plugins', plugin_name)):
    run_plugin(version, release_dir, 'remove', [plugin_name])

def run_plugin(version, release_dir, plugin_cmd, args):
  if parse_version(version) < parse_version('5.0.0'):
    script = 'bin/plugin'
  else:
    script = 'bin/elasticsearch-plugin'
  cmd = [os.path.join(release_dir, script), plugin_cmd] + args
  subprocess.check_call(cmd)

def create_client():
  logging.info('Waiting for node to startup')
  for _ in range(0, 30):
    try:
      client = Elasticsearch([{'host': 'localhost', 'port': 9200, 'http_auth':'es_admin:0123456789'}])
      health = client.cluster.health(wait_for_nodes=1)
      return client
    except ConnectionError:
      logging.info('Not started yet...')
    time.sleep(1)
  assert False, 'Timed out waiting for node for %s seconds' % timeout

def wait_for_yellow(version, client, index):
  logging.info('Waiting for %s to be yellow' % index)
  # The health call below uses `params` because it the 5.x client doesn't
  # support wait_for_relocating_shards and the 2.x client doesn't support
  # wait_for_relocating_shards and we'd like to use the same client for both
  # versions.
  if parse_version(version) < parse_version('5.0.0'):
    health = client.cluster.health(wait_for_status='yellow', index=index, params={'wait_for_relocating_shards':0})
  else:
    health = client.cluster.health(wait_for_status='yellow', index=index, params={'wait_for_no_relocating_shards':'true'})
  assert health['timed_out'] == False, 'cluster health timed out %s' % health

# this adds a user bwc_test_role/9876543210, a role bwc_test_role and some documents the user has or has not access to
def generate_security_index(client, version):

  logging.info('Add a group')
  # don't know how to use python client with shield so use curl instead
  # add a user
  body = {
           "password" : "9876543210",
           "roles" : [ "bwc_test_role" ]
         }

  while True:
    response = requests.put('http://localhost:9200/_shield/user/bwc_test_user', auth=('es_admin', '0123456789'), data=json.dumps(body))
    logging.info('put user reponse: ' + response.text)
    if response.status_code == 200:
      break
    else:
      if 'service has not been started' in response.text:
        continue
      raise Exception('PUT http://localhost:9200/_shield/role/bwc_test_role did not succeed!')


  # add a role
  body = {
           "cluster": ["all"],
           "indices": [
             {
               "names": [ "index1", "index2" ],
               "privileges": ["all"],
               "query": "{\"match\": {\"title\": \"foo\"}}"
             }
           ],
           "run_as": [ "other_user" ]
         }
  if parse_version(version) < parse_version('5.0.0'):
    body['indices'][0]['fields'] = [ "title", "body" ]
  else:
    body['indices'][0]['field_security'] = { "grant": [ "title", "body" ] }
  # order of params in put role request is important, see https://github.com/elastic/x-plugins/issues/2606
  response = requests.put('http://localhost:9200/_shield/role/bwc_test_role', auth=('es_admin', '0123456789')
                           , data=json.dumps(body, sort_keys=True))
  logging.info('put user reponse: ' + response.text)
  if (response.status_code != 200) :
      raise Exception('PUT http://localhost:9200/_shield/role/bwc_test_role did not succeed!')

  client.index(index="index1", doc_type="doc", body={"title": "foo",
  "body": "bwc_test_user should be able to see this field",
  "secured_body": "bwc_test_user should not be able to see this field"})
  client.index(index="index1", doc_type="doc", body={"title": "bwc_test_user should not be able to see this document"})

  client.index(index="index2", doc_type="doc", body={"title": "foo",
  "body": "bwc_test_user should be able to see this field",
  "secured_body": "bwc_test_user should not be able to see this field"})
  client.index(index="index2", doc_type="doc", body={"title": "bwc_test_user should not be able to see this document"})

  client.index(index="index3", doc_type="doc", body={"title": "bwc_test_user should not see this index"})

  if parse_version(version) < parse_version('5.1.0'):
    logging.info("Adding a alias that starts with - so we can test against it")
    client.indices.put_alias(index='index3', name='-index3')

  wait_for_yellow(version, client, '.security')

# this adds a couple of watches and waits for the the watch_history to accumulate some results
def generate_watcher_index(client, version):
  logging.info('Adding a watch')
  body = {
    "trigger" : {
      "schedule": {
        "interval": "1s"
      }
    },
    "input" : {
      "search" : {
        "timeout": "100s",
        "request" : {
          "indices" : [ ".watches" ],
          "body" : {
            "query" : { "match_all" : {}},
            "size": 1
          },
        }
      }
    },
    "condition" : {
      "always" : {}
    },
    "throttle_period": "1s",
    "actions" : {
      "index_payload" : {
        "transform" : {
          "search" : {
            "request" : {
              "body" : { "size": 1, "query" : { "match_all" : {} }}
            },
            "timeout": "100s"
          }
        },
        "index" : {
          "index" : "bwc_watch_index",
          "doc_type" : "bwc_watch_type",
          "timeout": "100s"
        }
      }
    }
  }
  response = requests.put('http://localhost:9200/_watcher/watch/bwc_watch', auth=('es_admin', '0123456789'), data=json.dumps(body))
  logging.info('PUT watch response: ' + response.text)
  if (response.status_code != 201) :
      raise Exception('PUT http://localhost:9200/_watcher/watch/bwc_watch did not succeed!')

  logging.info('Adding a watch with "fun" throttle periods')
  body = {
    "trigger" : {
      "schedule": {
        "interval": "1s"
      }
    },
    "condition" : {
      "never" : {}
    },
    "throttle_period": "100s",
    "actions" : {
      "index_payload" : {
        "throttle_period": "100s",
        "transform" : {
          "search" : {
            "request" : {
              "body" : { "size": 1, "query" : { "match_all" : {} }}
            }
          }
        },
        "index" : {
          "index" : "bwc_watch_index",
          "doc_type" : "bwc_watch_type"
        }
      }
    }
  }
  response = requests.put('http://localhost:9200/_watcher/watch/bwc_throttle_period', auth=('es_admin', '0123456789'), data=json.dumps(body))
  logging.info('PUT watch response: ' + response.text)
  if (response.status_code != 201) :
      raise Exception('PUT http://localhost:9200/_watcher/watch/bwc_throttle_period did not succeed!')

  if parse_version(version) < parse_version('2.3.0'):
    logging.info('Skipping watch with a funny read timeout because email attachement is not supported by this version')
  else:
    logging.info('Adding a watch with a funny read timeout')
    body = {
      "trigger" : {
        "schedule": {
          "interval": "100s"
        }
      },
      "condition": {
        "never": {}
      },
      "actions": {
        "work": {
          "email": {
            "to": "email@domain.com",
            "subject": "Test Kibana PDF report",
            "attachments": {
              "test_report.pdf": {
                "http": {
                  "content_type": "application/pdf",
                  "request": {
                    "read_timeout": "100s",
                    "scheme": "https",
                    "host": "example.com",
                    "path":"{{ctx.metadata.report_url}}",
                    "port": 8443,
                    "auth": {
                      "basic": {
                        "username": "Aladdin",
                        "password": "open sesame"
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    response = requests.put('http://localhost:9200/_watcher/watch/bwc_funny_timeout', auth=('es_admin', '0123456789'), data=json.dumps(body))
    logging.info('PUT watch response: ' + response.text)
    if (response.status_code != 201) :
        raise Exception('PUT http://localhost:9200/_watcher/watch/bwc_funny_timeout did not succeed!')

  # wait to accumulate some watches
  logging.info('Waiting for watch results index to fill up...')
  wait_for_search(10, lambda: client.search(index="bwc_watch_index", body={"query": {"match_all": {}}}))
  if parse_version(version) < parse_version('5.0.0'):
    watcher_history_name = ".watch_history*"
  else:
    watcher_history_name = ".watcher-history*"
  wait_for_search(10, lambda: client.search(index=watcher_history_name, body={"query": {"match_all": {}}}))

  wait_for_yellow(version, client, '.watches')
  wait_for_yellow(version, client, watcher_history_name)
  wait_for_yellow(version, client, 'bwc_watch_index')

def wait_for_monitoring_index_to_fill(client, version):
  if parse_version(version) < parse_version('5.0.0'):
    monitoring_name = '.marvel-*'
  else:
    monitoring_name = '.monitoring-*'
  def wait_for_monitoring_to_index(doc_type, count):
    logging.info('Waiting for %s to have count(%s) = %s...' % (monitoring_name, doc_type, count))
    wait_for_search(count, lambda:
        client.search(index=monitoring_name, doc_type=doc_type, body={"query": {"match_all": {}}}))

  wait_for_monitoring_to_index('cluster_info', 1)
  if parse_version(version) >= parse_version('2.1.0'):
    wait_for_monitoring_to_index('node', 1)
  wait_for_monitoring_to_index('index_stats', 10)
  wait_for_monitoring_to_index('shards', 10)
  wait_for_monitoring_to_index('indices_stats', 3)
  wait_for_monitoring_to_index('node_stats', 3)
  wait_for_monitoring_to_index('cluster_state', 3)

  wait_for_yellow(version, client, monitoring_name)

def wait_for_search(required_count, searcher):
  for attempt in range(1, 31):
    try:
      response = searcher()
      logging.info('(' + str(attempt) + ') Got ' + str(response['hits']['total']) + ' hits and want ' + str(required_count) + '...')
      if response['hits']['total'] >= required_count:
        return
    except NotFoundError:
      logging.info('(' + str(attempt) + ') Not found, retrying')
    time.sleep(1)
  logger.error("Ran out of retries")
  raise "Ran out of retries"

def compress_index(version, tmp_dir, output_dir):
  compress(tmp_dir, output_dir, 'x-pack-%s.zip' % version, 'data')

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
  parser.add_argument('--output-dir', '-o', default='plugin/src/test/resources/indices/bwc/',
                      help='The directory to write the zipped index into')

  cfg = parser.parse_args()

  if not os.path.exists(cfg.output_dir):
    parser.error('Output directory does not exist: %s' % cfg.output_dir)

  if not cfg.versions:
    # --all
    for bwc_index in glob.glob(os.path.join(cfg.output_dir, 'x-pack-*.zip')):
      version = os.path.basename(bwc_index)[len('x-pack-'):-len('.zip')]
      cfg.versions.append(version)

  return cfg

def shutdown_node(node):
  logging.info('Shutting down node with pid %d', node.pid)
  node.terminate()
  node.wait()

def parse_version(version):
  import re
  splitted = re.split('[.-]', version)
  if len(splitted) == 3:
    splitted = splitted + ['GA']
  splitted = [s.lower() for s in splitted]
  assert len(splitted) == 4;
  return splitted

def run(command, env_vars=None):
  if env_vars:
    for key, value in env_vars.items():
      os.putenv(key, value)
  logging.info('*** Running: %s%s%s' % (COLOR_OK, command, COLOR_END))
  if os.system(command):
    raise RuntimeError('    FAILED: %s' % (command))

assert parse_version('1.2.3') < parse_version('2.1.0')
assert parse_version('1.2.3') < parse_version('1.2.4')
assert parse_version('1.1.0') < parse_version('1.2.0')

# console colors
COLOR_OK = '\033[92m'
COLOR_END = '\033[0m'
COLOR_FAIL = '\033[91m'

def main():
  logging.basicConfig(format='[%(levelname)s] [%(asctime)s] %(message)s', level=logging.INFO,
                      datefmt='%Y-%m-%d %I:%M:%S %p')
  logging.getLogger('elasticsearch').setLevel(logging.ERROR)
  logging.getLogger('urllib3').setLevel(logging.WARN)
  cfg = parse_config()
  for version in cfg.versions:
    logging.info('--> Creating x-pack index for %s' % version)

    # setup for starting nodes
    release_dir = os.path.join(cfg.releases_dir, 'elasticsearch-%s' % version)
    if not os.path.exists(release_dir):
      raise RuntimeError('ES version %s does not exist in %s' % (version, cfg.releases_dir))
    tmp_dir = tempfile.mkdtemp()
    data_dir = os.path.join(tmp_dir, 'data')
    logging.info('Temp data dir: %s' % data_dir)
    node = None

    try:
      if parse_version(version) < parse_version('5.0.0'):
        # Remove old plugins just in case any are around
        remove_plugin(version, release_dir, 'marvel-agent')
        remove_plugin(version, release_dir, 'watcher')
        remove_plugin(version, release_dir, 'shield')
        remove_plugin(version, release_dir, 'license')
        # Remove the shield config too before fresh install
        run('rm -rf %s' %(os.path.join(release_dir, 'config/shield')))
        # Install plugins we'll need
        install_plugin(version, release_dir, 'license')
        install_plugin(version, release_dir, 'shield')
        install_plugin(version, release_dir, 'watcher')
        install_plugin(version, release_dir, 'marvel-agent')
        # define the stuff we need to make the esadmin user
        users_script = os.path.join(release_dir, 'bin/shield/esusers')
        esadmin_role = 'admin'
      else:
        # Remove old plugins just in case any are around
        remove_plugin(version, release_dir, 'x-pack')
        # Remove the x-pack config too before fresh install
        run('rm -rf %s' %(os.path.join(release_dir, 'config/x-pack')))
        # Install plugins we'll need
        install_plugin(version, release_dir, 'x-pack')
        # define the stuff we need to make the esadmin user
        users_script = os.path.join(release_dir, 'bin/x-pack/users')
        esadmin_role = 'superuser'

      # create admin
      run('%s  useradd es_admin -r %s -p 0123456789' %
          (users_script, esadmin_role))
      node = start_node(version, release_dir, data_dir)

      # create a client that authenticates as es_admin
      client = create_client()
      if parse_version(version) < parse_version('2.3.0'):
        logging.info('Version is ' + version + ' but shield supports native realm only from 2.3.0 on. Nothing to do for Shield.')
      else:
        generate_security_index(client, version)
      generate_watcher_index(client, version)
      wait_for_monitoring_index_to_fill(client, version)

      shutdown_node(node)
      node = None
      compress_index(version, tmp_dir, cfg.output_dir)
    finally:

      if node is not None:
        # This only happens if we've hit an exception:
        shutdown_node(node)
      shutil.rmtree(tmp_dir)

if __name__ == '__main__':
  try:
    main()
  except KeyboardInterrupt:
    logging.info('Caught keyboard interrupt, exiting...')
    sys.exit(signal.SIGTERM) # exit code
