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

# Smoke-tests a release candidate
#
# 1. Downloads the tar.gz, deb, RPM and zip file from the staging URL
# 2. Verifies it's sha1 hashes and GPG signatures against the release key
# 3. Installs all official plugins
# 4. Starts one node for tar.gz and zip packages and checks:
#    -- if it runs with Java 1.8
#    -- if the build hash given is the one that is returned by the status response
#    -- if the build is a release version and not a snapshot version
#    -- if all plugins are loaded
#    -- if the status response returns the correct version
#
# USAGE:
#
# python3 -B ./dev-tools/smoke_test_rc.py --version 2.0.0-beta1 --hash bfa3e47
#
# to also test other plugins try run
#
# python3 -B ./dev-tools/smoke_test_rc.py --version 2.0.0-beta1 --hash bfa3e47 --plugins license,shield,watcher
#
# Note: Ensure the script is run from the elasticsearch top level directory
#
# For testing a release from sonatype try this:
#
# python3 -B dev-tools/smoke_test_rc.py --version 2.0.0-beta1 --hash bfa3e47 --fetch_url https://oss.sonatype.org/content/repositories/releases/
#

import argparse
import tempfile
import os
import signal
import shutil
import urllib
import urllib.request
import hashlib
import time
import socket
import json
import base64
from urllib.parse import urlparse

from prepare_release_candidate import run
from http.client import HTTPConnection

DEFAULT_PLUGINS = ["analysis-icu",
                   "analysis-kuromoji",
                   "analysis-phonetic",
                   "analysis-smartcn",
                   "analysis-stempel",
                   "discovery-azure",
                   "discovery-ec2",
                   "discovery-gce",
                   "ingest-attachment",
                   "ingest-geoip",
                   "lang-javascript",
                   "lang-python",
                   "mapper-attachments",
                   "mapper-murmur3",
                   "mapper-size",
                   "repository-azure",
                   "repository-gcs",
                   "repository-hdfs",
                   "repository-s3",
                   "store-smb"]

try:
  JAVA_HOME = os.environ['JAVA_HOME']
except KeyError:
  raise RuntimeError("""
  Please set JAVA_HOME in the env before running release tool
  On OSX use: export JAVA_HOME=`/usr/libexec/java_home -v '1.8*'`""")

def java_exe():
  path = JAVA_HOME
  return 'export JAVA_HOME="%s" PATH="%s/bin:$PATH" JAVACMD="%s/bin/java"' % (path, path, path)

def verify_java_version(version):
  s = os.popen('%s; java -version 2>&1' % java_exe()).read()
  if ' version "%s.' % version not in s:
    raise RuntimeError('got wrong version for java %s:\n%s' % (version, s))


def sha1(file):
  with open(file, 'rb') as f:
    return hashlib.sha1(f.read()).hexdigest()

def read_fully(file):
  with open(file, encoding='utf-8') as f:
     return f.read()


def wait_for_node_startup(es_dir, timeout=60, header={}):
  print('     Waiting until node becomes available for at most %s seconds' % timeout)
  for _ in range(timeout):
    conn = None
    try:
      time.sleep(1)
      host = get_host_from_ports_file(es_dir)
      conn = HTTPConnection(host, timeout=1)
      conn.request('GET', '/', headers=header)
      res = conn.getresponse()
      if res.status == 200:
        return True
    except IOError as e:
      pass
      #that is ok it might not be there yet
    finally:
      if conn:
        conn.close()
  return False

def download_and_verify(version, hash, files, base_url, plugins=DEFAULT_PLUGINS):
  print('Downloading and verifying release %s from %s' % (version, base_url))
  tmp_dir = tempfile.mkdtemp()
  try:
    downloaded_files = []
    print('  ' + '*' * 80)
    for file in files:
      name = os.path.basename(file)
      print('  Smoketest file: %s' % name)
      url = '%s/%s' % (base_url, file)
      print('  Downloading %s' % (url))
      artifact_path = os.path.join(tmp_dir, file)
      downloaded_files.append(artifact_path)
      current_artifact_dir = os.path.dirname(artifact_path)
      os.makedirs(current_artifact_dir)
      urllib.request.urlretrieve(url, os.path.join(tmp_dir, file))
      sha1_url = ''.join([url, '.sha1'])
      checksum_file = artifact_path + ".sha1"
      print('  Downloading %s' % (sha1_url))
      urllib.request.urlretrieve(sha1_url, checksum_file)
      print('  Verifying checksum %s' % (checksum_file))
      expected = read_fully(checksum_file)
      actual = sha1(artifact_path)
      if expected != actual :
        raise RuntimeError('sha1 hash for %s doesn\'t match %s != %s' % (name, expected, actual))
      gpg_url = ''.join([url, '.asc'])
      gpg_file =  artifact_path + ".asc"
      print('  Downloading %s' % (gpg_url))
      urllib.request.urlretrieve(gpg_url, gpg_file)
      print('  Verifying gpg signature %s' % (gpg_file))
      # here we create a temp gpg home where we download the release key as the only key into
      # when we verify the signature it will fail if the signed key is not in the keystore and that
      # way we keep the executing host unmodified since we don't have to import the key into the default keystore
      gpg_home_dir = os.path.join(current_artifact_dir, "gpg_home_dir")
      os.makedirs(gpg_home_dir, 0o700)
      run('gpg --homedir %s --keyserver pool.sks-keyservers.net --recv-key D88E42B4' % gpg_home_dir)
      run('cd %s && gpg --homedir %s --verify %s' % (current_artifact_dir, gpg_home_dir, os.path.basename(gpg_file)))
      print('  ' + '*' * 80)
      print()
    smoke_test_release(version, downloaded_files, hash, plugins)
    print('  SUCCESS')
  finally:
    shutil.rmtree(tmp_dir)

def get_host_from_ports_file(es_dir):
  return read_fully(os.path.join(es_dir, 'logs/http.ports')).splitlines()[0]

def smoke_test_release(release, files, expected_hash, plugins):
  for release_file in files:
    if not os.path.isfile(release_file):
      raise RuntimeError('Smoketest failed missing file %s' % (release_file))
    tmp_dir = tempfile.mkdtemp()
    if release_file.endswith('tar.gz'):
      run('tar -xzf %s -C %s' % (release_file, tmp_dir))
    elif release_file.endswith('zip'):
      run('unzip %s -d %s' % (release_file, tmp_dir))
    else:
      print('  Skip SmokeTest for [%s]' % release_file)
      continue # nothing to do here
    es_dir = os.path.join(tmp_dir, 'elasticsearch-%s' % (release))
    es_run_path = os.path.join(es_dir, 'bin/elasticsearch')
    print('  Smoke testing package [%s]' % release_file)
    es_plugin_path = os.path.join(es_dir, 'bin/elasticsearch-plugin')
    plugin_names = {}
    for plugin  in plugins:
      print('     Install plugin [%s]' % (plugin))
      run('%s; export ES_JAVA_OPTS="-Des.plugins.staging=%s"; %s %s %s' % (java_exe(), expected_hash, es_plugin_path, 'install -b', plugin))
      plugin_names[plugin] = True
    if 'x-pack' in plugin_names:
      headers = { 'Authorization' : 'Basic %s' % base64.b64encode(b"es_admin:foobar").decode("UTF-8") }
      es_shield_path = os.path.join(es_dir, 'bin/x-pack/users')
      print("     Install dummy shield user")
      run('%s; %s  useradd es_admin -r superuser -p foobar' % (java_exe(), es_shield_path))
    else:
      headers = {}
    print('  Starting elasticsearch deamon from [%s]' % es_dir)
    try:
      run('%s; %s -Enode.name=smoke_tester -Ecluster.name=prepare_release -Escript.inline=true -Escript.stored=true -Erepositories.url.allowed_urls=http://snapshot.test* %s -Epidfile=%s -Enode.portsfile=true'
          % (java_exe(), es_run_path, '-d', os.path.join(es_dir, 'es-smoke.pid')))
      if not wait_for_node_startup(es_dir, header=headers):
        print("elasticsearch logs:")
        print('*' * 80)
        logs = read_fully(os.path.join(es_dir, 'logs/prepare_release.log'))
        print(logs)
        print('*' * 80)
        raise RuntimeError('server didn\'t start up')
      try: # we now get / and /_nodes to fetch basic infos like hashes etc and the installed plugins
        host = get_host_from_ports_file(es_dir)
        conn = HTTPConnection(host, timeout=20)
        conn.request('GET', '/', headers=headers)
        res = conn.getresponse()
        if res.status == 200:
          version = json.loads(res.read().decode("utf-8"))['version']
          if release != version['number']:
            raise RuntimeError('Expected version [%s] but was [%s]' % (release, version['number']))
          if version['build_snapshot']:
            raise RuntimeError('Expected non snapshot version')
          if expected_hash != version['build_hash'].strip():
            raise RuntimeError('HEAD hash does not match expected [%s] but got [%s]' % (expected_hash, version['build_hash']))
          print('  Verify if plugins are listed in _nodes')
          conn.request('GET', '/_nodes?plugin=true&pretty=true', headers=headers)
          res = conn.getresponse()
          if res.status == 200:
            nodes = json.loads(res.read().decode("utf-8"))['nodes']
            for _, node in nodes.items():
              node_plugins = node['plugins']
              for node_plugin in node_plugins:
                if not plugin_names.get(node_plugin['name'].strip(), False):
                  raise RuntimeError('Unexpected plugin %s' % node_plugin['name'])
                del plugin_names[node_plugin['name']]
            if plugin_names:
              raise RuntimeError('Plugins not loaded %s' % list(plugin_names.keys()))

          else:
            raise RuntimeError('Expected HTTP 200 but got %s' % res.status)
        else:
          raise RuntimeError('Expected HTTP 200 but got %s' % res.status)
      finally:
        conn.close()
    finally:
      pid_path = os.path.join(es_dir, 'es-smoke.pid')
      if os.path.exists(pid_path): # try reading the pid and kill the node
        pid = int(read_fully(pid_path))
        os.kill(pid, signal.SIGKILL)
      shutil.rmtree(tmp_dir)
    print('  ' + '*' * 80)
    print()


def parse_list(string):
  return [x.strip() for x in string.split(',')]

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='SmokeTests a Release Candidate from S3 staging repo')
  parser.add_argument('--version', '-v', dest='version', default=None,
                      help='The Elasticsearch Version to smoke-tests', required=True)
  parser.add_argument('--hash', '-s', dest='hash', default=None, required=True,
                      help='The sha1 short hash of the git commit to smoketest')
  parser.add_argument('--plugins', '-p', dest='plugins', default=[], required=False, type=parse_list,
                      help='A list of additional plugins to smoketest')
  parser.add_argument('--fetch_url', '-u', dest='url', default=None,
                      help='Fetched from the specified URL')
  parser.set_defaults(hash=None)
  parser.set_defaults(plugins=[])
  parser.set_defaults(version=None)
  parser.set_defaults(url=None)
  args = parser.parse_args()
  plugins = args.plugins
  version = args.version
  hash = args.hash
  url = args.url
  files = [ x % {'version': version} for x in [
    'org/elasticsearch/distribution/tar/elasticsearch/%(version)s/elasticsearch-%(version)s.tar.gz',
    'org/elasticsearch/distribution/zip/elasticsearch/%(version)s/elasticsearch-%(version)s.zip',
    'org/elasticsearch/distribution/deb/elasticsearch/%(version)s/elasticsearch-%(version)s.deb',
    'org/elasticsearch/distribution/rpm/elasticsearch/%(version)s/elasticsearch-%(version)s.rpm'
  ]]
  verify_java_version('1.8')
  if url:
    download_url = url
  else:
    download_url = '%s/%s-%s' % ('http://download.elasticsearch.org/elasticsearch/staging', version, hash)
  download_and_verify(version, hash, files, download_url, plugins=DEFAULT_PLUGINS + plugins)



