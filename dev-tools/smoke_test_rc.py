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
# 1. Downloads the tar.gz & zip file from the staging URL
# 2. Verifies it's sha1 hashes and GPG signatures against the release key
# 3. Installs all official plugins
# 4. Starts one node an checks:
#    -- if it runs with Java 1.7
#    -- if the build hash given is the one that is returned by the status response
#    -- if the build is a release version and not a snapshot version
#    -- if all plugins are loaded
#    -- if the status reponse returns the correct version
#
# USAGE:
#
# python3 -B ./dev-tools/smoke_tests_rc.py --version 2.0.0-beta1 --hash bfa3e47
#
# Note: Ensure the script is run from the root directory
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

from prepare_release_candidate import run
from http.client import HTTPConnection

DEFAULT_PLUGINS = ["analysis-icu",
                   "analysis-kuromoji",
                   "analysis-phonetic",
                   "analysis-smartcn",
                   "analysis-stempel",
                   "cloud-aws",
                   "cloud-azure",
                   "cloud-gce",
                   "delete-by-query",
                   "discovery-multicast",
                   "lang-javascript",
                   "lang-python",
                   "mapper-murmur3",
                   "mapper-size"]

#TODO install commercial plugins too?

try:
  JAVA_HOME = os.environ['JAVA_HOME']
except KeyError:
  raise RuntimeError("""
  Please set JAVA_HOME in the env before running release tool
  On OSX use: export JAVA_HOME=`/usr/libexec/java_home -v '1.7*'`""")

try:
  JAVA_HOME = os.environ['JAVA7_HOME']
except KeyError:
  pass #no JAVA7_HOME - we rely on JAVA_HOME

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

def wait_for_node_startup(host='127.0.0.1', port=9200, timeout=15):
  for _ in range(timeout):
    conn = HTTPConnection(host, port, timeout)
    try:
      print('   Waiting until node becomes available for 1 second')
      time.sleep(1)
      print('   Check if node is available')
      conn.request('GET', '')
      res = conn.getresponse()
      if res.status == 200:
        return True
    except socket.error as e:
      print("Failed while waiting for node - Exception: [%s]" % e)
      #that is ok it might not be there yet
    finally:
      conn.close()

  return False

def download_and_verify(version, hash, files, base_url='http://download.elasticsearch.org/elasticsearch/staging', plugins=DEFAULT_PLUGINS):
  base_url = '%s/%s-%s' % (base_url, version, hash)
  print('Downloading and verifying release %s from %s' % (version, base_url))
  tmp_dir = '/Users/simon/projects/release_stageing/foo' # tempfile.mkdtemp()
  try:
    downloaded_files = []
    for file in files:
      name = os.path.basename(file)
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
      gpg_home_dir = os.path.join(current_artifact_dir, "gpg_home_dir")
      os.makedirs(gpg_home_dir, 0o700)
      run('gpg --homedir %s --keyserver pgp.mit.edu --recv-key D88E42B4' % gpg_home_dir)
      run('cd %s && gpg --homedir %s --verify %s' % (current_artifact_dir, gpg_home_dir, os.path.basename(gpg_file)))

    smoke_test_release(version, downloaded_files, hash, plugins)
    print('  SUCCESS')
  finally:
    shutil.rmtree(tmp_dir)

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
      log('Skip SmokeTest for [%s]' % release_file)
      continue # nothing to do here
    es_run_path = os.path.join(tmp_dir, 'elasticsearch-%s' % (release), 'bin/elasticsearch')
    print('  Smoke testing package [%s]' % release_file)
    es_plugin_path = os.path.join(tmp_dir, 'elasticsearch-%s' % (release),'bin/plugin')
    plugin_names = {}
    for plugin  in plugins:
      print('  Install plugin [%s]' % (plugin))
      run('%s; %s -Des.plugins.staging=true %s %s' % (java_exe(), es_plugin_path, 'install', plugin))
      plugin_names[plugin] = True
    print('  Starting elasticsearch deamon from [%s]' % os.path.join(tmp_dir, 'elasticsearch-%s' % release))
    run('%s; %s -Des.node.name=smoke_tester -Des.cluster.name=prepare_release -Des.script.inline=on -Des.script.indexed=on -Des.repositories.url.allowed_urls=http://snapshot.test* %s -Des.pidfile=%s'
        % (java_exe(), es_run_path, '-d', os.path.join(tmp_dir, 'elasticsearch-%s' % (release), 'es-smoke.pid')))
    conn = HTTPConnection('127.0.0.1', 9200, 20);
    wait_for_node_startup()
    try:
      try:
        conn.request('GET', '')
        res = conn.getresponse()
        if res.status == 200:
          version = json.loads(res.read().decode("utf-8"))['version']
          if release != version['number']:
            raise RuntimeError('Expected version [%s] but was [%s]' % (release, version['number']))
          if version['build_snapshot']:
            raise RuntimeError('Expected non snapshot version')
          if expected_hash.startswith(version['build_hash'].strip()):
            raise RuntimeError('HEAD hash does not match expected [%s] but got [%s]' % (expected_hash, version['build_hash']))
          print('  Verify if plugins are listed in _nodes')
          conn.request('GET', '/_nodes?plugin=true&pretty=true')
          res = conn.getresponse()
          if res.status == 200:
            nodes = json.loads(res.read().decode("utf-8"))['nodes']
            for _, node in nodes.items():
              node_plugins = node['plugins']
              for node_plugin in node_plugins:
                if not plugin_names.get(node_plugin['name'], False):
                  raise RuntimeError('Unexpeced plugin %s' % node_plugin['name'])
                del plugin_names[node_plugin['name']]
            if plugin_names:
              raise RuntimeError('Plugins not loaded %s' % list(plugin_names.keys()))

          else:
            raise RuntimeError('Expected HTTP 200 but got %s' % res.status)
        else:
          raise RuntimeError('Expected HTTP 200 but got %s' % res.status)
      finally:
        pid_path = os.path.join(tmp_dir, 'elasticsearch-%s' % (release), 'es-smoke.pid')
        pid = int(read_fully(pid_path))
        os.kill(pid, signal.SIGKILL)
    finally:
      conn.close()
    shutil.rmtree(tmp_dir)


if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='SmokeTests a Release Candidate from S3 staging repo')
  parser.add_argument('--version', '-v', dest='version', default=None,
                      help='The Elasticsearch Version to smoke-tests', required=True)
  parser.add_argument('--hash', '-s', dest='hash', default=None, required=True,
                      help='The sha1 short hash of the git commit to smoketest')
  parser.set_defaults(hash=None)
  parser.set_defaults(version=None)
  args = parser.parse_args()
  version = args.version
  hash = args.hash
  files = [
    'org/elasticsearch/distribution/tar/elasticsearch/2.0.0-beta1/elasticsearch-2.0.0-beta1.tar.gz',
    'org/elasticsearch/distribution/zip/elasticsearch/2.0.0-beta1/elasticsearch-2.0.0-beta1.zip'
  ]
  verify_java_version('1.7')
  download_and_verify(version, hash, files)



