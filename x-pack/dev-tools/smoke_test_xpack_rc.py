# Smoke-tests a x-pack release candidate
#
# 1. Downloads the zip file from the staging URL
# 3. Installs x-pack plugin
# 4. Starts one node for zip package and checks:
#    -- if x-pack plugin is loaded
#    -- checks xpack info page, if response returns correct version and feature set info
#
# USAGE:
#
# python3 -B ./dev-tools/smoke_test_rc.py --version 5.0.0-beta1 --hash bfa3e47
#

import argparse
import tempfile
import os
import signal
import shutil
import urllib
import urllib.request
import time
import json
import base64
from http.client import HTTPConnection

# in case of debug, uncomment
# HTTPConnection.debuglevel = 4

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

def read_fully(file):
  with open(file, encoding='utf-8') as f:
     return f.read()

def wait_for_node_startup(es_dir, timeout=60, headers={}):
  print('     Waiting until node becomes available for at most %s seconds' % timeout)
  for _ in range(timeout):
    conn = None
    try:
      time.sleep(1)
      host = get_host_from_ports_file(es_dir)
      conn = HTTPConnection(host, timeout=1)
      conn.request('GET', '/', headers=headers)
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

def download_release(version, release_hash, url):
  print('Downloading release %s from %s' % (version, url))
  tmp_dir = tempfile.mkdtemp()
  try:
    downloaded_files = []
    print('  ' + '*' * 80)
    print('  Downloading %s' % (url))
    file = ('elasticsearch-%s.zip' % version)
    artifact_path = os.path.join(tmp_dir, file)
    downloaded_files.append(artifact_path)
    urllib.request.urlretrieve(url, os.path.join(tmp_dir, file))
    print('  ' + '*' * 80)
    print()
    
    smoke_test_release(version, downloaded_files, release_hash)
    print('  SUCCESS')
  finally:
    shutil.rmtree(tmp_dir)

def get_host_from_ports_file(es_dir):
  return read_fully(os.path.join(es_dir, 'logs/http.ports')).splitlines()[0]

def smoke_test_release(release, files, release_hash):
  for release_file in files:
    if not os.path.isfile(release_file):
      raise RuntimeError('Smoketest failed missing file %s' % (release_file))
    tmp_dir = tempfile.mkdtemp()
    run('unzip %s -d %s' % (release_file, tmp_dir))
    
    es_dir = os.path.join(tmp_dir, 'elasticsearch-%s' % (release))
    es_run_path = os.path.join(es_dir, 'bin/elasticsearch')
    
    print('  Smoke testing package [%s]' % release_file)
    es_plugin_path = os.path.join(es_dir, 'bin/elasticsearch-plugin')
    
    print('     Install xpack [%s]')
    run('%s; ES_JAVA_OPTS="-Des.plugins.staging=%s" %s install -b x-pack' % (java_exe(), release_hash, es_plugin_path))
    headers = { 'Authorization' : 'Basic %s' % base64.b64encode(b"es_admin:foobar").decode("UTF-8") }
    es_shield_path = os.path.join(es_dir, 'bin/x-pack/users')
    
    print("     Install dummy shield user")
    run('%s; %s  useradd es_admin -r superuser -p foobar' % (java_exe(), es_shield_path))
    
    print('  Starting elasticsearch daemon from [%s]' % es_dir)
    try:
      run('%s; %s -Enode.name=smoke_tester -Ecluster.name=prepare_release -Erepositories.url.allowed_urls=http://snapshot.test* %s -Epidfile=%s -Enode.portsfile=true'
          % (java_exe(), es_run_path, '-d', os.path.join(es_dir, 'es-smoke.pid')))
      if not wait_for_node_startup(es_dir, headers=headers):
        print("elasticsearch logs:")
        print('*' * 80)
        logs = read_fully(os.path.join(es_dir, 'logs/prepare_release.log'))
        print(logs)
        print('*' * 80)
        raise RuntimeError('server didn\'t start up')
      try: # we now get / and /_nodes to fetch basic infos like hashes etc and the installed plugins
        host = get_host_from_ports_file(es_dir)
        conn = HTTPConnection(host, timeout=20)
        
        # check if plugin is loaded
        conn.request('GET', '/_nodes/plugins?pretty=true', headers=headers)
        res = conn.getresponse()
        if res.status == 200:
          nodes = json.loads(res.read().decode("utf-8"))['nodes']
          for _, node in nodes.items():
            node_plugins = node['plugins']
            for node_plugin in node_plugins:
              if node_plugin['name'] != 'x-pack':
                raise RuntimeError('Unexpected plugin %s, expected x-pack only' % node_plugin['name'])
        else:
          raise RuntimeError('Expected HTTP 200 but got %s' % res.status)

        # check if license is the default one
        # also sleep for few more seconds, as the initial license generation might take some time
        time.sleep(5)
        conn.request('GET', '/_xpack', headers=headers)
        res = conn.getresponse()
        if res.status == 200:
          xpack = json.loads(res.read().decode("utf-8"))
          if xpack['license']['type'] != 'trial':
            raise RuntimeError('expected license type to be trial, was %s' % xpack['license']['type'])
          if xpack['license']['mode'] != 'trial':
            raise RuntimeError('expected license mode to be trial, was %s' % xpack['license']['mode'])
          if xpack['license']['status'] != 'active':
            raise RuntimeError('expected license status to be active, was %s' % xpack['license']['active'])
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

# console colors
COLOR_OK = '\033[92m'
COLOR_END = '\033[0m'

def run(command, env_vars=None):
  if env_vars:
    for key, value in env_vars.items():
      os.putenv(key, value)
  print('*** Running: %s%s%s' % (COLOR_OK, command, COLOR_END))
  if os.system(command):
    raise RuntimeError('    FAILED: %s' % (command))

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='SmokeTests a Release Candidate from S3 staging repo')
  parser.add_argument('--version', '-v', dest='version', default=None,
                      help='The Elasticsearch Version to smoke-tests', required=True)
  parser.add_argument('--hash', '-r', dest='hash', default=None, required=True,
                      help='The sha1 short hash of the release git commit to smoketest')
  parser.add_argument('--fetch_url', '-u', dest='url', default=None,
                      help='Fetched from the specified URL')
  parser.set_defaults(hash=None)
  parser.set_defaults(version=None)
  parser.set_defaults(url=None)
  args = parser.parse_args()
  version = args.version
  hash = args.hash
  url = args.url
  verify_java_version('1.8')
  if url:
    download_url = url
  else:
    download_url = 'https://staging.elastic.co/%s-%s/downloads/elasticsearch/elasticsearch-%s.zip' % (version, hash, version)
  download_release(version, hash, download_url)

