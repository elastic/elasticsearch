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

import datetime
import traceback
import json
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import threading
import time

from http.client import HTTPConnection

LOG = os.environ.get('ES_SMOKE_TEST_PLUGINS_LOG', '/tmp/elasticsearch_smoke_test_plugins.log')

print('Logging to %s' % LOG)

if os.path.exists(LOG):
  raise RuntimeError('please remove old log %s first' % LOG)

try:
  JAVA_HOME = os.environ['JAVA7_HOME']
except KeyError:
  try:
    JAVA_HOME = os.environ['JAVA_HOME']
  except KeyError:
    raise RuntimeError("""
    Please set JAVA_HOME in the env before running release tool
    On OSX use: export JAVA_HOME=`/usr/libexec/java_home -v '1.7*'`""")

JAVA_ENV = 'export JAVA_HOME="%s" PATH="%s/bin:$PATH" JAVACMD="%s/bin/java"' % (JAVA_HOME, JAVA_HOME, JAVA_HOME)

try:
  # make sure mvn3 is used if mvn3 is available
  # some systems use maven 2 as default
  subprocess.check_output('mvn3 --version', shell=True, stderr=subprocess.STDOUT)
  MVN = 'mvn3'
except subprocess.CalledProcessError:
  MVN = 'mvn'

def log(msg):
  f = open(LOG, mode='ab')
  f.write(('\n'+msg).encode('utf-8'))
  f.close()

def run(command, quiet=False):
  log('%s: RUN: %s\n' % (datetime.datetime.now(), command))
  if os.system('%s >> %s 2>&1' % (command, LOG)):
    msg = '    FAILED: %s [see log %s]' % (command, LOG)
    if not quiet:
      print(msg)
    raise RuntimeError(msg)

def readServerOutput(p, startupEvent, failureEvent):
  try:
    while True:
      line = p.stdout.readline()
      if len(line) == 0:
        p.poll()
        if not startupEvent.isSet():
          failureEvent.set()
          startupEvent.set()
        print('ES: **process exit**\n')
        break
      line = line.decode('utf-8').rstrip()
      if line.endswith('started') and not startupEvent.isSet():
        startupEvent.set()
      print('ES: %s' % line)
  except:
    print()
    print('Exception reading Elasticsearch output:')
    traceback.print_exc()
    failureEvent.set()
    startupEvent.set()

if __name__ == '__main__':
  print('Build release bits...')

  run('%s; %s clean package -DskipTests' % (JAVA_ENV, MVN))

  for f in os.listdir('core/target/releases/'):
    if f.endswith('.tar.gz'):
      artifact = f
      break
  else:
    raise RuntimeError('could not find elasticsearch release under core/target/releases/')
  
  tmp_dir = tempfile.mkdtemp()
  p = None
  try:
    # Extract artifact:
    run('tar -xzf core/target/releases/%s -C %s' % (artifact, tmp_dir))
    es_install_dir = os.path.join(tmp_dir, artifact[:-7])
    es_plugin_path = os.path.join(es_install_dir, 'bin/plugin')
    installed_plugin_names = set()
    print('Find plugins:')
    for name in os.listdir('plugins'):
      if name not in ('target', 'pom.xml'):
        url = 'file://%s/plugins/%s/target/releases/elasticsearch-%s-2.0.0-SNAPSHOT.zip' % (os.path.abspath('.'), name, name)
        print('  install plugin %s...' % name)
        run('%s; %s --url %s -install %s' % (JAVA_ENV, es_plugin_path, url, name))
        installed_plugin_names.add(name)

    print('Start Elasticsearch')

    env = os.environ.copy()
    env['JAVA_HOME'] = JAVA_HOME
    env['PATH'] = '%s/bin:%s' % (JAVA_HOME, env['PATH'])
    env['JAVA_CMD'] = '%s/bin/java' % JAVA_HOME
    
    startupEvent = threading.Event()
    failureEvent = threading.Event()
    p = subprocess.Popen(('%s/bin/elasticsearch' % es_install_dir,
                          '-Des.node.name=smoke_tester',
                          '-Des.cluster.name=smoke_tester_cluster'
                          '-Des.discovery.zen.ping.multicast.enabled=false',
                          '-Des.script.inline=on',
                          '-Des.script.indexed=on'),
                         stdout = subprocess.PIPE,
                         stderr = subprocess.STDOUT,
                         env = env)
    thread = threading.Thread(target=readServerOutput, args=(p, startupEvent, failureEvent))
    thread.setDaemon(True)
    thread.start()

    startupEvent.wait(1200)
    if failureEvent.isSet():
      raise RuntimeError('ES failed to start')

    print('Confirm plugins are installed')
    conn = HTTPConnection('127.0.0.1', 9200, 20);
    conn.request('GET', '/_nodes?plugin=true&pretty=true')
    res = conn.getresponse()
    if res.status == 200:
      nodes = json.loads(res.read().decode("utf-8"))['nodes']
      for _, node in nodes.items():
        node_plugins = node['plugins']
        for node_plugin in node_plugins:
          plugin_name = node_plugin['name']
          if plugin_name not in installed_plugin_names:
            raise RuntimeError('Unexpeced plugin %s' % plugin_name)
          installed_plugin_names.remove(plugin_name)
      if len(installed_plugin_names) > 0:
        raise RuntimeError('Plugins not loaded %s' % installed_plugin_names)
    else:
      raise RuntimeError('Expected HTTP 200 but got %s' % res.status)
  finally:
    if p is not None:
      try:
        os.kill(p.pid, signal.SIGKILL)
      except ProcessLookupError:
        pass
    shutil.rmtree(tmp_dir)

