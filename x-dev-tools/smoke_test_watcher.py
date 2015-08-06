import datetime
import traceback
import json
import os
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import xml.dom.minidom

from http.client import HTTPSConnection
from http.client import HTTPConnection
import sys


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
    f.write(('\n' + msg).encode('utf-8'))
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

def send_request(conn, method, path, body=None):
    conn.request(method, path, body)
    res = conn.getresponse()
    if 200 > res.status > 300:
        raise RuntimeError('Expected HTTP status code between 200 and 299 but got %s' % res.status)
    return json.loads(res.read().decode("utf-8"))

supported_es_version = ['2.0.0-SNAPSHOT']

conn = HTTPSConnection('oss.sonatype.org')
conn.request('GET', 'https://oss.sonatype.org/content/repositories/snapshots/org/elasticsearch/distribution/elasticsearch-tar/2.0.0-beta1-SNAPSHOT/maven-metadata.xml')
res = conn.getresponse()
dom = xml.dom.minidom.parseString(res.read().decode("utf-8"))
snapshotVersion = dom.getElementsByTagName("value")[0].firstChild.nodeValue
snapshot_url = 'https://oss.sonatype.org/content/repositories/snapshots/org/elasticsearch/distribution/elasticsearch-tar/2.0.0-beta1-SNAPSHOT/elasticsearch-tar-%s.tar.gz' % snapshotVersion
conn.close()

print("Using the following url for the snapshot version: %s" % snapshot_url)
version_url_matrix = [
    {'version': '2.0.0-beta1-SNAPSHOT', 'host' : 'oss.sonatype.org', 'url': snapshot_url}
]

test_watch = """
{
  "trigger": {
    "schedule": {
      "interval": "1s"
    }
  },
  "input": {
    "http": {
      "request": {
        "host": "localhost",
        "port": 9200,
        "path": "/_cluster/health"
      }
    }
  },
  "condition": {
    "compare": {
      "ctx.payload.status": {
        "eq": "yellow"
      }
    }
  },
  "actions": {
    "log": {
      "logging": {
        "text": "executed at {{ctx.execution_time}}"
      }
    }
  }
}
"""

watch_history_query = """
{
  "query": {
    "term": {
      "watch_id": {
        "value": "cluster_health_watch"
      }
    }
  }
}
"""

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('Specify the version of Watcher to be tested as an argument.')
        print('For example: `python3 x-dev-tools/smoke_test_watcher.py 2.0.0-beta1-SNAPSHOT`')
        exit()

    expected_watcher_version = sys.argv[1]
    print('Smoke testing Watcher version [%s] with the following es versions [%s]' % (expected_watcher_version, supported_es_version))

    print('Building x-plugins (skipping tests)...')
    #run('%s; %s clean package -DskipTests' % (JAVA_ENV, MVN))

    license_artifact = None
    for dirname, dirnames, filenames in os.walk('license/plugin/target/releases/'):
        for f in filenames:
            print(os.path.join(dirname, f))
            if f.endswith('.zip'):
                license_artifact = os.path.abspath(os.path.join(dirname, f))
                break

    if license_artifact is None:
        raise RuntimeError('could not find license release under license/plugin/target/releases/')

    watcher_artifact = None
    for dirname, dirnames, filenames in os.walk('watcher/target/releases/'):
        for f in filenames:
            print(os.path.join(dirname, f))
            if f.endswith('.zip'):
                watcher_artifact = os.path.abspath(os.path.join(dirname, f))
                break

    if watcher_artifact is None:
        raise RuntimeError('could not find Watcher release under watcher/target/releases/')

    base_tmp_dir = tempfile.mkdtemp() + '/watcher_smoker/'
    os.makedirs(base_tmp_dir)
    try:
        for version_url in version_url_matrix:
            es_version = version_url['version']
            print('Testing watcher [%s] with elasticsearch [%s]' % (expected_watcher_version, es_version))

            version_tmp_dir = '%s%s/' % (base_tmp_dir, es_version)
            os.makedirs(version_tmp_dir)
            download = version_tmp_dir + 'elasticsearch-%s.tar.gz' % (es_version)

            conn = HTTPSConnection(version_url['host'])
            conn.request('GET', version_url['url'])
            print('downloading %s to %s' % (version_url['url'], download))
            resp = conn.getresponse()
            data = resp.read()
            with open(download, 'wb') as f:
                f.write(data)
            conn.close()
            print('Downloading elasticsearch [%s]' % es_version)
            run('tar -xzf %s -C %s' % (download, version_tmp_dir))

            es_dir = version_tmp_dir + 'elasticsearch-%s/' % (es_version)
            print('Installing License plugin...')
            url = 'file://%s' % license_artifact
            run('%s; %s install %s --url %s' % (JAVA_ENV, es_dir + 'bin/plugin', 'license', url))

            url = 'file://%s' % watcher_artifact
            print('Installing Watcher plugin...')
            run('%s; %s install %s --url %s' % (JAVA_ENV, es_dir + 'bin/plugin', 'watcher', url))

            p = None
            try:
                print('Starting Elasticsearch...')

                env = os.environ.copy()
                env['JAVA_HOME'] = JAVA_HOME
                env['PATH'] = '%s/bin:%s' % (JAVA_HOME, env['PATH'])
                env['JAVA_CMD'] = '%s/bin/java' % JAVA_HOME

                startupEvent = threading.Event()
                failureEvent = threading.Event()
                p = subprocess.Popen(('%s/bin/elasticsearch' % es_dir,
                                      '-Des.node.name=watcher_smoke_tester',
                                      '-Des.cluster.name=watcher_smoke_tester'
                                      '-Des.discovery.zen.ping.multicast.enabled=false',
                                      '-Des.script.inline=on',
                                      '-Des.script.indexed=on'),
                                     stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT,
                                     env=env)
                thread = threading.Thread(target=readServerOutput, args=(p, startupEvent, failureEvent))
                thread.setDaemon(True)
                thread.start()

                startupEvent.wait(5000)
                if failureEvent.isSet():
                    raise RuntimeError('ES failed to start')

                conn = HTTPConnection('127.0.0.1', 9200, 20)
                # TODO: fix version in watcher info api!
                # print('> Checking watcher info api...')
                # version = send_request(conn, 'GET', '/_watcher')['version']['name']
                # if version != expected_watcher_version:
                #     raise RuntimeError('Expected Watcher version %s but got %s' % (expected_watcher_version, version))
                # print('> Successful!')

                print('> Checking watcher stats api...')
                watcher_state = send_request(conn, 'GET', '/_watcher/stats')['watcher_state']
                # we're too fast, lets wait and retry:
                if watcher_state == 'starting':
                    time.sleep(5)
                    watcher_state =  send_request(conn, 'GET', '/_watcher/stats')['watcher_state']
                if watcher_state != 'started':
                    raise RuntimeError('Expected watcher_state started but got %s' % watcher_state)

                watch_count =  send_request(conn, 'GET', '/_watcher/stats')['watch_count']
                if watch_count != 0:
                    raise RuntimeError('Expected watcher_count 0 but got %s' % watch_count)
                print('> Successful!')

                print('> Checking put watch api...')
                res_body = send_request(conn, 'PUT', '/_watcher/watch/cluster_health_watch', test_watch)
                if res_body['_version'] != 1:
                    raise RuntimeError('Expected watch _version 1 but got %s' % res_body['_version'])
                if res_body['created'] != True:
                    raise RuntimeError('Expected watch created True but got %s' % res_body['created'])

                watch_count = send_request(conn, 'GET', '/_watcher/stats')['watch_count']
                if watch_count != 1:
                    raise RuntimeError('Expected watcher_count 1 but got %s' % watch_count)
                print('> Successful!')

                print('> Checking if watch actually fires...')
                time.sleep(5)
                send_request(conn, 'GET', '/.watch_history-*/_refresh')
                hit_count = send_request(conn, 'GET', '/.watch_history-*/_search')['hits']['total']
                if hit_count == 0:
                    raise RuntimeError('Expected hits.total 1 or higher but got %s' % hit_count)
                print('> Added test watch triggered %s history records...' % hit_count)
                print('> Successful!')

                print('> Checking delete watch api...')
                found = send_request(conn, 'DELETE', '/_watcher/watch/cluster_health_watch')['found']
                if (found == False):
                    raise RuntimeError('Expected found to be True but got %s' % found)
                print('> Successful!')
                print('> Smoke tester ran succesful with elastic version [%s]!' % es_version)
                conn.close()
            finally:
                if p is not None:
                    try:
                        os.kill(p.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
    finally:
        shutil.rmtree(base_tmp_dir)

    print('> Smoke tester has successfully completed')
    print('> The following es version were test [%s] with Watcher version [%s]' % (supported_es_version, expected_watcher_version))
