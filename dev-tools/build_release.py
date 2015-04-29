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

import re
import tempfile
import shutil
import os
import datetime
import json
import time
import sys
import argparse
import hmac
import urllib
import fnmatch
import socket
import urllib.request
import subprocess

from http.client import HTTPConnection
from http.client import HTTPSConnection


""" 
 This tool builds a release from the a given elasticsearch branch.
 In order to execute it go in the top level directory and run:
   $ python3 dev_tools/build_release.py --branch 0.90 --publish --remote origin

 By default this script runs in 'dry' mode which essentially simulates a release. If the
 '--publish' option is set the actual release is done. The script takes over almost all
 steps necessary for a release from a high level point of view it does the following things:

  - run prerequisit checks ie. check for Java 1.7 being presend or S3 credentials available as env variables
  - detect the version to release from the specified branch (--branch) or the current branch
  - creates a release branch & updates pom.xml and Version.java to point to a release version rather than a snapshot
  - builds the artifacts and runs smoke-tests on the build zip & tar.gz files
  - commits the new version and merges the release branch into the source branch
  - creates a tag and pushes the commit to the specified origin (--remote)
  - publishes the releases to Sonatype and S3

Once it's done it will print all the remaining steps.

 Prerequisites:
    - Python 3k for script execution
    - Boto for S3 Upload ($ apt-get install python-boto)
    - RPM for RPM building ($ apt-get install rpm)
    - S3 keys exported via ENV variables (AWS_ACCESS_KEY_ID,  AWS_SECRET_ACCESS_KEY)
    - GPG data exported via ENV variables (GPG_KEY_ID, GPG_PASSPHRASE, optionally GPG_KEYRING)
    - S3 target repository via ENV variables (S3_BUCKET_SYNC_TO, optionally S3_BUCKET_SYNC_FROM)
"""
env = os.environ

PLUGINS = [('license', 'elasticsearch/license/latest'),
           ('bigdesk', 'lukas-vlcek/bigdesk'),
           ('paramedic', 'karmi/elasticsearch-paramedic'),
           ('segmentspy', 'polyfractal/elasticsearch-segmentspy'),
           ('inquisitor', 'polyfractal/elasticsearch-inquisitor'),
           ('head', 'mobz/elasticsearch-head')]

LOG = env.get('ES_RELEASE_LOG', '/tmp/elasticsearch_release.log')

def log(msg):
  log_plain('\n%s' % msg)

def log_plain(msg):
  f = open(LOG, mode='ab')
  f.write(msg.encode('utf-8'))
  f.close()

def run(command, quiet=False):
  log('%s: RUN: %s\n' % (datetime.datetime.now(), command))
  if os.system('%s >> %s 2>&1' % (command, LOG)):
    msg = '    FAILED: %s [see log %s]' % (command, LOG)
    if not quiet:
      print(msg)
    raise RuntimeError(msg)

try:
  JAVA_HOME = env['JAVA_HOME']
except KeyError:
  raise RuntimeError("""
  Please set JAVA_HOME in the env before running release tool
  On OSX use: export JAVA_HOME=`/usr/libexec/java_home -v '1.7*'`""")

try:
  JAVA_HOME = env['JAVA7_HOME']
except KeyError:
  pass #no JAVA7_HOME - we rely on JAVA_HOME


try:
  # make sure mvn3 is used if mvn3 is available
  # some systems use maven 2 as default
  subprocess.check_output('mvn3 --version', shell=True, stderr=subprocess.STDOUT)
  MVN = 'mvn3'
except subprocess.CalledProcessError:
  MVN = 'mvn'

def java_exe():
  path = JAVA_HOME
  return 'export JAVA_HOME="%s" PATH="%s/bin:$PATH" JAVACMD="%s/bin/java"' % (path, path, path)

def verify_java_version(version):
  s = os.popen('%s; java -version 2>&1' % java_exe()).read()
  if ' version "%s.' % version not in s:
    raise RuntimeError('got wrong version for java %s:\n%s' % (version, s))

# Verifies the java version. We guarantee that we run with Java 1.7
# If 1.7 is not available fail the build!
def verify_mvn_java_version(version, mvn):
  s = os.popen('%s; %s --version 2>&1' % (java_exe(), mvn)).read()
  if 'Java version: %s' % version not in s:
    raise RuntimeError('got wrong java version for %s %s:\n%s' % (mvn, version, s))

# Returns the hash of the current git HEAD revision
def get_head_hash():
  return os.popen(' git rev-parse --verify HEAD 2>&1').read().strip()

# Returns the hash of the given tag revision
def get_tag_hash(tag):
  return os.popen('git show-ref --tags %s --hash 2>&1' % (tag)).read().strip()

# Returns the name of the current branch
def get_current_branch():
  return os.popen('git rev-parse --abbrev-ref HEAD  2>&1').read().strip()

verify_java_version('1.7') # we require to build with 1.7
verify_mvn_java_version('1.7', MVN)

# Utility that returns the name of the release branch for a given version
def release_branch(version):
  return 'release_branch_%s' % version

# runs get fetch on the given remote
def fetch(remote):
  run('git fetch %s' % remote)

# Creates a new release branch from the given source branch
# and rebases the source branch from the remote before creating
# the release branch. Note: This fails if the source branch
# doesn't exist on the provided remote.
def create_release_branch(remote, src_branch, release):
  run('git checkout %s' % src_branch)
  run('git pull --rebase %s %s' % (remote, src_branch))
  run('git checkout -b %s' % (release_branch(release)))


# Reads the given file and applies the
# callback to it. If the callback changed
# a line the given file is replaced with
# the modified input.
def process_file(file_path, line_callback):
  fh, abs_path = tempfile.mkstemp()
  modified = False
  with open(abs_path,'w', encoding='utf-8') as new_file:
    with open(file_path, encoding='utf-8') as old_file:
      for line in old_file:
        new_line = line_callback(line)
        modified = modified or (new_line != line)
        new_file.write(new_line)
  os.close(fh)
  if modified:
    #Remove original file
    os.remove(file_path)
    #Move new file
    shutil.move(abs_path, file_path)
    return True
  else:
    # nothing to do - just remove the tmp file
    os.remove(abs_path)
    return False

# Walks the given directory path (defaults to 'docs')
# and replaces all 'coming[$version]' tags with
# 'added[$version]'. This method only accesses asciidoc files.
def update_reference_docs(release_version, path='docs'):
  pattern = 'coming[%s' % (release_version)
  replacement = 'added[%s' % (release_version)
  pending_files = []
  def callback(line):
    return line.replace(pattern, replacement)
  for root, _, file_names in os.walk(path):
    for file_name in fnmatch.filter(file_names, '*.asciidoc'):
      full_path = os.path.join(root, file_name)
      if process_file(full_path, callback):
        pending_files.append(os.path.join(root, file_name))
  return pending_files

# Moves the pom.xml file from a snapshot to a release
def remove_maven_snapshot(pom, release):
  pattern = '<version>%s-SNAPSHOT</version>' % (release)
  replacement = '<version>%s</version>' % (release)
  def callback(line):
    return line.replace(pattern, replacement)
  process_file(pom, callback)

# Moves the Version.java file from a snapshot to a release
def remove_version_snapshot(version_file, release):
  # 1.0.0.Beta1 -> 1_0_0_Beta1
  release = release.replace('.', '_')
  pattern = 'new Version(V_%s_ID, true' % (release)
  replacement = 'new Version(V_%s_ID, false' % (release)
  def callback(line):
    return line.replace(pattern, replacement)
  process_file(version_file, callback)

# Stages the given files for the next git commit
def add_pending_files(*files):
  for file in files:
    run('git add %s' % (file))

# Executes a git commit with 'release [version]' as the commit message
def commit_release(release):
  run('git commit -m "release [%s]"' % release)

def commit_feature_flags(release):
    run('git commit -m "Update Documentation Feature Flags [%s]"' % release)

def tag_release(release):
  run('git tag -a v%s -m "Tag release version %s"' % (release, release))

def run_mvn(*cmd):
  for c in cmd:
    run('%s; %s %s' % (java_exe(), MVN, c))

def build_release(run_tests=False, dry_run=True, cpus=1, bwc_version=None):
  target = 'deploy'
  if dry_run:
    target = 'package'
  if run_tests:
    run_mvn('clean',
            'test -Dtests.jvms=%s -Des.node.mode=local' % (cpus),
            'test -Dtests.jvms=%s -Des.node.mode=network' % (cpus))
  if bwc_version:
      print('Running Backwards compatibility tests against version [%s]' % (bwc_version))
      run_mvn('clean', 'test -Dtests.filter=@backwards -Dtests.bwc.version=%s -Dtests.bwc=true -Dtests.jvms=1' % bwc_version)
  run_mvn('clean test-compile -Dforbidden.test.signatures="org.apache.lucene.util.LuceneTestCase\$AwaitsFix @ Please fix all bugs before release"')
  gpg_args = '-Dgpg.key="%s" -Dgpg.passphrase="%s" -Ddeb.sign=true' % (env.get('GPG_KEY_ID'), env.get('GPG_PASSPHRASE'))
  if env.get('GPG_KEYRING'):
    gpg_args += ' -Dgpg.keyring="%s"' % env.get('GPG_KEYRING')
  run_mvn('clean %s -DskipTests %s' % (target, gpg_args))
  success = False
  try:
    run_mvn('-DskipTests rpm:rpm %s' % (gpg_args))
    success = True
  finally:
    if not success:
      print("""
  RPM Bulding failed make sure "rpm" tools are installed.
  Use on of the following commands to install:
    $ brew install rpm # on OSX
    $ apt-get install rpm # on Ubuntu et.al
  """)

# Uses the github API to fetch open tickets for the given release version
# if it finds any tickets open for that version it will throw an exception
def ensure_no_open_tickets(version):
  version = "v%s" % version
  conn = HTTPSConnection('api.github.com')
  try:
    log('Checking for open tickets on Github for version %s' % version)
    log('Check if node is available')
    conn.request('GET', '/repos/elastic/elasticsearch/issues?state=open&labels=%s' % version, headers= {'User-Agent' : 'Elasticsearch version checker'})
    res = conn.getresponse()
    if res.status == 200:
      issues = json.loads(res.read().decode("utf-8"))
      if issues:
        urls = []
        for issue in issues:
          urls.append(issue['html_url'])
        raise RuntimeError('Found open issues for release version %s:\n%s' % (version, '\n'.join(urls)))
      else:
        log("No open issues found for version %s" % version)
    else:
      raise RuntimeError('Failed to fetch issue list from Github for release version %s' % version)
  except socket.error as e:
    log("Failed to fetch issue list from Github for release version %s' % version - Exception: [%s]" % (version, e))
    #that is ok it might not be there yet
  finally:
    conn.close()

def wait_for_node_startup(host='127.0.0.1', port=9200,timeout=15):
  for _ in range(timeout):
    conn = HTTPConnection(host, port, timeout)
    try:
      log('Waiting until node becomes available for 1 second')
      time.sleep(1)
      log('Check if node is available')
      conn.request('GET', '')
      res = conn.getresponse()
      if res.status == 200:
        return True
    except socket.error as e:
      log("Failed while waiting for node - Exception: [%s]" % e)
      #that is ok it might not be there yet
    finally:
      conn.close()

  return False

# Ensures we are using a true Lucene release, not a snapshot build:
def verify_lucene_version():
  s = open('pom.xml', encoding='utf-8').read()
  if 'download.elastic.co/lucenesnapshots' in s:
    raise RuntimeError('pom.xml contains download.elastic.co/lucenesnapshots repository: remove that before releasing')

  m = re.search(r'<lucene.version>(.*?)</lucene.version>', s)
  if m is None:
    raise RuntimeError('unable to locate lucene.version in pom.xml')
  lucene_version = m.group(1)

  m = re.search(r'<lucene.maven.version>(.*?)</lucene.maven.version>', s)
  if m is None:
    raise RuntimeError('unable to locate lucene.maven.version in pom.xml')
  lucene_maven_version = m.group(1)
  if lucene_version != lucene_maven_version:
    raise RuntimeError('pom.xml is still using a snapshot release of lucene (%s): cutover to a real lucene release before releasing' % lucene_maven_version)
    
# Checks the pom.xml for the release version.
# This method fails if the pom file has no SNAPSHOT version set ie.
# if the version is already on a release version we fail.
# Returns the next version string ie. 0.90.7
def find_release_version(src_branch):
  run('git checkout %s' % src_branch)
  with open('pom.xml', encoding='utf-8') as file:
    for line in file:
      match = re.search(r'<version>(.+)-SNAPSHOT</version>', line)
      if match:
        return match.group(1)
    raise RuntimeError('Could not find release version in branch %s' % src_branch)

def artifact_names(release, path = ''):
  return [os.path.join(path, 'elasticsearch-%s.%s' % (release, t)) for t in ['deb', 'tar.gz', 'zip']]

def get_artifacts(release):
  common_artifacts = artifact_names(release, 'target/releases/')
  for f in common_artifacts:
    if not os.path.isfile(f):
      raise RuntimeError('Could not find required artifact at %s' % f)
  rpm = os.path.join('target/rpm/elasticsearch/RPMS/noarch/', 'elasticsearch-%s-1.noarch.rpm' % release)
  if os.path.isfile(rpm):
    log('RPM [%s] contains: ' % rpm)
    run('rpm -pqli %s' % rpm)
    # this is an oddness of RPM that is attches -1 so we have to rename it
    renamed_rpm = os.path.join('target/rpm/elasticsearch/RPMS/noarch/', 'elasticsearch-%s.noarch.rpm' % release)
    shutil.move(rpm, renamed_rpm)
    common_artifacts.append(renamed_rpm)
  else:
    raise RuntimeError('Could not find required artifact at %s' % rpm)
  return common_artifacts

# Checks the jar files in each package
# Barfs if any of the package jar files differ
def check_artifacts_for_same_jars(artifacts):
  jars = []
  for file in artifacts:
    if file.endswith('.zip'):
      jars.append(subprocess.check_output("unzip -l %s  | grep '\.jar$' | awk -F '/' '{ print $NF }' | sort" % file, shell=True))
    if file.endswith('.tar.gz'):
      jars.append(subprocess.check_output("tar tzvf %s  | grep '\.jar$' | awk -F '/' '{ print $NF }' | sort" % file, shell=True))
    if file.endswith('.rpm'):
      jars.append(subprocess.check_output("rpm -pqli %s | grep '\.jar$' | awk -F '/' '{ print $NF }' | sort" % file, shell=True))
    if file.endswith('.deb'):
      jars.append(subprocess.check_output("dpkg -c %s   | grep '\.jar$' | awk -F '/' '{ print $NF }' | sort" % file, shell=True))
  if len(set(jars)) != 1:
    raise RuntimeError('JAR contents of packages are not the same, please check the package contents. Use [unzip -l], [tar tzvf], [dpkg -c], [rpm -pqli] to inspect')

# Generates sha1 checsums for all files
# and returns the checksum files as well
# as the given files in a list
def generate_checksums(files):
  res = []
  for release_file in files:
    directory = os.path.dirname(release_file)
    file = os.path.basename(release_file)
    checksum_file = '%s.sha1.txt' % file

    if os.system('cd %s; shasum %s > %s' % (directory, file, checksum_file)):
      raise RuntimeError('Failed to generate checksum for file %s' % release_file)
    res = res + [os.path.join(directory, checksum_file), release_file]
  return res

def download_and_verify(release, files, plugins=None, base_url='https://download.elastic.co/elasticsearch/elasticsearch'):
  print('Downloading and verifying release %s from %s' % (release, base_url))
  tmp_dir = tempfile.mkdtemp()
  try:
    downloaded_files = []
    for file in files:
      name = os.path.basename(file)
      url = '%s/%s' % (base_url, name)
      abs_file_path = os.path.join(tmp_dir, name)
      print('  Downloading %s' % (url))
      downloaded_files.append(abs_file_path)
      urllib.request.urlretrieve(url, abs_file_path)
      url = ''.join([url, '.sha1.txt'])
      checksum_file = os.path.join(tmp_dir, ''.join([abs_file_path, '.sha1.txt']))
      urllib.request.urlretrieve(url, checksum_file)
      print('  Verifying checksum %s' % (checksum_file))
      run('cd %s && sha1sum -c %s' % (tmp_dir, os.path.basename(checksum_file)))
    smoke_test_release(release, downloaded_files, get_tag_hash('v%s' % release), plugins)
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
    for name, plugin  in plugins:
      print('  Install plugin [%s] from [%s]' % (name, plugin))
      run('%s; %s %s %s' % (java_exe(), es_plugin_path, '-install', plugin))
      plugin_names[name] = True

    if release.startswith("0.90."):
      background = '' # 0.90.x starts in background automatically
    else:
      background = '-d'
    print('  Starting elasticsearch deamon from [%s]' % os.path.join(tmp_dir, 'elasticsearch-%s' % release))
    run('%s; %s -Des.node.name=smoke_tester -Des.cluster.name=prepare_release -Des.discovery.zen.ping.multicast.enabled=false -Des.script.inline=on -Des.script.indexed=on %s'
         % (java_exe(), es_run_path, background))
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
          if version['build_hash'].strip() !=  expected_hash:
            raise RuntimeError('HEAD hash does not match expected [%s] but got [%s]' % (expected_hash, version['build_hash']))
          print('  Running REST Spec tests against package [%s]' % release_file)
          run_mvn('test -Dtests.cluster=%s -Dtests.class=*.*RestTests' % ("127.0.0.1:9300"))
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
        conn.request('POST', '/_cluster/nodes/_local/_shutdown')
        time.sleep(1) # give the node some time to shut down
        if conn.getresponse().status != 200:
          raise RuntimeError('Expected HTTP 200 but got %s on node shutdown' % res.status)

    finally:
      conn.close()
    shutil.rmtree(tmp_dir)

def merge_tag_push(remote, src_branch, release_version, dry_run):
  run('git checkout %s' %  src_branch)
  run('git merge %s' %  release_branch(release_version))
  run('git tag v%s' % release_version)
  if not dry_run:
    run('git push %s %s' % (remote, src_branch)) # push the commit
    run('git push %s v%s' % (remote, release_version)) # push the tag
  else:
    print('  dryrun [True] -- skipping push to remote %s' % remote)

def publish_artifacts(artifacts, base='elasticsearch/elasticsearch', dry_run=True):
  location = os.path.dirname(os.path.realpath(__file__))
  for artifact in artifacts:
    if dry_run:
      print('Skip Uploading %s to Amazon S3' % artifact)
    else:
      print('Uploading %s to Amazon S3' % artifact)
      # requires boto to be installed but it is not available on python3k yet so we use a dedicated tool
      run('python %s/upload-s3.py --file %s ' % (location, os.path.abspath(artifact)))

def publish_repositories(version, dry_run=True):
  if dry_run:
    print('Skipping package repository update')
  else:
    print('Triggering repository update - calling dev-tools/build_repositories.sh %s' % version)
    # src_branch is a version like 1.5/1.6/2.0/etc.. so we can use this
    run('dev-tools/build_repositories.sh %s' % src_branch)

def print_sonatype_notice():
  settings = os.path.join(os.path.expanduser('~'), '.m2/settings.xml')
  if os.path.isfile(settings):
     with open(settings, encoding='utf-8') as settings_file:
       for line in settings_file:
         if line.strip() == '<id>sonatype-nexus-snapshots</id>':
           # moving out - we found the indicator no need to print the warning
           return
  print("""
    NOTE: No sonatype settings detected, make sure you have configured
    your sonatype credentials in '~/.m2/settings.xml':

    <settings>
    ...
    <servers>
      <server>
        <id>sonatype-nexus-snapshots</id>
        <username>your-jira-id</username>
        <password>your-jira-pwd</password>
      </server>
      <server>
        <id>sonatype-nexus-staging</id>
        <username>your-jira-id</username>
        <password>your-jira-pwd</password>
      </server>
    </servers>
    ...
  </settings>
  """)

def check_s3_credentials():
  if not env.get('AWS_ACCESS_KEY_ID', None) or not env.get('AWS_SECRET_ACCESS_KEY', None):
    raise RuntimeError('Could not find "AWS_ACCESS_KEY_ID" / "AWS_SECRET_ACCESS_KEY" in the env variables please export in order to upload to S3')

def check_gpg_credentials():
  if not env.get('GPG_KEY_ID', None) or not env.get('GPG_PASSPHRASE', None):
    raise RuntimeError('Could not find "GPG_KEY_ID" / "GPG_PASSPHRASE" in the env variables please export in order to sign the packages (also make sure that GPG_KEYRING is set when not in ~/.gnupg)')

def check_command_exists(name, cmd):
  try:
    subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
  except subprocess.CalledProcessError:
    raise RuntimeError('Could not run command %s - please make sure it is installed' % (name))

VERSION_FILE = 'src/main/java/org/elasticsearch/Version.java'
POM_FILE = 'pom.xml'

# we print a notice if we can not find the relevant infos in the ~/.m2/settings.xml
print_sonatype_notice()

# finds the highest available bwc version to test against
def find_bwc_version(release_version, bwc_dir='backwards'):
  log('  Lookup bwc version in directory [%s]' % bwc_dir)
  bwc_version = None
  if os.path.exists(bwc_dir) and os.path.isdir(bwc_dir):
    max_version = [int(x) for x in release_version.split('.')]
    for dir in os.listdir(bwc_dir):
      if os.path.isdir(os.path.join(bwc_dir, dir)) and dir.startswith('elasticsearch-'):
        version = [int(x) for x in dir[len('elasticsearch-'):].split('.')]
        if version < max_version: # bwc tests only against smaller versions
          if (not bwc_version) or version > [int(x) for x in bwc_version.split('.')]:
            bwc_version = dir[len('elasticsearch-'):]
    log('  Using bwc version [%s]' % bwc_version)
  else:
    log('  bwc directory [%s] does not exists or is not a directory - skipping' % bwc_dir)
  return bwc_version

def ensure_checkout_is_clean(branchName):
  # Make sure no local mods:
  s = subprocess.check_output('git diff --shortstat', shell=True)
  if len(s) > 0:
    raise RuntimeError('git diff --shortstat is non-empty: got:\n%s' % s)

  # Make sure no untracked files:
  s = subprocess.check_output('git status', shell=True).decode('utf-8', errors='replace')
  if 'Untracked files:' in s:
    raise RuntimeError('git status shows untracked files: got:\n%s' % s)

  # Make sure we are on the right branch (NOTE: a bit weak, since we default to current branch):
  if 'On branch %s' % branchName not in s:
    raise RuntimeError('git status does not show branch %s: got:\n%s' % (branchName, s))

  # Make sure we have all changes from origin:
  if 'is behind' in s:
    raise RuntimeError('git status shows not all changes pulled from origin; try running "git pull origin %s": got:\n%s' % (branchName, s))

  # Make sure we no local unpushed changes (this is supposed to be a clean area):
  if 'is ahead' in s:
    raise RuntimeError('git status shows local commits; try running "git fetch origin", "git checkout %s", "git reset --hard origin/%s": got:\n%s' % (branchName, branchName, s))

# Checks all source files for //NORELEASE comments
def check_norelease(path='src'):
  pattern = re.compile(r'\bnorelease\b', re.IGNORECASE)
  for root, _, file_names in os.walk(path):
    for file_name in fnmatch.filter(file_names, '*.java'):
      full_path = os.path.join(root, file_name)
      line_number = 0
      with open(full_path, 'r', encoding='utf-8') as current_file:
        for line in current_file:
          line_number = line_number + 1
          if pattern.search(line):
            raise RuntimeError('Found //norelease comment in %s line %s' % (full_path, line_number))


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Builds and publishes a Elasticsearch Release')
  parser.add_argument('--branch', '-b', metavar='RELEASE_BRANCH', default=get_current_branch(),
                       help='The branch to release from. Defaults to the current branch.')
  parser.add_argument('--cpus', '-c', metavar='1', default=1,
                       help='The number of cpus to use for running the test. Default is [1]')
  parser.add_argument('--skiptests', '-t', dest='tests', action='store_false',
                      help='Skips tests before release. Tests are run by default.')
  parser.set_defaults(tests=True)
  parser.add_argument('--remote', '-r', metavar='origin', default='origin',
                      help='The remote to push the release commit and tag to. Default is [origin]')
  parser.add_argument('--publish', '-d', dest='dryrun', action='store_false',
                      help='Publishes the release. Disable by default.')
  parser.add_argument('--smoke', '-s', dest='smoke', default='',
                      help='Smoke tests the given release')
  parser.add_argument('--bwc', '-w', dest='bwc', metavar='backwards', default='backwards',
                      help='Backwards compatibility version path to use to run compatibility tests against')

  parser.set_defaults(dryrun=True)
  parser.set_defaults(smoke=None)
  args = parser.parse_args()
  bwc_path = args.bwc
  src_branch = args.branch
  remote = args.remote
  run_tests = args.tests
  dry_run = args.dryrun
  cpus = args.cpus
  build = not args.smoke
  smoke_test_version = args.smoke

  if os.path.exists(LOG):
    raise RuntimeError('please remove old release log %s first' % LOG)

  check_gpg_credentials()
  check_command_exists('gpg', 'gpg --version')
  check_command_exists('expect', 'expect -v')
  
  if not dry_run:
    check_s3_credentials()
    check_command_exists('createrepo', 'createrepo --version')
    check_command_exists('s3cmd', 's3cmd --version')
    check_command_exists('apt-ftparchive', 'apt-ftparchive --version')
    print('WARNING: dryrun is set to "false" - this will push and publish the release')
    input('Press Enter to continue...')

  print(''.join(['-' for _ in range(80)]))
  print('Preparing Release from branch [%s] running tests: [%s] dryrun: [%s]' % (src_branch, run_tests, dry_run))
  print('  JAVA_HOME is [%s]' % JAVA_HOME)
  print('  Running with maven command: [%s] ' % (MVN))
  if build:
    check_norelease(path='src')
    ensure_checkout_is_clean(src_branch)
    verify_lucene_version()
    release_version = find_release_version(src_branch)
    ensure_no_open_tickets(release_version)
    if not dry_run:
      smoke_test_version = release_version
    head_hash = get_head_hash()
    run_mvn('clean') # clean the env!
    print('  Release version: [%s]' % release_version)
    create_release_branch(remote, src_branch, release_version)
    print('  Created release branch [%s]' % (release_branch(release_version)))
    success = False
    try:
      pending_files = [POM_FILE, VERSION_FILE]
      remove_maven_snapshot(POM_FILE, release_version)
      remove_version_snapshot(VERSION_FILE, release_version)
      print('  Done removing snapshot version')
      add_pending_files(*pending_files) # expects var args use * to expand
      commit_release(release_version)
      pending_files = update_reference_docs(release_version)
      version_head_hash = None
      # split commits for docs and version to enable easy cherry-picking
      if pending_files:
        add_pending_files(*pending_files) # expects var args use * to expand
        commit_feature_flags(release_version)
        version_head_hash = get_head_hash()
      print('  Committed release version [%s]' % release_version)
      print(''.join(['-' for _ in range(80)]))
      print('Building Release candidate')
      input('Press Enter to continue...')
      if not dry_run:
        print('  Running maven builds now and publish to Sonatype - run-tests [%s]' % run_tests)
      else:
        print('  Running maven builds now run-tests [%s]' % run_tests)
      build_release(run_tests=run_tests, dry_run=dry_run, cpus=cpus, bwc_version=find_bwc_version(release_version, bwc_path))
      artifacts = get_artifacts(release_version)
      print('Checking if all artifacts contain the same jars')
      check_artifacts_for_same_jars(artifacts)
      artifacts_and_checksum = generate_checksums(artifacts)
      smoke_test_release(release_version, artifacts, get_head_hash(), PLUGINS)
      print(''.join(['-' for _ in range(80)]))
      print('Finish Release -- dry_run: %s' % dry_run)
      input('Press Enter to continue...')
      print('  merge release branch, tag and push to %s %s -- dry_run: %s' % (remote, src_branch, dry_run))
      merge_tag_push(remote, src_branch, release_version, dry_run)
      print('  publish artifacts to S3 -- dry_run: %s' % dry_run)
      publish_artifacts(artifacts_and_checksum, dry_run=dry_run)
      print('  Updating package repositories -- dry_run: %s' % dry_run)
      publish_repositories(src_branch, dry_run=dry_run)
      cherry_pick_command = '.'
      if version_head_hash:
        cherry_pick_command = ' and cherry-pick the documentation changes: \'git cherry-pick %s\' to the development branch' % (version_head_hash)
      pending_msg = """
      Release successful pending steps:
        * create a new vX.Y.Z label on github for the next release, with label color #dddddd (https://github.com/elastic/elasticsearch/labels)
        * publish the maven artifacts on Sonatype: https://oss.sonatype.org/index.html
           - here is a guide: http://central.sonatype.org/pages/releasing-the-deployment.html
        * check if the release is there https://oss.sonatype.org/content/repositories/releases/org/elasticsearch/elasticsearch/%(version)s
        * announce the release on the website / blog post
        * tweet about the release
        * announce the release in the google group/mailinglist
        * Move to a Snapshot version to the current branch for the next point release%(cherry_pick)s
      """
      print(pending_msg % { 'version' : release_version, 'cherry_pick' : cherry_pick_command} )
      success = True
    finally:
      if not success:
        run('git reset --hard HEAD')
        run('git checkout %s' %  src_branch)
      elif dry_run:
        run('git reset --hard %s' % head_hash)
        run('git tag -d v%s' % release_version)
      # we delete this one anyways
      run('git branch -D %s' %  (release_branch(release_version)))
  else:
    print("Skipping build - smoketest only against version %s" % smoke_test_version)
    run_mvn('clean') # clean the env!
    
  if smoke_test_version:
    fetch(remote)
    download_and_verify(smoke_test_version, artifact_names(smoke_test_version), plugins=PLUGINS)
