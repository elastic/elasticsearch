# Licensed to  ElasticSearch and Shay Banon under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the 'License'); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from http.client import HTTPConnection

""" 
 This tool builds a release from the a given elasticsearch branch.
 In order to execute it go in the top level directory and run:
   $ python3 dev_tools/build_release.py --branch 0.90 --publish --remote origin

 By default this script runs in 'dry' mode which essentially simulates a release. If the
 '--publish' option is set the actual release is done. The script takes over almost all
 steps necessary for a release from a high level point of view it does the following things:

  - run prerequisit checks ie. check for Java 1.6 being presend or S3 credentials available as env variables
  - detect the version to release from the specified branch (--branch) or the current branch
  - creates a release branch & updates pom.xml and Version.java to point to a release version rather than a snapshot
  - builds the artifacts and runs smoke-tests on the build zip & tar.gz files
  - commits the new version and merges the release branch into the source branch
  - creates a tag and pushes the commit to the specified origin (--remote)
  - publishes the releases to sonar-type and S3

Once it's done it will print all the remaining steps.

 Prerequisites:
    - Python 3k for script execution
    - Boto for S3 Upload ($ apt-get install python-boto)
    - RPM for RPM building ($ apt-get install rpm)
    - S3 keys exported via ENV Variables (AWS_ACCESS_KEY_ID,  AWS_SECRET_ACCESS_KEY)
"""
env = os.environ

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
  On OSX use: export JAVA_HOME=`/usr/libexec/java_home -v '1.6*'`""")

try:
  JAVA_HOME = env['JAVA6_HOME']
except KeyError:
  pass #no JAVA6_HOME - we rely on JAVA_HOME


try:
  MVN='mvn'
  # make sure mvn3 is used if mvn3 is available
  # some systems use maven 2 as default
  run('mvn3 --version', quiet=True)
  MVN='mvn3'
except RuntimeError:
  pass


def java_exe():
  path = JAVA_HOME
  return 'export JAVA_HOME="%s" PATH="%s/bin:$PATH" JAVACMD="%s/bin/java"' % (path, path, path)

def verify_java_version(version):
  s = os.popen('%s; java -version 2>&1' % java_exe()).read()
  if s.find(' version "%s.' % version) == -1:
    raise RuntimeError('got wrong version for java %s:\n%s' % (version, s))

# Verifies the java version. We guarantee that we run with Java 1.6
# If 1.6 is not available fail the build!
def verify_mvn_java_version(version, mvn):
  s = os.popen('%s; %s --version 2>&1' % (java_exe(), mvn)).read()
  if s.find('Java version: %s' % version) == -1:
    raise RuntimeError('got wrong java version for %s %s:\n%s' % (mvn, version, s))

# Returns the hash of the current git HEAD revision
def get_head_hash():
  return os.popen('git rev-parse --verify HEAD 2>&1').read().strip()

# Returns the name of the current branch
def get_current_branch():
  return os.popen('git rev-parse --abbrev-ref HEAD  2>&1').read().strip()

verify_java_version('1.6') # we require to build with 1.6
verify_mvn_java_version('1.6', MVN)

# Utility that returns the name of the release branch for a given version
def release_branch(version):
  return 'release_branch_%s' % version

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
  with open(abs_path,'w') as new_file:
    with open(file_path) as old_file:
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
  # 1.0.0.Beta1 -> 1_0_0_Beat1
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

def tag_release(release):
  run('git tag -a v%s -m "Tag release version %s"' % (release, release))

def run_mvn(*cmd):
  for c in cmd:
    run('%s; %s %s' % (java_exe(), MVN, c))

def build_release(run_tests=False, dry_run=True, cpus=1):
  target = 'deploy'
  if dry_run:
    target = 'package'
  if run_tests:
    run_mvn('clean',
            'test -Dtests.jvms=%s -Des.node.mode=local' % (cpus),
            'test -Dtests.jvms=%s -Des.node.mode=network' % (cpus))
  run_mvn('clean %s -DskipTests' %(target))
  success = False
  try:
    run_mvn('-DskipTests rpm:rpm')
    success = True
  finally:
    if not success:
      print("""
  RPM Bulding failed make sure "rpm" tools are installed.
  Use on of the following commands to install:
    $ brew install rpm # on OSX
    $ apt-get install rpm # on Ubuntu et.al
  """)



def wait_for_node_startup(host='127.0.0.1', port=9200,timeout=15):
  for _ in range(timeout):
    conn = HTTPConnection(host, port, timeout);
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

# Checks the pom.xml for the release version.
# This method fails if the pom file has no SNAPSHOT version set ie.
# if the version is already on a release version we fail.
# Returns the next version string ie. 0.90.7
def find_release_version(src_branch):
  run('git checkout %s' % src_branch)
  with open('pom.xml') as file:
    for line in file:
      match = re.search(r'<version>(.+)-SNAPSHOT</version>', line)
      if match:
        return match.group(1)
    raise RuntimeError('Could not find release version in branch %s' % src_branch)

def get_artifacts(release):
  common_artifacts = [os.path.join('target/releases/', 'elasticsearch-%s.%s' % (release, t)) for t in ['deb', 'tar.gz', 'zip']]
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

def smoke_test_release(release, files):
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
    print('  Starting elasticsearch deamon from [%s]' % os.path.join(tmp_dir, 'elasticsearch-%s' % release))
    run('%s; %s -Des.node.name=smoke_tester -Des.cluster.name=prepare_release -Des.discovery.zen.ping.multicast.enabled=false'
         % (java_exe(), es_run_path))
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
          if version['build_hash'].strip() !=  get_head_hash():
            raise RuntimeError('HEAD hash does not match expected [%s] but got [%s]' % (get_head_hash(), version['build_hash']))
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

def print_sonartype_notice():
  settings = os.path.join(os.path.expanduser('~'), '.m2/settings.xml')
  if os.path.isfile(settings):
     with open(settings) as settings_file:
       for line in settings_file:
         if line.strip() == '<id>sonatype-nexus-snapshots</id>':
           # moving out - we found the indicator no need to print the warning
           return 
  print("""
    NOTE: No sonartype settings detected, make sure you have configured
    your sonartype credentials in '~/.m2/settings.xml':

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

VERSION_FILE = 'src/main/java/org/elasticsearch/Version.java'    
POM_FILE = 'pom.xml'

# we print a notice if we can not find the relevant infos in the ~/.m2/settings.xml 
print_sonartype_notice()

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Builds and publishes a Elasticsearch Release')
  parser.add_argument('--branch', '-b', metavar='master', default=get_current_branch(),
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
  parser.set_defaults(dryrun=True)
  args = parser.parse_args()

  src_branch = args.branch
  remote = args.remote
  run_tests = args.tests
  dry_run = args.dryrun
  cpus = args.cpus
  if not dry_run:
    check_s3_credentials()
    print('WARNING: dryrun is set to "false" - this will push and publish the release') 
    input('Press Enter to continue...')

  print(''.join(['-' for _ in range(80)]))
  print('Preparing Release from branch [%s] running tests: [%s] dryrun: [%s]' % (src_branch, run_tests, dry_run))
  print('  JAVA_HOME is [%s]' % JAVA_HOME)
  print('  Running with maven command: [%s] ' % (MVN))
  release_version = find_release_version(src_branch)
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
    pending_files = pending_files + update_reference_docs(release_version)
    print('  Done removing snapshot version')
    add_pending_files(*pending_files) # expects var args use * to expand
    commit_release(release_version)
    print('  Committed release version [%s]' % release_version)
    print(''.join(['-' for _ in range(80)]))
    print('Building Release candidate')
    input('Press Enter to continue...')
    print('  Running maven builds now and publish to sonartype- run-tests [%s]' % run_tests)
    build_release(run_tests=run_tests, dry_run=dry_run, cpus=cpus)
    artifacts = get_artifacts(release_version)
    artifacts_and_checksum = generate_checksums(artifacts)
    smoke_test_release(release_version, artifacts)
    print(''.join(['-' for _ in range(80)]))
    print('Finish Release -- dry_run: %s' % dry_run)
    input('Press Enter to continue...')
    print('  merge release branch, tag and push to %s %s -- dry_run: %s' % (remote, src_branch, dry_run))
    merge_tag_push(remote, src_branch, release_version, dry_run)
    print('  publish artifacts to S3 -- dry_run: %s' % dry_run)
    publish_artifacts(artifacts_and_checksum, dry_run=dry_run)
    pending_msg = """
    Release successful pending steps: 
      * create a version tag on github for version 'v%(version)s'
      * check if there are pending issues for this version (https://github.com/elasticsearch/elasticsearch/issues?labels=v%(version)s&page=1&state=open)
      * publish the maven artifacts on sonartype: https://oss.sonatype.org/index.html
         - here is a guide: https://docs.sonatype.org/display/Repository/Sonatype+OSS+Maven+Repository+Usage+Guide#SonatypeOSSMavenRepositoryUsageGuide-8a.ReleaseIt
      * check if the release is there https://oss.sonatype.org/content/repositories/releases/org/elasticsearch/elasticsearch/%(version)s
      * announce the release on the website / blog post
      * tweet about the release
    """
    print(pending_msg % { 'version' : release_version} )
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




