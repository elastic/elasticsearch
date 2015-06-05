# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance  with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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
import argparse
import github3
import smtplib
import sys

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from os.path import dirname, abspath

"""
 This tool builds a release from the a given elasticsearch plugin branch.
 In order to execute it go in the top level directory and run:
   $ python3 dev_tools/build_release.py --branch master --publish --remote origin

 By default this script runs in 'dry' mode which essentially simulates a release. If the
 '--publish' option is set the actual release is done.

   $ python3 dev_tools/build_release.py --publish --remote origin

 The script takes over almost all
 steps necessary for a release from a high level point of view it does the following things:

  - run prerequisite checks
  - detect the version to release from the specified branch (--branch) or the current branch
  - creates a version release branch & updates pom.xml to point to a release version rather than a snapshot
  - builds the artifacts
  - commits the new version and merges the version release branch into the source branch
  - merges the master release branch into the master branch
  - creates a tag and pushes branch and master to the specified origin (--remote)
  - publishes the releases to sonatype

Once it's done it will print all the remaining steps.

 Prerequisites:
    - Python 3k for script execution
"""
env = os.environ

LOG = env.get('ES_RELEASE_LOG', '/tmp/elasticsearch_release.log')
ROOT_DIR = abspath(os.path.join(abspath(dirname(__file__)), '../'))
POM_FILE = ROOT_DIR + '/pom.xml'

##########################################################
#
# Utility methods (log and run)
#
##########################################################
# Log a message
def log(msg):
    log_plain('\n%s' % msg)


# Purge the log file
def purge_log():
    try:
        os.remove(LOG)
    except FileNotFoundError:
        pass


# Log a message to the LOG file
def log_plain(msg):
    f = open(LOG, mode='ab')
    f.write(msg.encode('utf-8'))
    f.close()


# Run a command and log it
def run(command, quiet=False):
    log('%s: RUN: %s\n' % (datetime.datetime.now(), command))
    if os.system('%s >> %s 2>&1' % (command, LOG)):
        msg = '    FAILED: %s [see log %s]' % (command, LOG)
        if not quiet:
            print(msg)
        raise RuntimeError(msg)

##########################################################
#
# Clean logs and check JAVA and Maven
#
##########################################################
try:
    purge_log()
    JAVA_HOME = env['JAVA_HOME']
except KeyError:
    raise RuntimeError("""
  Please set JAVA_HOME in the env before running release tool
  On OSX use: export JAVA_HOME=`/usr/libexec/java_home -v '1.7*'`""")

try:
    MVN = 'mvn'
    # make sure mvn3 is used if mvn3 is available
    # some systems use maven 2 as default
    run('mvn3 --version', quiet=True)
    MVN = 'mvn3'
except RuntimeError:
    pass


def java_exe():
    path = JAVA_HOME
    return 'export JAVA_HOME="%s" PATH="%s/bin:$PATH" JAVACMD="%s/bin/java"' % (path, path, path)


##########################################################
#
# String and file manipulation utils
#
##########################################################
# Utility that returns the name of the release branch for a given version
def release_branch(branchsource, version):
    return 'release_branch_%s_%s' % (branchsource, version)


# Reads the given file and applies the
# callback to it. If the callback changed
# a line the given file is replaced with
# the modified input.
def process_file(file_path, line_callback):
    fh, abs_path = tempfile.mkstemp()
    modified = False
    with open(abs_path, 'w', encoding='utf-8') as new_file:
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


# Split a version x.y.z as an array of digits [x,y,z]
def split_version_to_digits(version):
    return list(map(int, re.findall(r'\d+', version)))


# Guess the next snapshot version number (increment last digit)
def guess_snapshot(version):
    digits = split_version_to_digits(version)
    source = '%s.%s.%s' % (digits[0], digits[1], digits[2])
    destination = '%s.%s.%s' % (digits[0], digits[1], digits[2] + 1)
    return version.replace(source, destination)


# Guess the anchor in generated documentation
# Looks like this "#version-230-for-elasticsearch-13"
def get_doc_anchor(release, esversion):
    plugin_digits = split_version_to_digits(release)
    es_digits = split_version_to_digits(esversion)
    return '#version-%s%s%s-for-elasticsearch-%s%s' % (
        plugin_digits[0], plugin_digits[1], plugin_digits[2], es_digits[0], es_digits[1])


# Moves the pom.xml file from a snapshot to a release
def remove_maven_snapshot(pom, release):
    pattern = '<version>%s-SNAPSHOT</version>' % release
    replacement = '<version>%s</version>' % release

    def callback(line):
        return line.replace(pattern, replacement)

    process_file(pom, callback)


# Moves the pom.xml file to the next snapshot
def add_maven_snapshot(pom, release, snapshot):
    pattern = '<version>%s</version>' % release
    replacement = '<version>%s-SNAPSHOT</version>' % snapshot

    def callback(line):
        return line.replace(pattern, replacement)

    process_file(pom, callback)


# Checks the pom.xml for the release version. <version>2.0.0-SNAPSHOT</version>
# This method fails if the pom file has no SNAPSHOT version set ie.
# if the version is already on a release version we fail.
# Returns the next version string ie. 0.90.7
def find_release_version(src_branch):
    git_checkout(src_branch)
    with open(POM_FILE, encoding='utf-8') as file:
        for line in file:
            match = re.search(r'<version>(.+)-SNAPSHOT</version>', line)
            if match:
                return match.group(1)
        raise RuntimeError('Could not find release version in branch %s' % src_branch)


# extract a value from pom.xml
def find_from_pom(tag):
    with open(POM_FILE, encoding='utf-8') as file:
        for line in file:
            match = re.search(r'<%s>(.+)</%s>' % (tag, tag), line)
            if match:
                return match.group(1)
        raise RuntimeError('Could not find <%s> in pom.xml file' % (tag))


##########################################################
#
# GIT commands
#
##########################################################
# Returns the hash of the current git HEAD revision
def get_head_hash():
    return os.popen('git rev-parse --verify HEAD 2>&1').read().strip()


# Returns the name of the current branch
def get_current_branch():
    return os.popen('git rev-parse --abbrev-ref HEAD  2>&1').read().strip()


# runs get fetch on the given remote
def fetch(remote):
    run('git fetch %s' % remote)


# Creates a new release branch from the given source branch
# and rebases the source branch from the remote before creating
# the release branch. Note: This fails if the source branch
# doesn't exist on the provided remote.
def create_release_branch(remote, src_branch, release):
    git_checkout(src_branch)
    run('git pull --rebase %s %s' % (remote, src_branch))
    run('git checkout -b %s' % (release_branch(src_branch, release)))


# Stages the given files for the next git commit
def add_pending_files(*files):
    for file in files:
        run('git add %s' % file)


# Executes a git commit with 'release [version]' as the commit message
def commit_release(artifact_id, release):
    run('git commit -m "prepare release %s-%s"' % (artifact_id, release))


# Commit documentation changes on the master branch
def commit_master(release):
    run('git commit -m "update documentation with release %s"' % release)


# Commit next snapshot files
def commit_snapshot():
    run('git commit -m "prepare for next development iteration"')


# Put the version tag on on the current commit
def tag_release(release):
    run('git tag -a v%s -m "Tag release version %s"' % (release, release))


# Checkout a given branch
def git_checkout(branch):
    run('git checkout %s' % branch)


# Merge the release branch with the actual branch
def git_merge(src_branch, release_version):
    git_checkout(src_branch)
    run('git merge %s' % release_branch(src_branch, release_version))


# Push the actual branch and master branch
def git_push(remote, src_branch, release_version, dry_run):
    if not dry_run:
        run('git push %s %s master' % (remote, src_branch))  # push the commit and the master
        run('git push %s v%s' % (remote, release_version))  # push the tag
    else:
        print('  dryrun [True] -- skipping push to remote %s %s master' % (remote, src_branch))


##########################################################
#
# Maven commands
#
##########################################################
# Run a given maven command
def run_mvn(*cmd):
    for c in cmd:
        run('%s; %s -f %s %s' % (java_exe(), MVN, POM_FILE, c))


# Run deploy or package depending on dry_run
# Default to run mvn package
# When run_tests=True a first mvn clean test is run
def build_release(run_tests=False, dry_run=True):
    target = 'deploy'
    tests = '-DskipTests'
    if run_tests:
        tests = ''
    if dry_run:
        target = 'package'
    run_mvn('clean %s %s' % (target, tests))


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


# we print a notice if we can not find the relevant infos in the ~/.m2/settings.xml
print_sonatype_notice()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Builds and publishes a Elasticsearch Plugin Release')
    parser.add_argument('--branch', '-b', metavar='master', default=get_current_branch(),
                        help='The branch to release from. Defaults to the current branch.')
    parser.add_argument('--skiptests', '-t', dest='tests', action='store_false',
                        help='Skips tests before release. Tests are run by default.')
    parser.set_defaults(tests=True)
    parser.add_argument('--remote', '-r', metavar='origin', default='origin',
                        help='The remote to push the release commit and tag to. Default is [origin]')
    parser.add_argument('--publish', '-p', dest='dryrun', action='store_false',
                        help='Publishes the release. Disable by default.')
    parser.add_argument('--disable_mail', '-dm', dest='mail', action='store_false',
                        help='Do not send a release email. Email is sent by default.')

    parser.set_defaults(dryrun=True)
    parser.set_defaults(mail=True)
    args = parser.parse_args()

    src_branch = args.branch
    remote = args.remote
    run_tests = args.tests
    dry_run = args.dryrun
    mail = args.mail

    if src_branch == 'master':
        raise RuntimeError('Can not release the master branch. You need to create another branch before a release')

    if not dry_run:
        print('WARNING: dryrun is set to "false" - this will push and publish the release')
        input('Press Enter to continue...')

    print(''.join(['-' for _ in range(80)]))
    print('Preparing Release from branch [%s] running tests: [%s] dryrun: [%s]' % (src_branch, run_tests, dry_run))
    print('  JAVA_HOME is [%s]' % JAVA_HOME)
    print('  Running with maven command: [%s] ' % (MVN))

    release_version = find_release_version(src_branch)
    artifact_id = find_from_pom('artifactId')
    artifact_name = find_from_pom('name')
    artifact_description = find_from_pom('description')
    elasticsearch_version = find_from_pom('elasticsearch.version')
    print('  Artifact Id: [%s]' % artifact_id)
    print('  Release version: [%s]' % release_version)
    print('  Elasticsearch: [%s]' % elasticsearch_version)
    if elasticsearch_version.find('-SNAPSHOT') != -1:
        raise RuntimeError('Can not release with a SNAPSHOT elasticsearch dependency: %s' % elasticsearch_version)

    # extract snapshot
    default_snapshot_version = guess_snapshot(release_version)
    snapshot_version = input('Enter next snapshot version [%s]:' % default_snapshot_version)
    snapshot_version = snapshot_version or default_snapshot_version

    print('  Next version: [%s-SNAPSHOT]' % snapshot_version)
    print('  Artifact Name: [%s]' % artifact_name)
    print('  Artifact Description: [%s]' % artifact_description)

    if not dry_run:
        smoke_test_version = release_version

    try:
        git_checkout(src_branch)
        version_hash = get_head_hash()
        run_mvn('clean')  # clean the env!
        create_release_branch(remote, src_branch, release_version)
        print('  Created release branch [%s]' % (release_branch(src_branch, release_version)))
    except RuntimeError:
        print('Logs:')
        with open(LOG, 'r') as log_file:
            print(log_file.read())
        sys.exit(-1)

    success = False
    try:
        ########################################
        # Start update process in version branch
        ########################################
        pending_files = [POM_FILE]
        remove_maven_snapshot(POM_FILE, release_version)
        print('  Done removing snapshot version')
        add_pending_files(*pending_files)  # expects var args use * to expand
        commit_release(artifact_id, release_version)
        print('  Committed release version [%s]' % release_version)
        print(''.join(['-' for _ in range(80)]))
        print('Building Release candidate')
        input('Press Enter to continue...')
        if not dry_run:
            print('  Running maven builds now and publish to sonatype - run-tests [%s]' % run_tests)
        else:
            print('  Running maven builds now run-tests [%s]' % run_tests)
        build_release(run_tests=run_tests, dry_run=dry_run)

        print(''.join(['-' for _ in range(80)]))

        print('Finish Release -- dry_run: %s' % dry_run)
        input('Press Enter to continue...')

        print('  merge release branch')
        git_merge(src_branch, release_version)
        print('  tag')
        tag_release(release_version)

        add_maven_snapshot(POM_FILE, release_version, snapshot_version)
        add_pending_files(*pending_files)
        commit_snapshot()

        print('  push to %s %s -- dry_run: %s' % (remote, src_branch, dry_run))
        git_push(remote, src_branch, release_version, dry_run)

        pending_msg = """
Release successful pending steps:
    * close and release sonatype repo: https://oss.sonatype.org/
    * check if the release is there https://oss.sonatype.org/content/repositories/releases/org/elasticsearch/%(artifact_id)s/%(version)s
"""
        print(pending_msg % {'version': release_version,
                             'artifact_id': artifact_id})
        success = True
    finally:
        if not success:
            print('Logs:')
            with open(LOG, 'r') as log_file:
                print(log_file.read())
            git_checkout(src_branch)
            run('git reset --hard %s' % version_hash)
            try:
                run('git tag -d v%s' % release_version)
            except RuntimeError:
                pass
        elif dry_run:
            print('End of dry_run')
            input('Press Enter to reset changes...')
            git_checkout(src_branch)
            run('git reset --hard %s' % version_hash)
            run('git tag -d v%s' % release_version)

        # we delete this one anyways
        run('git branch -D %s' % (release_branch(src_branch, release_version)))

        # Checkout the branch we started from
        git_checkout(src_branch)
