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
import argparse
import github3
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from os.path import dirname, abspath

"""
 This tool builds a release from the a given elasticsearch plugin branch.
 In order to execute it go in the top level directory and run:
   $ python3 dev_tools/build_release.py --branch master --publish --remote origin

 By default this script runs in 'dry' mode which essentially simulates a release. If the
 '--publish' option is set the actual release is done.
 If not in 'dry' mode, a mail will be automatically sent to the mailing list.
 You can disable it with the option  '--disable_mail'

   $ python3 dev_tools/build_release.py --publish --remote origin --disable_mail

 The script takes over almost all
 steps necessary for a release from a high level point of view it does the following things:

  - run prerequisite checks ie. check for S3 credentials available as env variables
  - detect the version to release from the specified branch (--branch) or the current branch
  - creates a release branch & updates pom.xml and README.md to point to a release version rather than a snapshot
  - builds the artifacts
  - commits the new version and merges the release branch into the source branch
  - creates a tag and pushes the commit to the specified origin (--remote)
  - publishes the releases to sonatype and S3
  - send a mail based on github issues fixed by this version

Once it's done it will print all the remaining steps.

 Prerequisites:
    - Python 3k for script execution
    - Boto for S3 Upload ($ apt-get install python-boto or pip-3.3 install boto)
    - github3 module (pip-3.3 install github3.py)
    - S3 keys exported via ENV Variables (AWS_ACCESS_KEY_ID,  AWS_SECRET_ACCESS_KEY)
    - GITHUB (login/password) or key exported via ENV Variables (GITHUB_LOGIN,  GITHUB_PASSWORD or GITHUB_KEY)
    (see https://github.com/settings/applications#personal-access-tokens) - Optional: default to no authentication
    - SMTP_HOST - Optional: default to localhost
    - MAIL_SENDER - Optional: default to 'david@pilato.fr': must be authorized to send emails to elasticsearch mailing list
    - MAIL_TO - Optional: default to 'elasticsearch@googlegroups.com'
"""
env = os.environ

LOG = env.get('ES_RELEASE_LOG', '/tmp/elasticsearch_release.log')
ROOT_DIR = os.path.join(abspath(dirname(__file__)), '../')
README_FILE = ROOT_DIR + 'README.md'
POM_FILE = ROOT_DIR + 'pom.xml'

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

# Returns the hash of the current git HEAD revision
def get_head_hash():
    return os.popen(' git rev-parse --verify HEAD 2>&1').read().strip()

# Returns the hash of the given tag revision
def get_tag_hash(tag):
    return os.popen('git show-ref --tags %s --hash 2>&1' % (tag)).read().strip()

# Returns the name of the current branch
def get_current_branch():
    return os.popen('git rev-parse --abbrev-ref HEAD  2>&1').read().strip()

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

# Guess the next snapshot version number (increment second digit)
def guess_snapshot(version):
    digits=list(map(int, re.findall(r'\d+', version)))
    source='%s.%s' % (digits[0], digits[1])
    destination='%s.%s' % (digits[0], digits[1]+1)
    return version.replace(source, destination)

# Moves the pom.xml file from a snapshot to a release
def remove_maven_snapshot(pom, release):
    pattern = '<version>%s-SNAPSHOT</version>' % release
    replacement = '<version>%s</version>' % release
    def callback(line):
        return line.replace(pattern, replacement)
    process_file(pom, callback)

# Moves the README.md file from a snapshot to a release
def remove_version_snapshot(readme_file, release):
    pattern = '%s-SNAPSHOT' % release
    replacement = '%s         ' % release
    def callback(line):
        return line.replace(pattern, replacement)
    process_file(readme_file, callback)

# Moves the pom.xml file to the next snapshot
def add_maven_snapshot(pom, release, snapshot):
    pattern = '<version>%s</version>' % release
    replacement = '<version>%s-SNAPSHOT</version>' % snapshot
    def callback(line):
        return line.replace(pattern, replacement)
    process_file(pom, callback)

# Add in README.md file the next snapshot
def add_version_snapshot(readme_file, release, snapshot):
    pattern = '| %s         ' % release
    replacement = '| %s-SNAPSHOT' % snapshot
    def callback(line):
        # If we find pattern, we copy the line and replace its content
        if line.find(pattern) >= 0:
            return line.replace(pattern, replacement).replace('%s' % (datetime.datetime.now().strftime("%Y-%m-%d")),
                                                              'XXXX-XX-XX')+line
        else:
            return line
    process_file(readme_file, callback)

# Moves the README.md file from a snapshot to a release (documentation link)
def remove_documentation_snapshot(readme_file, repo_url, release, branch):
    pattern = '* [%s-SNAPSHOT](%sblob/%s/README.md)' % (release, repo_url, branch)
    replacement = '* [%s](%sblob/v%s/README.md)' % (release, repo_url, release)
    def callback(line):
        # If we find pattern, we replace its content
        if line.find(pattern) >= 0:
            return line.replace(pattern, replacement)
        else:
            return line
    process_file(readme_file, callback)

# Add in README.markdown file the documentation for the next version
def add_documentation_snapshot(readme_file, repo_url, release, snapshot, branch):
    pattern = '* [%s](%sblob/v%s/README.md)' % (release, repo_url, release)
    replacement = '* [%s-SNAPSHOT](%sblob/%s/README.md)' % (snapshot, repo_url, branch)
    def callback(line):
        # If we find pattern, we copy the line and replace its content
        if line.find(pattern) >= 0:
            return line.replace(pattern, replacement)+line
        else:
            return line
    process_file(readme_file, callback)

# Set release date in README.md file
def set_date(readme_file):
    pattern = 'XXXX-XX-XX'
    replacement = '%s' % (datetime.datetime.now().strftime("%Y-%m-%d"))
    def callback(line):
        return line.replace(pattern, replacement)
    process_file(readme_file, callback)

# Update installation instructions in README.md file
def set_install_instructions(readme_file, artifact_name, release):
    pattern = '`bin/plugin -install elasticsearch/%s/.+`' % artifact_name
    replacement = '`bin/plugin -install elasticsearch/%s/%s`' % (artifact_name, release)
    def callback(line):
        return re.sub(pattern, replacement, line)
    process_file(readme_file, callback)


# Stages the given files for the next git commit
def add_pending_files(*files):
    for file in files:
        run('git add %s' % file)

# Executes a git commit with 'release [version]' as the commit message
def commit_release(artifact_id, release):
    run('git commit -m "prepare release %s-%s"' % (artifact_id, release))

def commit_snapshot():
    run('git commit -m "prepare for next development iteration"')

def tag_release(release):
    run('git tag -a v%s -m "Tag release version %s"' % (release, release))

def run_mvn(*cmd):
    for c in cmd:
        run('%s; %s -f %s %s' % (java_exe(), MVN, POM_FILE, c))

def build_release(run_tests=False, dry_run=True):
    target = 'deploy'
    if dry_run:
        target = 'package'
    if run_tests:
        run_mvn('clean test')
    run_mvn('clean %s -DskipTests' %(target))

# Checks the pom.xml for the release version. <version>2.0.0-SNAPSHOT</version>
# This method fails if the pom file has no SNAPSHOT version set ie.
# if the version is already on a release version we fail.
# Returns the next version string ie. 0.90.7
def find_release_version(src_branch):
    run('git checkout %s' % src_branch)
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

def get_artifacts(artifact_id, release):
    artifact_path = ROOT_DIR + 'target/releases/%s-%s.zip' % (artifact_id, release)
    print('  Path %s' % (artifact_path))
    if not os.path.isfile(artifact_path):
        raise RuntimeError('Could not find required artifact at %s' % (artifact_path))
    return artifact_path

# Generates sha1 for a file
# and returns the checksum files as well
# as the given files in a list
def generate_checksums(release_file):
    res = []
    directory = os.path.dirname(release_file)
    file = os.path.basename(release_file)
    checksum_file = '%s.sha1.txt' % file

    if os.system('cd %s; shasum %s > %s' % (directory, file, checksum_file)):
        raise RuntimeError('Failed to generate checksum for file %s' % release_file)
    res = res + [os.path.join(directory, checksum_file), release_file]
    return res

def git_merge(src_branch, release_version):
    run('git checkout %s' %  src_branch)
    run('git merge %s' %  release_branch(release_version))

def git_push(remote, src_branch, release_version, dry_run):
    if not dry_run:
        run('git push %s %s' % (remote, src_branch)) # push the commit
        run('git push %s v%s' % (remote, release_version)) # push the tag
    else:
        print('  dryrun [True] -- skipping push to remote %s' % remote)

def publish_artifacts(artifacts, base='elasticsearch/elasticsearch', dry_run=True):
    location = os.path.dirname(os.path.realpath(__file__))
    for artifact in artifacts:
        if dry_run:
            print('Skip Uploading %s to Amazon S3 in %s' % (artifact, base))
        else:
            print('Uploading %s to Amazon S3' % artifact)
            # requires boto to be installed but it is not available on python3k yet so we use a dedicated tool
            run('python %s/upload-s3.py --file %s --path %s' % (location, os.path.abspath(artifact), base))


#################
##
##
## Email and Github Management
##
##
#################
def format_issues_plain(issues, title='Fix'):
    response = ""

    if len(issues) > 0:
        response += '%s:\n' % title
        for issue in issues:
            response += ' * [%s] - %s (%s)\n' % (issue.number, issue.title, issue.html_url)

    return response

def format_issues_html(issues, title='Fix'):
    response = ""

    if len(issues) > 0:
        response += '<h2>%s</h2>\n<ul>\n' % title
        for issue in issues:
            response += '<li>[<a href="%s">%s</a>] - %s\n' % (issue.html_url, issue.number, issue.title)
        response += '</ul>\n'

    return response

def get_github_repository(reponame,
                          login=env.get('GITHUB_LOGIN', None),
                          password=env.get('GITHUB_PASSWORD', None),
                          key=env.get('GITHUB_KEY', None)):
    if login:
        g = github3.login(login, password)
    elif key:
        g = github3.login(token=key)
    else:
        g = github3.GitHub()

    return g.repository("elasticsearch", reponame)

# Check if there are some remaining open issues and fails
def check_opened_issues(version, repository, reponame):
    opened_issues = [i for i in repository.iter_issues(state='open', labels='%s' % version)]
    if len(opened_issues)>0:
        raise NameError('Some issues [%s] are still opened. Check https://github.com/elasticsearch/%s/issues?labels=%s&state=open'
                        % (len(opened_issues), reponame, version))

# List issues from github: can be done anonymously if you don't
# exceed a given number of github API calls per day
# Check if there are some remaining open issues and fails
def list_issues(version,
                repository,
                severity='bug'):
    issues = [i for i in repository.iter_issues(state='closed', labels='%s,%s' % (severity, version))]
    return issues

# Get issues from github and generates a Plain/HTML Multipart email
# And send it if dry_run=False
def prepare_email(artifact_id, release_version, repository,
                  artifact_name, artifact_description, project_url,
                  severity_labels_bug='bug',
                  severity_labels_update='update',
                  severity_labels_new='new',
                  severity_labels_doc='doc'):

    ## Get bugs from github
    issues_bug = list_issues(release_version, repository, severity=severity_labels_bug)
    issues_update = list_issues(release_version, repository, severity=severity_labels_update)
    issues_new = list_issues(release_version, repository, severity=severity_labels_new)
    issues_doc = list_issues(release_version, repository, severity=severity_labels_doc)

    ## Format content to plain text
    plain_issues_bug = format_issues_plain(issues_bug, 'Fix')
    plain_issues_update = format_issues_plain(issues_update, 'Update')
    plain_issues_new = format_issues_plain(issues_new, 'New')
    plain_issues_doc = format_issues_plain(issues_doc, 'Doc')

    ## Format content to html
    html_issues_bug = format_issues_html(issues_bug, 'Fix')
    html_issues_update = format_issues_html(issues_update, 'Update')
    html_issues_new = format_issues_html(issues_new, 'New')
    html_issues_doc = format_issues_html(issues_doc, 'Doc')

    if len(issues_bug)+len(issues_update)+len(issues_new)+len(issues_doc) > 0:
        plain_empty_message = ""
        html_empty_message = ""

    else:
        plain_empty_message = "No issue listed for this release"
        html_empty_message = "<p>No issue listed for this release</p>"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = '[ANN] %s %s released' % (artifact_name, release_version)
    text = """
Heya,


We are pleased to announce the release of the %(artifact_name)s, version %(release_version)s.

%(artifact_description)s.

%(project_url)s

Release Notes - %(artifact_id)s - Version %(release_version)s

%(empty_message)s
%(issues_bug)s
%(issues_update)s
%(issues_new)s
%(issues_doc)s

Issues, Pull requests, Feature requests are warmly welcome on %(artifact_id)s project repository: %(project_url)s
For questions or comments around this plugin, feel free to use elasticsearch mailing list: https://groups.google.com/forum/#!forum/elasticsearch

Enjoy,

-The Elasticsearch team
""" % {'release_version': release_version,
       'artifact_id': artifact_id,
       'artifact_name': artifact_name,
       'artifact_description': artifact_description,
       'project_url': project_url,
       'empty_message': plain_empty_message,
       'issues_bug': plain_issues_bug,
       'issues_update': plain_issues_update,
       'issues_new': plain_issues_new,
       'issues_doc': plain_issues_doc}

    html = """
<html>
    <body>
        <p>Heya,</p>

        <p>We are pleased to announce the release of the <b>%(artifact_name)s</b>, <b>version %(release_version)s</b></p>

<blockquote>%(artifact_description)s.</blockquote>

<h1>Release Notes - Version %(release_version)s</h1>
%(empty_message)s
%(issues_bug)s
%(issues_update)s
%(issues_new)s
%(issues_doc)s

<p>Issues, Pull requests, Feature requests are warmly welcome on
<a href='%(project_url)s'>%(artifact_id)s</a> project repository!</p>
<p>For questions or comments around this plugin, feel free to use elasticsearch
<a href='https://groups.google.com/forum/#!forum/elasticsearch'>mailing list</a>!</p>

<p>Enjoy,</p>

<p>- The <a href="http://www.elasticsearch.com/">Elasticsearch team</a></p>
</body></html>
""" % {'release_version': release_version,
       'artifact_id': artifact_id,
       'artifact_name': artifact_name,
       'artifact_description': artifact_description,
       'project_url': project_url,
       'empty_message': html_empty_message,
       'issues_bug': html_issues_bug,
       'issues_update': html_issues_update,
       'issues_new': html_issues_new,
       'issues_doc': html_issues_doc}

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    return msg

def send_email(msg,
               dry_run=True,
               mail=True,
               sender=env.get('MAIL_SENDER'),
               to=env.get('MAIL_TO', 'elasticsearch@googlegroups.com'),
               smtp_server=env.get('SMTP_SERVER', 'localhost')):
    msg['From'] = 'Elasticsearch Team <%s>' % sender
    msg['To'] = 'Elasticsearch Mailing List <%s>' % to
    # save mail on disk
    with open(ROOT_DIR+'target/email.txt', 'w') as email_file:
        email_file.write(msg.as_string())
    if mail and not dry_run:
        s = smtplib.SMTP(smtp_server, 25)
        s.sendmail(sender, to, msg.as_string())
        s.quit()
    else:
        print('generated email: open %starget/email.txt' % ROOT_DIR)

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

def check_github_credentials():
    if not env.get('GITHUB_KEY', None) and not env.get('GITHUB_LOGIN', None):
        log('WARN: Could not find "GITHUB_LOGIN" / "GITHUB_PASSWORD" or "GITHUB_KEY" in the env variables. You could need it.')

def check_email_settings():
    if not env.get('MAIL_SENDER', None):
        raise RuntimeError('Could not find "MAIL_SENDER"')

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

    if not dry_run:
        check_s3_credentials()
        print('WARNING: dryrun is set to "false" - this will push and publish the release')
        if mail:
            check_email_settings()
            print('An email to %s will be sent after the release'
                  % env.get('MAIL_TO', 'elasticsearch@googlegroups.com'))
        input('Press Enter to continue...')

    check_github_credentials()

    print(''.join(['-' for _ in range(80)]))
    print('Preparing Release from branch [%s] running tests: [%s] dryrun: [%s]' % (src_branch, run_tests, dry_run))
    print('  JAVA_HOME is [%s]' % JAVA_HOME)
    print('  Running with maven command: [%s] ' % (MVN))

    release_version = find_release_version(src_branch)
    artifact_id = find_from_pom('artifactId')
    artifact_name = find_from_pom('name')
    artifact_description = find_from_pom('description')
    project_url = find_from_pom('url')
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
    print('  Project URL: [%s]' % project_url)

    if not dry_run:
        smoke_test_version = release_version
    head_hash = get_head_hash()
    run_mvn('clean') # clean the env!
    create_release_branch(remote, src_branch, release_version)
    print('  Created release branch [%s]' % (release_branch(release_version)))
    success = False
    try:
        pending_files = [POM_FILE, README_FILE]
        remove_maven_snapshot(POM_FILE, release_version)
        remove_documentation_snapshot(README_FILE, project_url, release_version, src_branch)
        remove_version_snapshot(README_FILE, release_version)
        set_date(README_FILE)
        set_install_instructions(README_FILE, artifact_id, release_version)
        print('  Done removing snapshot version')
        add_pending_files(*pending_files) # expects var args use * to expand
        commit_release(artifact_id, release_version)
        print('  Committed release version [%s]' % release_version)
        print(''.join(['-' for _ in range(80)]))
        print('Building Release candidate')
        input('Press Enter to continue...')
        print('  Checking github issues')
        repository = get_github_repository(artifact_id)
        check_opened_issues(release_version, repository, artifact_id)
        if not dry_run:
            print('  Running maven builds now and publish to sonatype - run-tests [%s]' % run_tests)
        else:
            print('  Running maven builds now run-tests [%s]' % run_tests)
        build_release(run_tests=run_tests, dry_run=dry_run)
        artifact = get_artifacts(artifact_id, release_version)
        artifact_and_checksums = generate_checksums(artifact)
        print(''.join(['-' for _ in range(80)]))

        print('Finish Release -- dry_run: %s' % dry_run)
        input('Press Enter to continue...')
        print('  merge release branch')
        git_merge(src_branch, release_version)
        print('  tag')
        tag_release(release_version)

        add_maven_snapshot(POM_FILE, release_version, snapshot_version)
        add_version_snapshot(README_FILE, release_version, snapshot_version)
        add_documentation_snapshot(README_FILE, project_url, release_version, snapshot_version, src_branch)
        add_pending_files(*pending_files)
        commit_snapshot()

        print('  push to %s %s -- dry_run: %s' % (remote, src_branch, dry_run))
        git_push(remote, src_branch, release_version, dry_run)
        print('  publish artifacts to S3 -- dry_run: %s' % dry_run)
        publish_artifacts(artifact_and_checksums, base='elasticsearch/%s' % (artifact_id) , dry_run=dry_run)
        print('  preparing email (from github issues)')
        msg = prepare_email(artifact_id, release_version, repository, artifact_name, artifact_description, project_url)
        print('  sending email -- dry_run: %s, mail: %s' % (dry_run, mail))
        send_email(msg, dry_run=dry_run, mail=mail)

        pending_msg = """
Release successful pending steps:
    * close and release sonatype repo: https://oss.sonatype.org/
    * check if the release is there https://oss.sonatype.org/content/repositories/releases/org/elasticsearch/%(artifact_id)s/%(version)s
    * tweet about the release
"""
        print(pending_msg % {'version': release_version,
                             'artifact_id': artifact_id,
                             'project_url': project_url})
        success = True
    finally:
        if not success:
            run('git reset --hard HEAD')
            run('git checkout %s' %  src_branch)
        elif dry_run:
            print('End of dry_run')
            input('Press Enter to reset changes...')

            run('git reset --hard %s' % head_hash)
            run('git tag -d v%s' % release_version)
        # we delete this one anyways
        run('git branch -D %s' %  (release_branch(release_version)))
