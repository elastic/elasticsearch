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

import datetime
import os
import shutil
import sys
import time
import urllib
import urllib.request
import zipfile

from os.path import dirname, abspath

"""
 This tool builds a release from the a given elasticsearch plugin branch.

 It is basically a wrapper on top of launch_release.py which:

 - tries to get a more recent version of launch_release.py in ...
 - download it if needed
 - launch it passing all arguments to it, like:

   $ python3 dev_tools/release.py --branch master --publish --remote origin

 Important options:

   # Dry run
   $ python3 dev_tools/release.py

   # Dry run without tests
   python3 dev_tools/release.py --skiptests

   # Release, publish artifacts and announce
   $ python3 dev_tools/release.py --publish

 See full documentation in launch_release.py
"""
env = os.environ

# Change this if the source repository for your scripts is at a different location
SOURCE_REPO = 'elasticsearch/elasticsearch-plugins-script'
# We define that we should download again the script after 1 days
SCRIPT_OBSOLETE_DAYS = 1
# We ignore in master.zip file the following files
IGNORED_FILES = ['.gitignore', 'README.md']


ROOT_DIR = abspath(os.path.join(abspath(dirname(__file__)), '../'))
TARGET_TOOLS_DIR = ROOT_DIR + '/plugin_tools'
DEV_TOOLS_DIR = ROOT_DIR + '/dev-tools'
BUILD_RELEASE_FILENAME = 'release.zip'
BUILD_RELEASE_FILE = TARGET_TOOLS_DIR + '/' + BUILD_RELEASE_FILENAME
SOURCE_URL = 'https://github.com/%s/archive/master.zip' % SOURCE_REPO

# Download a recent version of the release plugin tool
try:
    os.mkdir(TARGET_TOOLS_DIR)
    print('directory %s created' % TARGET_TOOLS_DIR)
except FileExistsError:
    pass


try:
    # we check latest update. If we ran an update recently, we
    # are not going to check it again
    download = True

    try:
        last_download_time = datetime.datetime.fromtimestamp(os.path.getmtime(BUILD_RELEASE_FILE))
        if (datetime.datetime.now()-last_download_time).days < SCRIPT_OBSOLETE_DAYS:
            download = False
    except FileNotFoundError:
        pass

    if download:
        urllib.request.urlretrieve(SOURCE_URL, BUILD_RELEASE_FILE)
        with zipfile.ZipFile(BUILD_RELEASE_FILE) as myzip:
            for member in myzip.infolist():
                filename = os.path.basename(member.filename)
                # skip directories
                if not filename:
                    continue
                if filename in IGNORED_FILES:
                    continue

                # copy file (taken from zipfile's extract)
                source = myzip.open(member.filename)
                target = open(os.path.join(TARGET_TOOLS_DIR, filename), "wb")
                with source, target:
                    shutil.copyfileobj(source, target)
                    # We keep the original date
                    date_time = time.mktime(member.date_time + (0, 0, -1))
                    os.utime(os.path.join(TARGET_TOOLS_DIR, filename), (date_time, date_time))
        print('plugin-tools updated from %s' % SOURCE_URL)
except urllib.error.HTTPError:
    pass


# Let see if we need to update the release.py script itself
source_time = os.path.getmtime(TARGET_TOOLS_DIR + '/release.py')
repo_time = os.path.getmtime(DEV_TOOLS_DIR + '/release.py')
if source_time > repo_time:
    input('release.py needs an update. Press a key to update it...')
    shutil.copyfile(TARGET_TOOLS_DIR + '/release.py', DEV_TOOLS_DIR + '/release.py')

# We can launch the build process
try:
    PYTHON = 'python'
    # make sure python3 is used if python3 is available
    # some systems use python 2 as default
    os.system('python3 --version > /dev/null 2>&1')
    PYTHON = 'python3'
except RuntimeError:
    pass

release_args = ''
for x in range(1, len(sys.argv)):
    release_args += ' ' + sys.argv[x]

os.system('%s %s/build_release.py %s' % (PYTHON, TARGET_TOOLS_DIR, release_args))
