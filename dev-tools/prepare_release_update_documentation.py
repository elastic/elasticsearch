# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0 and the Server Side Public License, v 1; you may not use this file except
# in compliance with, at your election, the Elastic License 2.0 or the Server
# Side Public License, v 1.

# Prepare a release: Update the documentation and commit
#
# USAGE:
#
# python3 ./dev-tools/prepare_release_update_documentation.py
#
# Note: Ensure the script is run from the root directory
#       This script needs to be run and then pushed,
#       before proceeding with prepare_release_create-release-version.py
#       on your build VM
#

import fnmatch
import subprocess
import tempfile
import re
import os
import shutil

def run(command):
  if os.system('%s' % (command)):
    raise RuntimeError('    FAILED: %s' % (command))

def ensure_checkout_is_clean():
  # Make sure no local mods:
  s = subprocess.check_output('git diff --shortstat', shell=True)
  if len(s) > 0:
    raise RuntimeError('git diff --shortstat is non-empty: got:\n%s' % s)

  # Make sure no untracked files:
  s = subprocess.check_output('git status', shell=True).decode('utf-8', errors='replace')
  if 'Untracked files:' in s:
    raise RuntimeError('git status shows untracked files: got:\n%s' % s)

  # Make sure we have all changes from origin:
  if 'is behind' in s:
    raise RuntimeError('git status shows not all changes pulled from origin; try running "git pull origin" in this branch: got:\n%s' % (s))

  # Make sure we no local unpushed changes (this is supposed to be a clean area):
  if 'is ahead' in s:
    raise RuntimeError('git status shows local commits; try running "git fetch origin", "git checkout ", "git reset --hard origin/" in this branch: got:\n%s' % (s))

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

# Checks the pom.xml for the release version.
# This method fails if the pom file has no SNAPSHOT version set ie.
# if the version is already on a release version we fail.
# Returns the next version string ie. 0.90.7
def find_release_version():
  with open('pom.xml', encoding='utf-8') as file:
    for line in file:
      match = re.search(r'<version>(.+)-SNAPSHOT</version>', line)
      if match:
        return match.group(1)
    raise RuntimeError('Could not find release version in branch')

# Stages the given files for the next git commit
def add_pending_files(*files):
  for file in files:
    if file:
      # print("Adding file: %s" % (file))
      run('git add %s' % (file))

# Updates documentation feature flags
def commit_feature_flags(release):
    run('git commit -m "Update Documentation Feature Flags [%s]"' % release)

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

if __name__ == "__main__":
  release_version = find_release_version()

  print('*** Preparing release version documentation: [%s]' % release_version)

  ensure_checkout_is_clean()

  pending_files = update_reference_docs(release_version)

  if pending_files:
    add_pending_files(*pending_files) # expects var args use * to expand
    commit_feature_flags(release_version)
  else:
    print('WARNING: no documentation references updates for release %s' % (release_version))

  print('*** Done.')

