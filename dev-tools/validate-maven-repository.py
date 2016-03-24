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

# Helper python script to check if a sonatype staging repo contains
# all the required files compared to a local repository
#
# The script does the following steps
#
# 1. Scans the local maven repo for all files in /org/elasticsearch
# 2. Opens a HTTP connection to the staging repo
# 3. Executes a HEAD request for each file found in step one
# 4. Compares the content-length response header with the real file size
# 5. Return an error if those two numbers differ
#
# A pre requirement to run this, is to find out via the oss.sonatype.org web UI, how that repo is named
# - After logging in you go to 'Staging repositories' and search for the one you just created
# - Click into the `Content` tab
# - Open any artifact (not a directory)
# - Copy the link of `Repository Path` on the right and reuse that part of the URL
#
# Alternatively you can just use the name of the repository and reuse the rest (ie. the repository
# named for the example below would have been named orgelasticsearch-1012)
#
#
# Example call
#   python dev-tools/validate-maven-repository.py /path/to/repo/org/elasticsearch/ \
#          https://oss.sonatype.org/service/local/repositories/orgelasticsearch-1012/content/org/elasticsearch

import sys
import os
import httplib
import urlparse
import re

# Draw a simple progress bar, a couple of hundred HEAD requests might take a while
# Note, when drawing this, it uses the carriage return character, so you should not
# write anything in between
def drawProgressBar(percent, barLen = 40):
    sys.stdout.write("\r")
    progress = ""
    for i in range(barLen):
        if i < int(barLen * percent):
            progress += "="
        else:
            progress += " "
    sys.stdout.write("[ %s ] %.2f%%" % (progress, percent * 100))
    sys.stdout.flush()

if __name__ == "__main__":
  if len(sys.argv) != 3:
    print 'Usage: %s <localRep> <stagingRepo> [user:pass]' % (sys.argv[0])
    print ''
    print 'Example: %s /tmp/my-maven-repo/org/elasticsearch https://oss.sonatype.org/service/local/repositories/orgelasticsearch-1012/content/org/elasticsearch' % (sys.argv[0])
  else:
    sys.argv[1] = re.sub('/$', '', sys.argv[1])
    sys.argv[2] = re.sub('/$', '', sys.argv[2])

    localMavenRepo = sys.argv[1]
    endpoint = sys.argv[2]

    filesToCheck = []
    foundSignedFiles = False

    for root, dirs, files in os.walk(localMavenRepo):
      for file in files:
        # no metadata files (they get renamed from maven-metadata-local.xml to maven-metadata.xml while deploying)
        # no .properties and .repositories files (they don't get uploaded)
        if not file.startswith('maven-metadata') and not file.endswith('.properties') and not file.endswith('.repositories'):
          filesToCheck.append(os.path.join(root, file))
        if file.endswith('.asc'):
          foundSignedFiles = True

    print "Need to check %i files" % len(filesToCheck)
    if not foundSignedFiles:
      print '### Warning: No signed .asc files found'

    # set up http
    parsed_uri = urlparse.urlparse(endpoint)
    domain = parsed_uri.netloc
    if parsed_uri.scheme == 'https':
      conn = httplib.HTTPSConnection(domain)
    else:
      conn = httplib.HTTPConnection(domain)
    #conn.set_debuglevel(5)

    drawProgressBar(0)
    errors = []
    for idx, file in enumerate(filesToCheck):
      request_uri = parsed_uri.path + file[len(localMavenRepo):]
      conn.request("HEAD", request_uri)
      res = conn.getresponse()
      res.read() # useless call for head, but prevents httplib.ResponseNotReady raise

      absolute_url = parsed_uri.scheme + '://' + parsed_uri.netloc + request_uri
      if res.status == 200:
        content_length = res.getheader('content-length')
        local_file_size = os.path.getsize(file)
        if int(content_length) != int(local_file_size):
          errors.append('LENGTH MISMATCH:   %s differs in size. local %s <=> %s remote' % (absolute_url, content_length, local_file_size))
      elif res.status == 404:
        errors.append('MISSING:           %s' % absolute_url)
      elif res.status == 301 or res.status == 302:
        errors.append('REDIRECT:          %s to %s' % (absolute_url, res.getheader('location')))
      else:
        errors.append('ERROR:             %s http response: %s %s' %(absolute_url, res.status, res.reason))

      # update progressbar at the end
      drawProgressBar((idx+1)/float(len(filesToCheck)))

    print

    if len(errors) != 0:
      print 'The following errors occurred (%s out of %s files)' % (len(errors), len(filesToCheck))
      print
      for error in errors:
        print error
      sys.exit(-1)
