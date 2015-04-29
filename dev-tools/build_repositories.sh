#!/bin/bash

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


# This tool uploads the debian and RPM packages to the specified S3 buckets
# The packages get signed as well
# A requirement is the sync of the existing repository

set -e

###################
## environment variables
##
## required
##
##  GPG_PASSPHRASE:         Passphrase of your GPG key
##  GPG_KEY_ID:             Key id of your GPG key
##  AWS_ACCESS_KEY_ID:      AWS access key id
##  AWS_SECRET_ACCESS_KEY:  AWS secret access key
##  S3_BUCKET_SYNC_TO       Bucket to write packages to, should be set packages.elasticsearch.org for a regular release
##
##
## optional
##
##  S3_BUCKET_SYNC_FROM     Bucket to read packages from, defaults to packages.elasticsearch.org
##  KEEP_DIRECTORIES        Allows to keep all the generated directory structures for debugging
##  GPG_KEYRING             Configure GPG keyring home, defaults to ~/.gnupg/
##
###################



###################
## configuration
###################

# No trailing slashes!
if [ -z $S3_BUCKET_SYNC_FROM ] ; then
  S3_BUCKET_SYNC_FROM="packages.elasticsearch.org"
fi
if [ ! -z $GPG_KEYRING ] ; then
  GPG_HOMEDIR="--homedir ${GPG_KEYRING}"
fi

###################
## parameters
###################

# Must be major and minor version, i.e. 1.5 instead of 1.5.0
version=$1

###################
## prerequisites
###################

if [ "$#" != "1" ] || [ "x$1" == "x-h" ] || [ "x$1" == "x--help" ] ; then
  echo "Usage: $0 version"
  echo
  echo " version: The elasticsearch major and minor version, i.e. 1.5"
  exit
fi

echo "Checking for correct environment"

error=""

if [ -z "$GPG_PASSPHRASE" ] ; then
  echo "Environment variable GPG_PASSPHRASE is not set"
  error="true"
fi

if [ -z "$S3_BUCKET_SYNC_TO" ] ; then
  echo "Environment variable S3_BUCKET_SYNC_TO is not set"
  error="true"
fi

if [ -z "$GPG_KEY_ID" ] ; then
  echo "Environment variable GPG_KEY_ID is not set"
  error="true"
fi

if [ -z "$AWS_ACCESS_KEY_ID" ] ; then
  echo "Environment variable AWS_ACCESS_KEY_ID is not set"
  error="true"
fi

if [ -z "$AWS_SECRET_ACCESS_KEY" ] ; then
  echo "Environment variable AWS_SECRET_ACCESS_KEY is not set"
  error="true"
fi

if [ "x$error" == "xtrue" ] ; then
  echo "Please set all of the above environment variables first. Exiting..."
  exit
fi

echo "Checking for available command line tools:"

check_for_command() {
  echo -n "  $1"
  if [ -z "`which $1`" ]; then
    echo "NO"
    error="true"
  else
    echo "ok"
  fi
}

error=""
check_for_command "createrepo"
check_for_command "s3cmd"
check_for_command "apt-ftparchive"
check_for_command "gpg"
check_for_command "expect" # needed for the RPM plugin

if [ "x$error" == "xtrue" ] ; then
  echo "Please install all of the above tools first. Exiting..."
  exit
fi

###################
## setup
###################
tempdir=`mktemp -d /tmp/elasticsearch-repo.XXXX`
mkdir -p $tempdir

# create custom s3cmd conf, in case s3cmd does not support --aws-secret-key like on ubuntu
( cat <<EOF
[default]
access_key = $AWS_ACCESS_KEY_ID
secret_key = $AWS_SECRET_ACCESS_KEY
EOF
) > $tempdir/.s3cmd
s3cmd="s3cmd -c $tempdir/.s3cmd"

###################
## RPM
###################

centosdir=$tempdir/repository/elasticsearch/$version/centos
mkdir -p $centosdir

echo "RPM: Syncing repository for version $version into $centosdir"
$s3cmd sync s3://$S3_BUCKET_SYNC_FROM/elasticsearch/$version/centos/ $centosdir

rpm=target/rpm/elasticsearch/RPMS/noarch/elasticsearch*.rpm
echo "RPM: Copying $rpm into $centosdor"
cp $rpm $centosdir

echo "RPM: Running createrepo in $centosdir"
createrepo --update $centosdir

echo "RPM: Resigning repomd.xml"
rm -f $centosdir/repodata/repomd.xml.asc
gpg $GPG_HOMEDIR --passphrase "$GPG_PASSPHRASE" -a -b -o $centosdir/repodata/repomd.xml.asc $centosdir/repodata/repomd.xml

echo "RPM: Syncing back repository for $version into S3 bucket $S3_BUCKET_SYNC_TO"
$s3cmd sync -P $centosdir/ s3://$S3_BUCKET_SYNC_TO/elasticsearch/$version/centos/

###################
## DEB
###################

deb=target/releases/elasticsearch*.deb

echo "DEB: Creating repository directory structure"

if [ -z $tempdir ] ; then
  echo "DEB: Could not create tempdir directory name, exiting"
  exit
fi

debbasedir=$tempdir/repository/elasticsearch/$version/debian
mkdir -p $debbasedir


echo "DEB: Syncing debian repository of version $version to $debbasedir"
# sync all former versions into directory
$s3cmd sync s3://$S3_BUCKET_SYNC_FROM/elasticsearch/$version/debian/ $debbasedir

# create directories in case of a new release so that syncing did not create this structure
mkdir -p $debbasedir/dists/stable/main/binary-all
mkdir -p $debbasedir/dists/stable/main/binary-i386
mkdir -p $debbasedir/dists/stable/main/binary-amd64
mkdir -p $debbasedir/.cache
mkdir -p $debbasedir/pool/main

# create elasticsearch-1.4.conf
( cat <<EOF
APT::FTPArchive::Release::Origin "Elasticsearch";
APT::FTPArchive::Release::Label "Elasticsearch ${version}.x";
APT::FTPArchive::Release::Suite "stable";
APT::FTPArchive::Release::Codename "stable";
APT::FTPArchive::Release::Architectures "i386 amd64";
APT::FTPArchive::Release::Components "main";
APT::FTPArchive::Release::Description "Elasticsearch repo for all ${version}.x packages";
EOF
) > $tempdir/elasticsearch-$version-releases.conf

# create packages file using apt-ftparchive
mkdir -p $debbasedir/dists/stable/main/binary-all
mkdir -p $debbasedir/pool/main/e/elasticsearch

echo "DEB: Copying $deb to elasticsearch repo directory"
cp $deb $debbasedir/pool/main/e/elasticsearch

echo "DEB: Creating new Packages and Release files"
cd $debbasedir
apt-ftparchive packages pool > dists/stable/main/binary-all/Packages
cat dists/stable/main/binary-all/Packages | gzip -9 > dists/stable/main/binary-all/Packages.gz
cp dists/stable/main/binary-all/Packages* dists/stable/main/binary-i386/
cp dists/stable/main/binary-all/Packages* dists/stable/main/binary-amd64/
apt-ftparchive -c $tempdir/elasticsearch-$version-releases.conf release $debbasedir/dists/stable/ > $debbasedir/dists/stable/Release

echo "DEB: Signing newly created release file at $debbasedir/dists/stable/Release.gpg"
rm -f $debbasedir/dists/stable/Release.gpg
gpg $GPG_HOMEDIR --passphrase "$GPG_PASSPHRASE" -a -b -o $debbasedir/dists/stable/Release.gpg $debbasedir/dists/stable/Release

# upload to S3
echo "DEB: Uploading to S3 bucket to $S3_BUCKET_SYNC_TO"
$s3cmd sync -P $debbasedir/ s3://$S3_BUCKET_SYNC_TO/elasticsearch/$version/debian/

# back to original dir
cd -

# delete directories unless configured otherwise
if [ -z $KEEP_DIRECTORIES ] ; then
  echo "Done! Deleting repository directories at $tempdir"
  rm -fr $tempdir
else
  echo "Done! Keeping repository directories at $tempdir"
fi
