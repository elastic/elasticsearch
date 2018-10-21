#!/bin/bash

# Licensed to Elasticsearch under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

# KDC installation steps and considerations based on https://web.mit.edu/kerberos/krb5-latest/doc/admin/install_kdc.html
# and helpful input from https://help.ubuntu.com/community/Kerberos

VDIR=/vagrant
RESOURCES=$VDIR/src/main/resources
PROV_DIR=$RESOURCES/provision
ENVPROP_FILE=$RESOURCES/env.properties
BUILD_DIR=$VDIR/build
CONF_DIR=$BUILD_DIR/conf
KEYTAB_DIR=$BUILD_DIR/keytabs
LOCALSTATEDIR=/etc
LOGDIR=/var/log/krb5

MARKER_FILE=/etc/marker

# Output location for our rendered configuration files and keytabs
mkdir -p $BUILD_DIR
rm -rf $BUILD_DIR/*
mkdir -p $CONF_DIR
mkdir -p $KEYTAB_DIR

if [ -f $MARKER_FILE ]; then
  echo "Already provisioned..."
  echo "Recopying configuration files..."
  cp $LOCALSTATEDIR/krb5.conf $CONF_DIR/krb5.conf
  cp $LOCALSTATEDIR/krb5kdc/kdc.conf $CONF_DIR/kdc.conf
  exit 0;
fi

# Pull environment information
REALM_NAME=$(cat $ENVPROP_FILE | grep realm= | cut -d '=' -f 2)
KDC_NAME=$(cat $ENVPROP_FILE | grep kdc= | cut -d '=' -f 2)
BUILD_ZONE=$(cat $ENVPROP_FILE | grep zone= | cut -d '=' -f 2)
ELASTIC_ZONE=$(echo $BUILD_ZONE | cut -d '.' -f 1,2)

# Transfer and interpolate krb5.conf
cp $PROV_DIR/krb5.conf.template $LOCALSTATEDIR/krb5.conf
sed -i 's/${REALM_NAME}/'$REALM_NAME'/g' $LOCALSTATEDIR/krb5.conf
sed -i 's/${KDC_NAME}/'$KDC_NAME'/g' $LOCALSTATEDIR/krb5.conf
sed -i 's/${BUILD_ZONE}/'$BUILD_ZONE'/g' $LOCALSTATEDIR/krb5.conf
sed -i 's/${ELASTIC_ZONE}/'$ELASTIC_ZONE'/g' $LOCALSTATEDIR/krb5.conf
cp $LOCALSTATEDIR/krb5.conf $CONF_DIR/krb5.conf

# Transfer and interpolate the kdc.conf
mkdir -p $LOCALSTATEDIR/krb5kdc
cp $PROV_DIR/kdc.conf.template $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${REALM_NAME}/'$REALM_NAME'/g' $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${KDC_NAME}/'$KDC_NAME'/g' $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${BUILD_ZONE}/'$BUILD_ZONE'/g' $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${ELASTIC_ZONE}/'$ELASTIC_ZONE'/g' $LOCALSTATEDIR/krb5.conf
cp $LOCALSTATEDIR/krb5kdc/kdc.conf $CONF_DIR/kdc.conf

# Touch logging locations
mkdir -p $LOGDIR
touch $LOGDIR/kadmin.log
touch $LOGDIR/krb5kdc.log
touch $LOGDIR/krb5lib.log

# Update package manager
apt-get update -qqy

# Installation asks a bunch of questions via debconf. Set the answers ahead of time
debconf-set-selections <<< "krb5-config krb5-config/read_conf boolean true"
debconf-set-selections <<< "krb5-config krb5-config/kerberos_servers string $KDC_NAME"
debconf-set-selections <<< "krb5-config krb5-config/add_servers boolean true"
debconf-set-selections <<< "krb5-config krb5-config/admin_server string $KDC_NAME"
debconf-set-selections <<< "krb5-config krb5-config/add_servers_realm string $REALM_NAME"
debconf-set-selections <<< "krb5-config krb5-config/default_realm string $REALM_NAME"
debconf-set-selections <<< "krb5-admin-server krb5-admin-server/kadmind boolean true"
debconf-set-selections <<< "krb5-admin-server krb5-admin-server/newrealm note"
debconf-set-selections <<< "krb5-kdc krb5-kdc/debconf boolean true"
debconf-set-selections <<< "krb5-kdc krb5-kdc/purge_data_too boolean false"

# Install krb5 packages
apt-get install -qqy krb5-{admin-server,kdc}

# /dev/random produces output very slowly on Ubuntu VM's. Install haveged to increase entropy.
apt-get install -qqy haveged
haveged

# Create kerberos database with stash file and garbage password
kdb5_util create -s -r $REALM_NAME -P zyxwvutsrpqonmlk9876

# Set up admin acls
cat << EOF > /etc/krb5kdc/kadm5.acl
*/admin@$REALM_NAME	*
*/*@$REALM_NAME		i
EOF

# Create admin principal
kadmin.local -q "addprinc -pw elastic admin/admin@$REALM_NAME"
kadmin.local -q "ktadd -k /etc/admin.keytab admin/admin@$REALM_NAME"

# Start Kerberos Services
krb5kdc
kadmind

# Mark that the vm is already provisioned
touch $MARKER_FILE