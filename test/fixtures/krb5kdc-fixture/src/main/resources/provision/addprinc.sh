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

if [[ $# -lt 1 ]]; then
  echo 'Usage: addprinc.sh <principalNameNoRealm>'
  exit 1
fi

PRINC="$1"
USER=$(echo $PRINC | tr "/" "_")

VDIR=/vagrant
RESOURCES=$VDIR/src/main/resources
PROV_DIR=$RESOURCES/provision
ENVPROP_FILE=$RESOURCES/env.properties
BUILD_DIR=$VDIR/build
CONF_DIR=$BUILD_DIR/conf
KEYTAB_DIR=$BUILD_DIR/keytabs
LOCALSTATEDIR=/etc
LOGDIR=/var/log/krb5

mkdir -p $KEYTAB_DIR

REALM=$(cat $ENVPROP_FILE | grep realm= | head -n 1 | cut -d '=' -f 2)

ADMIN_PRIN=admin/admin@$REALM
ADMIN_KTAB=$LOCALSTATEDIR/admin.keytab

USER_PRIN=$PRINC@$REALM
USER_KTAB=$LOCALSTATEDIR/$USER.keytab

if [ -f $USER_KTAB ]; then
  echo "Principal '${PRINC}@${REALM}' already exists. Re-copying keytab..."
else
  echo "Provisioning '${PRINC}@${REALM}' principal and keytab..."
  sudo kadmin -p $ADMIN_PRIN -kt $ADMIN_KTAB -q "addprinc -randkey $USER_PRIN"
  sudo kadmin -p $ADMIN_PRIN -kt $ADMIN_KTAB -q "ktadd -k $USER_KTAB $USER_PRIN"
fi

sudo cp $USER_KTAB $KEYTAB_DIR/$USER.keytab
