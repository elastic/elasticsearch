#!/bin/sh

 # Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 # or more contributor license agreements. Licensed under the "Elastic License
 # 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 # Public License v 1"; you may not use this file except in compliance with, at
 # your election, the "Elastic License 2.0", the "GNU Affero General Public
 # License v3.0 only", or the "Server Side Public License, v 1".

set -e

# KDC installation steps and considerations based on https://web.mit.edu/kerberos/krb5-latest/doc/admin/install_kdc.html
# and helpful input from https://help.ubuntu.com/community/Kerberos

RESOURCES=/fixture
PROV_DIR=$RESOURCES/provision
ENVPROP_FILE=$RESOURCES/env.properties
LOCALSTATEDIR=/etc
LOGDIR=/var/log/krb5

MARKER_FILE=/etc/marker

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


# Transfer and interpolate the kdc.conf
mkdir -p $LOCALSTATEDIR/krb5kdc
cp $PROV_DIR/kdc.conf.template $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${REALM_NAME}/'$REALM_NAME'/g' $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${KDC_NAME}/'$KDC_NAME'/g' $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${BUILD_ZONE}/'$BUILD_ZONE'/g' $LOCALSTATEDIR/krb5kdc/kdc.conf
sed -i 's/${ELASTIC_ZONE}/'$ELASTIC_ZONE'/g' $LOCALSTATEDIR/krb5.conf

# Touch logging locations
mkdir -p $LOGDIR
touch $LOGDIR/kadmin.log
touch $LOGDIR/krb5kdc.log
touch $LOGDIR/krb5lib.log

# Create kerberos database with stash file and garbage password
kdb5_util create -s -r $REALM_NAME -P zyxwvutsrpqonmlk9876

# Set up admin acls
cat << EOF > /var/lib/krb5kdc/kadm5.acl
*/admin@$REALM_NAME	*
*/*@$REALM_NAME		i
EOF

# Create admin principal
kadmin.local -q "addprinc -pw elastic admin/admin@$REALM_NAME"
kadmin.local -q "ktadd -k /etc/admin.keytab admin/admin@$REALM_NAME"

# Create a link so addprinc.sh is on path
ln -s $PROV_DIR/addprinc.sh /usr/bin/
