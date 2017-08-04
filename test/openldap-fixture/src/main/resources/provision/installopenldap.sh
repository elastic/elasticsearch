#!/bin/bash

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

set -ex

VDIR=/vagrant
RESOURCES=$VDIR/src/main/resources
PROV_DIR=$RESOURCES/provision
SSL_DIR=/etc/ssl/private
OLDAP_PASSWORD=NickFuryHeartsES
OLDAP_DOMAIN=oldap.test.elasticsearch.com
OLDAP_DOMAIN_DN=dc=oldap,dc=test,dc=elasticsearch,dc=com

MARKER_FILE=/etc/marker

if [ -f $MARKER_FILE ]; then
  echo "Already provisioned..."
  exit 0;
fi

# Update package manager
apt-get update -qqy

# /dev/random produces output very slowly on Ubuntu VM's. Install haveged to increase entropy.
apt-get install -qqy haveged
haveged

# set the openldap configuration options
debconf-set-selections <<EOF
slapd slapd/password1 password $OLDAP_PASSWORD
slapd slapd/password2 password $OLDAP_PASSWORD
slapd slapd/domain string $OLDAP_DOMAIN
slapd shared/organization string test
EOF

# install openldap (slapd)
apt-get install -qqy slapd ldap-utils

# copy certs and update permissions
cp $PROV_DIR/*.pem $SSL_DIR
groupadd -f ssl-cert
chgrp ssl-cert $SSL_DIR
chgrp ssl-cert $SSL_DIR/key.pem
chgrp ssl-cert $SSL_DIR/cert.pem
chmod g+X $SSL_DIR
chmod g+r $SSL_DIR/key.pem
chmod g+r $SSL_DIR/cert.pem
adduser openldap ssl-cert
sudo -u openldap cat /etc/ssl/private/cert.pem

# restart to pick up new permissions
service slapd restart

# set TLS file locations
ldapmodify -Y EXTERNAL -H ldapi:/// <<EOF
dn: cn=config
add: olcTLSCertificateFile
olcTLSCertificateFile: $SSL_DIR/cert.pem
-
add: olcTLSCertificateKeyFile
olcTLSCertificateKeyFile: $SSL_DIR/key.pem
EOF

# add ldaps
cp /etc/default/slapd /etc/default/slapd.orig
cat /etc/default/slapd | sed -e "s/SLAPD_SERVICES=\"ldap:\/\/\/ ldapi:\/\/\/\"/SLAPD_SERVICES=\"ldap:\/\/\/ ldapi:\/\/\/ ldaps:\/\/\/\"/" > /tmp/slapd
mv /tmp/slapd /etc/default/slapd

# restart to listen on port 636
service slapd restart

# create the people container
ldapadd -D "cn=admin,dc=oldap,dc=test,dc=elasticsearch,dc=com" -w $OLDAP_PASSWORD <<EOF
dn: ou=people,$OLDAP_DOMAIN_DN
objectClass: organizationalUnit
ou: people
EOF

uidNum=1000

# add user entries
function add_person {
    local uid=$1; shift
    local name=$1; shift
    ldapadd -D "cn=admin,dc=oldap,dc=test,dc=elasticsearch,dc=com" -w $OLDAP_PASSWORD <<EOF
dn: uid=$uid,ou=people,$OLDAP_DOMAIN_DN
objectClass: top
objectClass: posixAccount
objectClass: inetOrgPerson
userPassword: $(slappasswd -s $OLDAP_PASSWORD)
uid: $uid
uidNumber: $((++uidNum))
gidNumber: $uidNum
homeDirectory: /home/$uid
mail: $name@$OLDAP_DOMAIN
cn: $name
sn: $name
EOF
}

add_person "kraken" "Commander Kraken"
add_person "hulk" "Bruce Banner"
add_person "hawkeye" "Clint Barton"
add_person "jarvis" "Jarvis"
add_person "blackwidow" "Natasha Romanova"
add_person "fury" "Nick Fury"
add_person "phil" "Phil Colson"
add_person "cap" "Steve Rogers"
add_person "thor" "Thor Odinson"
add_person "ironman" "Tony Stark"
add_person "odin" "Gods"
add_person "selvig" "Erik Selvig"

# add group entries
function add_group {
    local name=$1; shift
    local gid=$1; shift
    local uids=("$1"); shift

    local uidsSection=""
    for uid in ${uids[@]}
    do
        uidsSection=$uidsSection"memberUid: ${uid}"$'\n'
    done
    echo $uidsSection
    ldapadd -D "cn=admin,dc=oldap,dc=test,dc=elasticsearch,dc=com" -w $OLDAP_PASSWORD <<EOF
dn: cn=$name,ou=people,$OLDAP_DOMAIN_DN
cn: $name
objectClass: top
objectClass: posixGroup
gidNumber: $gid
$uidsSection
EOF
}

add_group "Hydra" "101" "kraken"

group_members=("hulk" "ironman" "selvig")
add_group "Geniuses" "102" "$(echo ${group_members[@]})"

group_members=("hulk" "hawkeye" "blackwidow" "fury" "phil" "cap" "thor" "ironman")
add_group "SHIELD" "103" "$(echo ${group_members[@]})"

group_members=("hulk" "thor" "ironman")
add_group "Philanthropists" "104" "$(echo ${group_members[@]})"

group_members=("hulk" "hawkeye" "blackwidow" "fury" "cap" "thor" "ironman")
add_group "Avengers" "105" "$(echo ${group_members[@]})"

group_members=("thor" "odin")
add_group "Gods" "106" "$(echo ${group_members[@]})"

group_members=("ironman")
add_group "Playboys" "107" "$(echo ${group_members[@]})"
add_group "Billionaries" "108" "$(echo ${group_members[@]})"

# search for users and print some of their attributes.
ldapsearch -x -LLL -b $OLDAP_DOMAIN_DN '(objectClass=person)' cn mail uid

# search for groups
ldapsearch -x -LLL -b $OLDAP_DOMAIN_DN '(objectClass=posixGroup)' memberUid

# touch the marker file
touch $MARKER_FILE
