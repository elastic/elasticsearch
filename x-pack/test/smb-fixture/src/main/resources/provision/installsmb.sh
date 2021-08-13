#! /bin/bash

# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0; you may not use this file except in compliance with the Elastic License
# 2.0.

set -ex

VDIR=/fixture
RESOURCES=$VDIR/src/main/resources
CERTS_DIR=$RESOURCES/certs
SSL_DIR=/var/lib/samba/private/tls

# install ssl certs
mkdir -p $SSL_DIR
cp $CERTS_DIR/*.pem $SSL_DIR
chmod 600 $SSL_DIR/key.pem

mkdir -p /etc/ssl/certs/
cat $SSL_DIR/ca.pem >> /etc/ssl/certs/ca-certificates.crt

mv /etc/samba/smb.conf /etc/samba/smb.conf.orig

samba-tool domain provision --server-role=dc --use-rfc2307 --dns-backend=SAMBA_INTERNAL --realm=AD.TEST.ELASTICSEARCH.COM --domain=ADES --adminpass=Passw0rd --use-ntvfs

cp /var/lib/samba/private/krb5.conf /etc/krb5.conf

service samba-ad-dc restart

# Add users
samba-tool user add ironman Passw0rd --surname=Stark --given-name=Tony --job-title=CEO
samba-tool user add hulk Passw0rd --surname=Banner --given-name=Bruce
samba-tool user add phil Passw0rd --surname=Coulson --given-name=Phil
samba-tool user add cap Passw0rd --surname=Rogers --given-name=Steve
samba-tool user add blackwidow Passw0rd --surname=Romanoff --given-name=Natasha
samba-tool user add hawkeye Passw0rd --surname=Barton --given-name=Clint
samba-tool user add Thor Passw0rd
samba-tool user add selvig Passw0rd --surname=Selvig --given-name=Erik
samba-tool user add Odin Passw0rd
samba-tool user add Jarvis Passw0rd
samba-tool user add kraken Passw0rd --surname=Kraken --given-name=Commander
samba-tool user add fury Passw0rd --surname=Fury --given-name=Nick

# Add groups
samba-tool group add SHIELD
samba-tool group add Avengers
samba-tool group add Supers
samba-tool group add Geniuses
samba-tool group add Playboys
samba-tool group add Philanthropists
samba-tool group add Gods
samba-tool group add Billionaires
samba-tool group add "World Security Council"
samba-tool group add Hydra

# Group membership
samba-tool group addmembers "SHIELD" Thor,hawkeye,blackwidow,cap,phil,hulk,ironman
samba-tool group addmembers "Avengers" Thor,hawkeye,blackwidow,cap,hulk,ironman
samba-tool group addmembers "Supers" Avengers
samba-tool group addmembers "Geniuses" selvig,hulk,ironman
samba-tool group addmembers "Playboys" ironman
samba-tool group addmembers "Philanthropists" Thor,hulk,ironman
samba-tool group addmembers "Gods" Thor,Odin
samba-tool group addmembers "Billionaires" ironman
samba-tool group addmembers "World Security Council" fury
samba-tool group addmembers "Hydra" kraken

# update UPN
cat > /tmp/entrymods << EOL
dn: CN=Erik Selvig,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com
changetype: modify
replace: userPrincipalName
userPrincipalName: erik.selvig@ad.test.elasticsearch.com

dn: CN=Bruce Banner,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com
changetype: modify
add: seeAlso
seeAlso: CN=Avengers,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com
EOL

ldapmodify -D Administrator@ad.test.elasticsearch.com -w Passw0rd -H ldaps://127.0.0.1:636 -f /tmp/entrymods -v

