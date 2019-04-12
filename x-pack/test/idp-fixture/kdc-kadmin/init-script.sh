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

KEYTABS_DIR=/fixture/build/shared/keytabs/
KRB_CONF_DIR=/fixture/build/shared/krb-conf/

echo "==================================================================================="
echo "Kerberos KDC and Kadmin configuration"
KADMIN_PRINCIPAL_FULL=$KADMIN_PRINCIPAL@$REALM

echo "REALM: $REALM"
echo "KADMIN_PRINCIPAL_FULL: $KADMIN_PRINCIPAL_FULL"
echo "KADMIN_PASSWORD: $KADMIN_PASSWORD"
echo ""

KDC_KADMIN_SERVER=$(hostname -f)
tee /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = $REALM
        dns_lookup_realm = false
        dns_lookup_kdc = false
        rdns = false
        ticket_lifetime = 24h
        forwardable = true
        udp_preference_limit = 1

[realms]
    $REALM = {
        kdc_ports = 88,750
        kadmind_port = 749
        kdc = $KDC_KADMIN_SERVER
        admin_server = $KDC_KADMIN_SERVER
    }
EOF
echo ""

tee /etc/krb5kdc/kdc.conf <<EOF
[realms]
    $REALM = {
        acl_file = /etc/krb5kdc/kadm5.acl
        max_renewable_life = 7d 0h 0m 0s
        supported_enctypes = $SUPPORTED_ENCRYPTION_TYPES
        default_principal_flags = +preauth
    }
EOF
echo ""

echo "==================================================================================="
echo "ACL"

tee /etc/krb5kdc/kadm5.acl <<EOF
$KADMIN_PRINCIPAL_FULL *
noPermissions@$REALM X
EOF
echo ""

MASTER_PASSWORD=$(tr -cd '[:alnum:]' < /dev/urandom | fold -w30 | head -n1)
# This command also starts the krb5-kdc and krb5-admin-server services
krb5_newrealm <<EOF
$MASTER_PASSWORD
$MASTER_PASSWORD
EOF
echo ""

echo "Adding $KADMIN_PRINCIPAL principal"
kadmin.local -q "delete_principal -force $KADMIN_PRINCIPAL_FULL"
echo ""
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD $KADMIN_PRINCIPAL_FULL"
echo ""

echo "Adding noPermissions principal"
kadmin.local -q "delete_principal -force noPermissions@$REALM"
echo ""
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD noPermissions@$REALM"
echo ""

echo "Adding a user for bind"
kadmin.local -q "delete_principal -force kerb-bind-user@$REALM"
echo ""
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD kerb-bind-user@$REALM"
echo ""

echo "Adding a user for bind using Keytab"
kadmin.local -q "delete_principal -force kerb-ktab-bind-user@$REALM"
echo ""
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD kerb-ktab-bind-user@$REALM"
echo ""

echo "Adding LDAP Service principal"
kadmin.local -q "delete_principal -force ldap/localhost@$REALM"
echo ""
kadmin.local -q "addprinc -pw $KADMIN_PASSWORD ldap/localhost@$REALM"
echo ""

echo "Creating keytab for LDAP"
rm -rf $KEYTABS_DIR/ldap.keytab
echo ""
kadmin.local -q "ktadd -k $KEYTABS_DIR/ldap.keytab ldap/localhost@$REALM"
echo ""

echo "Creating keytab for ES bind"
rm -rf $KEYTABS_DIR/es-bind.keytab
echo ""
kadmin.local -q "ktadd -k $KEYTABS_DIR/es-bind.keytab kerb-ktab-bind-user@$REALM"
echo ""

echo "List principals"
echo ""
kadmin.local -q "list_principals *"
echo ""

echo "==================================================================================="
echo "Generate LDAP krb5.conf"

tee $KRB_CONF_DIR/ldap-krb5.conf <<EOF
[libdefaults]
        default_realm = $REALM
        dns_lookup_kdc = false
        dns_lookup_kdc = false
        rdns = false
        ticket_lifetime = 24h
        forwardable = true
        udp_preference_limit = 1

[realms]
        $REALM = {
                kdc = 127.0.0.1:88
                kdc = kdc-kadmin:88
                admin_server = kdc-kadmin
        }

[domain_realm]
        .localhost = $REALM
        localhost = $REALM
EOF
echo ""

echo "==================================================================================="
echo "Generate krb5.conf.template which is used by tests"

tee $KRB_CONF_DIR/krb5.conf.template <<EOF
[libdefaults]
        default_realm = $REALM
        dns_lookup_kdc = false
        dns_lookup_kdc = false
        rdns = false
        ticket_lifetime = 24h
        forwardable = true
        udp_preference_limit = 1

[realms]
        $REALM = {
                kdc = 127.0.0.1:\${MAPPED_PORT}
                kdc = kdc-kadmin:\${MAPPED_PORT}
                admin_server = kdc-kadmin
        }

[domain_realm]
        .localhost = $REALM
        localhost = $REALM
EOF
echo ""

echo "==================================================================================="
echo "Start the services ..."

# We want the container to keep running until we explicitly kill it.
# So the last command cannot immediately exit. See
#   https://docs.docker.com/engine/reference/run/#detached-vs-foreground
# for a better explanation.

krb5kdc
kadmind -nofork

touch /tmp/kerb.started

sleep infinity
