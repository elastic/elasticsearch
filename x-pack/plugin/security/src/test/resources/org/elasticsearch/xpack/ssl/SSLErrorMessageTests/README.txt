#
# NOTE: This readme is also an executable shell script.
#       Run it with bash ./README.txt
#

#
# Make sure we can call certutil
#
[ -n "$ES_HOME" ] || { printf '%s: $ES_HOME is not set\n' "$0" ; exit 1; }
[ -d "$ES_HOME" ] || { printf '%s: $ES_HOME is not a directory\n' "$0" ; exit 1; }

function certutil() { "$ES_HOME/bin/elasticsearch-certutil" "$@"; }

#
# Helper functions to generate files & convert file types
#
function new-p12-ca() {
    local P12File="$1"
    local P12Pass="$2"
    local CaDn="$3"

    certutil ca --ca-dn="$CaDn" --days=5000 --out ${PWD}/$P12File --pass="$P12Pass"
}

function new-p12-cert() {
    local CertFile="$1"
    local CertPass="$2"
    local CertName="$3"
    local CaFile="$4"
    local CaPass="$5"
    shift 5

    certutil cert --ca="${PWD}/$CaFile" --ca-pass="$CaPass" --days=5000 --out ${PWD}/$CertFile --pass="$CertPass" --name="$CertName" "$@"
}

function new-pem-cert() {
    local CrtFile="$1"
    local KeyFile="$2"
    local KeyPass="$3"
    local CertName="$4"
    local CaFile="$5"
    local CaPass="$6"
    shift 6

    local ZipFile=${PWD}/$CertName.zip
    local PassOpt=""
    if [ -n "$KeyPass" ]
    then
        PassOpt="--pass=$KeyPass"
    fi

    certutil cert --pem \
        --ca="${PWD}/$CaFile" --ca-pass="$CaPass" \
        --name="$CertName" --out $ZipFile \
        --days=5000 $PassOpt \
        "$@"
    unzip -p $ZipFile "$CertName/$CertName.crt" > $CrtFile
    unzip -p $ZipFile "$CertName/$CertName.key" > $KeyFile
    rm $ZipFile
}

function p12-to-jks() {
    local P12File="$1"
    local P12Pass="$2"
    local JksFile="$3"
    local JksPass="$4"
    
    keytool -importkeystore -srckeystore "${PWD}/$P12File" -srcstorepass "$P12Pass" \
        -destkeystore "${PWD}/$JksFile"  -deststoretype JKS -deststorepass "$JksPass"
}

function p12-export-cert() {
    local P12File="$1"
    local P12Pass="$2"
    local P12Name="$3"
    local PemFile="$4"
    
    keytool -exportcert -keystore "${PWD}/$P12File" -storepass "$P12Pass" -alias "$P12Name" \
        -rfc -file "${PWD}/$PemFile" 
}

function p12-export-pair() {
    local P12File="$1"
    local P12Pass="$2"
    local P12Name="$3"
    local CrtFile="$4"
    local KeyFile="$5"

    local TmpFile="${PWD}/$(basename $P12File .p12).tmp.p12"
    
    # OpenSSL doesn't have a way to export a single entry
    # Keytool doesn't have a way to export keys
    # So we use keytool to export the whole entry to a temporary PKCS#12 and then use openssl to export that to PEM

    keytool -importkeystore -srckeystore "${PWD}/$P12File" -srcstorepass "$P12Pass" -srcalias "$P12Name" \
        -destkeystore "$TmpFile" -deststorepass "tmp_password" 

    # This produces an unencrypted PKCS#1 key. Use other commands to convert it if needed
    # The sed is to skip "BagAttributes" which we don't need
    openssl pkcs12 -in "$TmpFile" -nodes   -nocerts -passin "pass:tmp_password" | sed -n -e'/^-----/,/^-----/p' > $KeyFile
    openssl pkcs12 -in "$TmpFile" -clcerts -nokeys  -passin "pass:tmp_password" | sed -n -e'/^-----/,/^-----/p' > $CrtFile

    rm $TmpFile
}

function no-op() {
#
# Create a CA in PKCS#12
#
new-p12-ca ca1.p12 "ca1-p12-password" 'CN=Certificate Authority 1,OU=ssl-error-message-test,DC=elastic,DC=co'

# Make a JKS version of the CA
p12-to-jks ca1.p12 "ca1-p12-password" ca1.jks "ca1-jks-password" 

# Make a PEM version of the CA cert
p12-export-cert ca1.p12 "ca1-p12-password" "ca" ca1.crt 

#
# Create a Cert/Key Pair in PKCS#12
#  - "cert1a" is signed by "ca1"
#  - "cert1a.p12" is password protected, and can act as a keystore or truststore
#
new-p12-cert cert1a.p12 "cert1a-p12-password" "cert1a" "ca1.p12" "ca1-p12-password"

# Convert to JKS
#  - "cert1a.jks" is password protected, and can act as a keystore or truststore
p12-to-jks cert1a.p12 "cert1a-p12-password" cert1a.jks "cert1a-jks-password" 

# Convert to PEM
#  - "cert1a.key" is an (unprotected) PKCS#1 key
p12-export-pair cert1a.p12 "cert1a-p12-password" "cert1a" cert1a.crt cert1a.key 
}

#
# Create a Cert/Key Pair in PEM with a hostname "not.this.host"
#  - "not_this_host.crt" is signed by "ca1"
#
new-pem-cert not-this-host.crt not-this-host.key "" "not-this-host" "ca1.p12" "ca1-p12-password" --dns not.this.host
