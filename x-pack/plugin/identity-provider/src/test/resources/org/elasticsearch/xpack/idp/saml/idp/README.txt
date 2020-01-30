## Create JKS keystore with multiple entries
keytool -genkey -alias signing1 -keyalg RSA -keysize 2048 -keystore multi_signing.jks -storepass signing -dname "CN=saml1-test"

keytool -genkey -alias signing2 -keyalg RSA -keysize 2048 -keystore multi_signing.jks -storepass signing -dname "CN=saml2-test"

keytool -genkey -alias signing3 -keyalg RSA -keysize 2048 -keystore multi_signing.jks -storepass signing -dname "CN=saml3-test"

keytool -genkey -alias signing4 -keyalg EC -keysize 256 -keystore multi_signing.jks -storepass signing -dname "CN=saml4-test"

## Create a PKCS12 with only signing1
keytool -importkeystore -srckeystore multi_signing.jks  -destkeystore signing.p12 -deststoretype PKCS12 -deststorepass signing -destkeypass signing -alias signing1

## Convert the keystore to a PKCS#12 one
keytool -importkeystore -srckeystore multi_signing.jks  -destkeystore multi_signing.p12 -deststoretype PKCS12 -deststorepass signing -destkeypass signing

## Export all keys (openssl pkcs12 doesn't have an `-alias` flag :( )
openssl pkcs12 -in multi_signing.p12 -nocerts -nodes -out all_keys

## Copy and paste the private keys files from all_keys into signing{1,2,3,4}.key

## Export all certificates (openssl pkcs12 doesn't have an `-alias` flag :( )
openssl pkcs12 -in multi_signing.p12 -nokeys -out all_certs

## Copy and paste the certificates into signing{1,2,3,4}.crt
