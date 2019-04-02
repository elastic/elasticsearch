#!/bin/bash
#
# This is README describes how the certificates in this directory were created.
# This file can also be executed as a script
#

# 1. Create first CA PEM ("ca1")

elasticsearch-certutil ca --pem --out ca1.zip --days 9999 --ca-dn "CN=Test CA 1"
unzip ca1.zip 
mv ca ca1

# 2. Create first CA PEM ("ca2")

elasticsearch-certutil ca --pem --out ca2.zip --days 9999 --ca-dn "CN=Test CA 2"
unzip ca2.zip 
mv ca ca2

# 3. Create first CA PEM ("ca3")

elasticsearch-certutil ca --pem --out ca3.zip --days 9999 --ca-dn "CN=Test CA 3"
unzip ca3.zip 
mv ca ca3

# 4. Create "cert1" PEM

elasticsearch-certutil cert --pem --out cert1.zip --name cert1 --ip 127.0.0.1 --dns localhost --days 9999 --ca-key ca1/ca.key --ca-cert ca1/ca.crt
unzip cert1.zip

# 5. Create "cert2" PEM (same as cert1, but with a password)

elasticsearch-certutil cert --pem --out cert2.zip --name cert2 --ip 127.0.0.1 --dns localhost --days 9999 --ca-key ca1/ca.key --ca-cert ca1/ca.crt --pass "c2-pass"
unzip cert2.zip

# 6. Convert CAs to PKCS#12

for n in 1 2 3
do
    keytool -importcert -file ca${n}/ca.crt -alias ca -keystore ca${n}/ca.p12 -storetype PKCS12 -storepass p12-pass -v 
    keytool -importcert -file ca${n}/ca.crt -alias ca${n} -keystore ca-all/ca.p12 -storetype PKCS12 -storepass p12-pass -v 
done

# 7. Convert CAs to JKS

for n in 1 2 3
do
    keytool -importcert -file ca${n}/ca.crt -alias ca${n} -keystore ca-all/ca.jks -storetype jks -storepass jks-pass -v 
done

# 8. Convert Certs to PKCS#12

for Cert in cert1 cert2 
do
    openssl pkcs12 -export -out $Cert/$Cert.p12 -inkey $Cert/$Cert.key -in $Cert/$Cert.crt -name $Cert -passout pass:p12-pass 
done

# 9. Import Certs into single PKCS#12 keystore

for Cert in cert1 cert2 
do
    keytool -importkeystore -noprompt \
            -srckeystore $Cert/$Cert.p12 -srcstoretype PKCS12 -srcstorepass p12-pass  \
            -destkeystore cert-all/certs.p12 -deststoretype PKCS12 -deststorepass p12-pass
done

# 10. Import Certs into single JKS keystore with separate key-password

for Cert in cert1 cert2 
do
    keytool -importkeystore -noprompt \
            -srckeystore $Cert/$Cert.p12 -srcstoretype PKCS12 -srcstorepass p12-pass  \
            -destkeystore cert-all/certs.jks -deststoretype jks -deststorepass jks-pass
    keytool -keypasswd -keystore cert-all/certs.jks -alias $Cert -keypass p12-pass -new key-pass -storepass jks-pass
done

