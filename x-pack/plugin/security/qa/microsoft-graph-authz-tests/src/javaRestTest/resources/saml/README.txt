#
# Script / Instructions to (re)generate the certificates + keys in this directory
#
# You can run this with bash, provided "elasticsearch-certutil" is somewhere on your path (or aliased)

#
# Step 1: Create a Signing Key in PEM format
#
elasticsearch-certutil cert --self-signed --pem --out ${PWD}/signing.zip -days 9999 -keysize 2048 -name "signing"
unzip signing.zip
mv signing/signing.* ./
rmdir signing
rm signing.zip

