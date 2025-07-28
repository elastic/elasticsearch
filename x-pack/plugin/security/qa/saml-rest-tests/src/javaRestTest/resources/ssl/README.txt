#
# Script / Instructions to (re)generate the certificates + keys in this directory
#
# You can run this with bash, provided "elasticsearch-certutil" is somewhere on your path (or aliased)

#
# Step 1: Create a CA in PEM format
#
elasticsearch-certutil ca --pem --out ${PWD}/ca.zip -days 9999 -keysize 2048 -ca-dn "CN=Certificate Authority,DC=localhost"
unzip ca.zip
mv ca/ca.* ./
rmdir ca
rm ca.zip

#
# Step 2: Create a certificate for the HTTP server
#
elasticsearch-certutil cert --name "http" --ca-cert ${PWD}/ca.crt --ca-key ${PWD}/ca.key --days 9999 --dns "localhost" --ip "127.0.0.1" --ip "0:0:0:0:0:0:0:1" --keysize 2048 --out ${PWD}/http.zip --pem 
unzip http.zip
mv http/http.* ./
rmdir http
rm http.zip

#
# Step 3: Create a certificate for the Elasticsearch node 
#
elasticsearch-certutil cert --name "node" --ca-cert ${PWD}/ca.crt --ca-key ${PWD}/ca.key --days 9999 --dns "localhost" --ip "127.0.0.1" --ip "0:0:0:0:0:0:0:1" --keysize 2048 --out ${PWD}/node.zip --pem 
unzip node.zip
mv node/node.* ./
rmdir node
rm node.zip
