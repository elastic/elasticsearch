# ca.p12

$ES_HOME/bin/elasticsearch-certutil ca --out ca.p12 --pass "ca-password" --days 9999

# ca.pem

openssl pkcs12 -info -in ./ca.p12 -nokeys -out ca.pem -passin "pass:ca-password"

# http.p12

$ES_HOME/bin/elasticsearch-certutil cert --out http.zip --pass "http-password" \
    --days 9999 --pem --name "http" \
    --ca ca.p12 --ca-pass "ca-password" \
    --dns=localhost --dns=localhost.localdomain --dns=localhost4 --dns=localhost4.localdomain4 --dns=localhost6 --dns=localhost6.localdomain6 \
    --ip=127.0.0.1 --ip=0:0:0:0:0:0:0:1
unzip http.zip
rm http.zip

# client.p12

$ES_HOME/bin/elasticsearch-certutil cert --out client.zip --pass "client-password" \
    --name "client" --days 9999  --pem \
    --ca ca.p12 --ca-pass "ca-password"
unzip client.zip
rm client.zip
