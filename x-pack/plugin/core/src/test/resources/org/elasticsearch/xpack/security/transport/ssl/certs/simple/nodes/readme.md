# Create the nodes certificates

cd <your_source_root>
export SOURCE_ROOT=$PWD
./gradlew localDistro
cd $SOURCE_ROOT/build/distribution/local/elasticsearch-<version>-SNAPSHOT

### Create instance.yml

```bash
rm instances.yml
echo 'instances:'                                   >> instances.yml
for n in {1..8}
do
for c in {1..8}
do
echo "  - name: \"n$n.c$c\""                        >> instances.yml
echo "    cn:"                                      >> instances.yml
echo "      - \"node$n.cluster$c.elasticsearch\""   >> instances.yml
echo "    dns: "                                    >> instances.yml
echo "      - \"node$n.cluster$c.elasticsearch\""   >> instances.yml
done
done
cat instances.yml
```

### Create the self signed certificates

```bash
rm -rf /tmp/certs; mkdir /tmp/certs; rm -rf local-self
bin/elasticsearch-certutil cert --pem --silent --in instances.yml --out /tmp/certs/self.zip --days 7300 --self-signed
unzip /tmp/certs/self.zip -d ./local-self
cp -r ./local-self/n*/*.crt $SOURCE_ROOT/x-pack/plugin/core/src/test/resources/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/self-signed
cp -r ./local-self/n*/*.key $SOURCE_ROOT/x-pack/plugin/core/src/test/resources/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/self-signed
```

### Create the ca signed certificates

```bash

rm -rf /tmp/certs; mkdir /tmp/certs; rm -rf local-ca
cp $SOURCE_ROOT/x-pack/plugin/core/src/test/resources/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.crt .
cp $SOURCE_ROOT/x-pack/plugin/core/src/test/resources/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca.key .
bin/elasticsearch-certutil cert --pem --silent --in instances.yml --out /tmp/certs/ca.zip --days 7300 --ca-key ca.key --ca-cert ca.crt
unzip /tmp/certs/ca.zip -d ./local-ca
cp -r ./local-ca/n*/*.crt $SOURCE_ROOT/x-pack/plugin/core/src/test/resources/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca-signed
cp -r ./local-ca/n*/*.key $SOURCE_ROOT/x-pack/plugin/core/src/test/resources/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes/ca-signed
```

### Read the certificates

```bash
export CERT_PATH=$SOURCE_ROOT/x-pack/plugin/core/src/test/resources/org/elasticsearch/xpack/security/transport/ssl/certs/simple/nodes
export CERT=ca-signed/n1.c1.crt
openssl x509 -in $CERT_PATH/$CERT -text
openssl asn1parse -in $CERT_PATH/$CERT
openssl asn1parse -in $CERT_PATH/$CERT -strparse 492 # location for SAN OCTET STRING
```
