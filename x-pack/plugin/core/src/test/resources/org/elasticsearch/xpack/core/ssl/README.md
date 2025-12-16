The certificates in these subdirectories were created using Elasticsearch's "
certutil" tool
and can safely be recreated as needed if they expire or need to be improved in
some way

The commands to be used are

### A single CA

```
elasticsearch-certutil ca \
  --pem --out ${PWD}/ca.zip \
  --ca-dn "CN=Test CA,DC=elasticsearch,DC=org" \
  --days 3000 --keysize 4096

 unzip ca.zip;
 rm ca.zip
```

### A server certificate with DNS + IP Subject-Alternative Names

```
elasticsearch-certutil cert \
  --pem --out ${PWD}/server.zip \
  --ca-cert ${PWD}/ca/ca.crt --ca-key ${PWD}/ca/ca.key \
  --days 3000 --keysize 4096 \
  --dns "localhost" --ip "127.0.0.1" \
  --name "server";

unzip server.zip;
rm server.zip
```

### A server certificate without DNS + IP Subject-Alternative Names

```
elasticsearch-certutil cert \
  --pem --out ${PWD}/server-no-san.zip \
  --ca-cert ${PWD}/ca/ca.crt --ca-key ${PWD}/ca/ca.key \
  --days 3000 --keysize 4096 \
  --name "server-no-san";

unzip server-no-san.zip;
rm server-no-san.zip
```

### A client certificate

```
elasticsearch-certutil cert \
  --pem --out ${PWD}/client.zip \
  --ca-cert ${PWD}/ca/ca.crt --ca-key ${PWD}/ca/ca.key \
  --days 3000 --keysize 4096 \
  --name "client";

unzip client.zip;
rm client.zip
```
