This directory contains public and private keys for use with the manual run tasks.

All certs expire September 13, 2042,
have no required password,
require the hostname to be localhost or esX (where X=the name in the certificate) for hostname verification,
were created by the elasticsearch-certutil tool.

## Usage

Use public-ca.pem as the `xpack.security.transport.ssl.certificate_authorities`
Use public-ca.pem for third party tools to trust the certificates in use.
Use private-certX.p12 for node X's `xpack.security.[transport|http].ssl.keystore`

## Certificate Authority (CA):

* private-ca.key : the private key of the signing CA in PEM format. Useful if desired to sign additional certificates.
* public-ca.pem : the public key of the signing CA in PEM format. Useful as the certificate_authorities.

To recreate CA :
```bash
bin/elasticsearch-certutil ca -pem -days 7305
unzip elastic-stack-ca.zip
mv ca/ca.crt public-ca.pem
mv ca/ca.key private-ca.key
````

## Node Certificates signed by the CA

* private-certX.p12 : the public/private key of the certificate signed by the CA. Useful as the keystore.

To create new certificates signed by CA:
```bash
export i=1 # update this
rm -rf certificate-bundle.zip public-cert$i.pem private-cert$i.key private-cert$i.p12 instance
bin/elasticsearch-certutil cert -ca-key private-ca.key -ca-cert public-ca.pem -days 7305 -pem -dns localhost,es$i -ip 127.0.0.1,::1
unzip certificate-bundle.zip
mv instance/instance.crt public-cert$i.pem
mv instance/instance.key private-cert$i.key
openssl pkcs12 -export -out private-cert$i.p12 -inkey private-cert$i.key -in public-cert$i.pem -passout pass: #convert public/private key to p12
```

Other useful commands
```bash
openssl rsa -in private-ca.key -check # check private key
openssl pkcs12 -info -in private-cert$i.p12 -nodes -nocerts # read private keys from p12
openssl pkcs12 -info -in private-cert$i.p12 -nodes -nokeys # read public keys from p12
openssl x509 -in public-cert$i.pem -text # decode PEM formatted public key
openssl s_client -showcerts -connect localhost:9200 </dev/null # show cert from URL
```





