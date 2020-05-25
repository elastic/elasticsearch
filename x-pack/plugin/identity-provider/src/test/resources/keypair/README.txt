# The RSA keypair certificates were generated with:

elasticsearch-certutil cert --pem --out ${PWD}/keypair-rsa-2048.zip --days 54321 --keysize 2048 --name "CN=test,OU=idp,DC=elasticsearch,DC=org"

elasticsearch-certutil cert --pem --out ${PWD}/keypair-rsa-1024.zip --days 54321 --keysize 1024 --name "CN=test-1024,OU=idp,DC=elasticsearch,DC=org"

