#!/bin/bash

#
# The certificates in this directory were created with:

elasticsearch-certutil cert --pem --out ${PWD}/http.zip --dns localhost --ip 127.0.0.1 --days 5000 --name http

unzip http.zip
mv ca/ca.crt http/http.* ./
rmdir ca http
rm http.zip

