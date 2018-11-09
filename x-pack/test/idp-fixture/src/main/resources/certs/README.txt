File in this directory are:

idp-ca.crt
idp-ca.key
    Description: A CA for the IdP
    Generated Date: 2018-02-07
    Command: bin/elasticsearch-certutil ca --ca-dn 'CN=idp-fixture,OU=elasticsearch,DC=elastic,DC=co' --days 5000 -keysize 1024 --out idp-ca.zip --pem
    X-Pack Version: 6.2.0

idptrust.jks
    Description: Java Keystore Format of CA cert
    Generated Date: 2018-02-07
    Command: keytool -importcert -file ca.crt -alias idp-fixture-ca  -keystore idptrust.jks -noprompt -storepass changeit
    Java Version: Java(TM) SE Runtime Environment (build 9.0.1+11)
    
