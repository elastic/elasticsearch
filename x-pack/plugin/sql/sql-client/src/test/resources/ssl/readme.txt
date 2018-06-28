# setup of the SSL files

# generate keys for server and client
$ keytool -v -genkey -keyalg rsa -alias server -keypass password -keystore server.keystore -storepass password -validity 99999 -ext SAN=dns:localhost,ip:127.0.0.1
$ keytool -v -genkey -keyalg rsa -alias client -keypass password -keystore client.keystore -storepass password -validity 99999 -ext SAN=dns:localhost,ip:127.0.0.1

# generate certificates
$ keytool -v -export -alias server -file server.crt -keystore server.keystore -storepass password
$ keytool -v -export -alias client -file client.crt -keystore client.keystore -storepass password

# import the client cert into the server keystore and vice-versa
$ keytool -v -importcert -alias client -file client.crt -keystore server.keystore -storepass password
$ keytool -v -importcert -alias server -file server.crt -keystore client.keystore -storepass password