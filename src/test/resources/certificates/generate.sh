#!/bin/bash

# Number of nodes key pair to create
nb=3

prefix="esnode"

die () {
    echo >&2 "$@"
    exit 1
}

[ "$#" -eq 1 ] || die "1 argument required, $# provided"
echo $1 | grep -E -q '^[0-9]+$' || die "Numeric argument required, $1 provided"

nb=$1

rm ./$prefix*.jks
rm ./$prefix*.cert

# Create key pair and certificate for every node
nb_nodes=1
while [ $nb_nodes -le $nb ]
do
   	node_name=$prefix$nb_nodes

	echo "Create" $node_name  "key pair:"
	keytool -genkeypair -alias $node_name -keystore $node_name.jks -keyalg RSA -storepass $node_name -keypass $node_name -dname "cn=Elasticsearch Node, ou=elasticsearch, o=org"

	echo "Generate" $node_name "certificate:"
	keytool -export -alias $node_name -keystore $node_name.jks -rfc -file $node_name.cert -storepass $node_name
	
	nb_nodes=$(( $nb_nodes + 1 ))
done

# Import certificates in nodes
current_node=1
while [ $current_node -le $nb ]
do
        node_name=$prefix$current_node
	import_node=1

	while [ $import_node -le $nb ]
	do
		import_node_name=$prefix$import_node
		if [ "$import_node" -ne "$current_node" ]
		then
			echo "Importing" $node_name "certificate into" $import_node_name "keystore"
			keytool -import -trustcacerts -alias $node_name -file $node_name.cert -keystore $import_node_name.jks -storepass $import_node_name -noprompt
		fi
		import_node=$(( $import_node + 1 ))
	done

        current_node=$(( $current_node + 1 ))
done

exit;

