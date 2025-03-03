---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-azure-classic-scale.html
---

# Scaling out! [discovery-azure-classic-scale]

You need first to create an image of your previous machine. Disconnect from your machine and run locally the following commands:

```sh
# Shutdown the instance
azure vm shutdown myesnode1

# Create an image from this instance (it could take some minutes)
azure vm capture myesnode1 esnode-image --delete

# Note that the previous instance has been deleted (mandatory)
# So you need to create it again and BTW create other instances.

azure vm create azure-elasticsearch-cluster \
                esnode-image \
                --vm-name myesnode1 \
                --location "West Europe" \
                --vm-size extrasmall \
                --ssh 22 \
                --ssh-cert /tmp/azure-certificate.pem \
                elasticsearch password1234\!\!
```

::::{tip}
It could happen that azure changes the endpoint public IP address. DNS propagation could take some minutes before you can connect again using name. You can get from azure the IP address if needed, using:

```sh
# Look at Network `Endpoints 0 Vip`
azure vm show myesnode1
```

::::


Let’s start more instances!

```sh
for x in $(seq  2 10)
	do
		echo "Launching azure instance #$x..."
		azure vm create azure-elasticsearch-cluster \
		                esnode-image \
		                --vm-name myesnode$x \
		                --vm-size extrasmall \
		                --ssh $((21 + $x)) \
		                --ssh-cert /tmp/azure-certificate.pem \
		                --connect \
		                elasticsearch password1234\!\!
	done
```

If you want to remove your running instances:

```sh
azure vm delete myesnode1
```

