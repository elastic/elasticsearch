Azure Cloud Plugin for Elasticsearch
====================================

The Azure Cloud plugin allows to use Azure API for the unicast discovery mechanism.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-cloud-azure/2.3.0`.

* For master elasticsearch versions, look at [master branch](https://github.com/elasticsearch/elasticsearch-cloud-azure/tree/master).
* For 1.3.x elasticsearch versions, look at [es-1.3 branch](https://github.com/elasticsearch/elasticsearch-cloud-azure/tree/es-1.3).
* For 1.2.x elasticsearch versions, look at [es-1.2 branch](https://github.com/elasticsearch/elasticsearch-cloud-azure/tree/es-1.2).
* For 1.1.x elasticsearch versions, look at [es-1.1 branch](https://github.com/elasticsearch/elasticsearch-cloud-azure/tree/es-1.1).
* For 1.0.x elasticsearch versions, look at [es-1.0 branch](https://github.com/elasticsearch/elasticsearch-cloud-azure/tree/es-1.0).
* For 0.90.x elasticsearch versions, look at [es-0.90 branch](https://github.com/elasticsearch/elasticsearch-cloud-azure/tree/es-0.90).

|     Azure Cloud Plugin      |    elasticsearch    | Release date |
|-----------------------------|---------------------|:------------:|
| 3.0.0-SNAPSHOT              | master              |  XXXX-XX-XX  |

Please read documentation relative to the version you are using:

* [3.0.0-SNAPSHOT](https://github.com/elasticsearch/elasticsearch-cloud-azure/blob/master/README.md)


Azure Virtual Machine Discovery
===============================

Azure VM discovery allows to use the azure APIs to perform automatic discovery (similar to multicast in non hostile
multicast environments). Here is a simple sample configuration:

```
cloud:
    azure:
        keystore: /path/to/keystore
        password: your_password_for_keystore
        subscription_id: your_azure_subscription_id
        service_name: your_azure_cloud_service_name
discovery:
        type: azure

# recommended
# path.data: /mnt/resource/elasticsearch/data
```

How to start (short story)
--------------------------

* Create Azure instances
* Install Elasticsearch
* Install Azure plugin
* Modify `elasticsearch.yml` file
* Start Elasticsearch

How to start (long story)
--------------------------

We will expose here one strategy which is to hide our Elasticsearch cluster from outside.

With this strategy, only VM behind this same virtual port can talk to each other.
That means that with this mode, you can use elasticsearch unicast discovery to build a cluster.

Best, you can use the `elasticsearch-cloud-azure` plugin to let it fetch information about your nodes using
azure API.

### Prerequisites

Before starting, you need to have:

* A [Windows Azure account](http://www.windowsazure.com/)
* SSH keys and certificate
* OpenSSL that isn't from MacPorts, specifically `OpenSSL 1.0.1f 6 Jan
  2014` doesn't seem to create a valid keypair for ssh.  FWIW,
  `OpenSSL 1.0.1c 10 May 2012` on Ubuntu 12.04 LTS is known to work.

Here is a description on how to generate this using `openssl`:

```sh
# You may want to use another dir than /tmp
cd /tmp
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout azure-private.key -out azure-certificate.pem
chmod 600 azure-private.key azure-certificate.pem
openssl x509 -outform der -in azure-certificate.pem -out azure-certificate.cer

# Generate a keystore (azurekeystore.pkcs12)
# Transform private key to PEM format
openssl pkcs8 -topk8 -nocrypt -in azure-private.key -inform PEM -out azure-pk.pem -outform PEM
# Transform certificate to PEM format
openssl x509 -inform der -in azure-certificate.cer -out azure-cert.pem
cat azure-cert.pem azure-pk.pem > azure.pem.txt
# You MUST enter a password!
openssl pkcs12 -export -in azure.pem.txt -out azurekeystore.pkcs12 -name azure -noiter -nomaciter
```

Upload the generated key to Azure platform. **Important**: when prompted for a password,
you need to enter a non empty one.

See this [guide](http://www.windowsazure.com/en-us/manage/linux/how-to-guides/ssh-into-linux/) to have
more details on how to create keys for Azure.

Once done, you need to upload your certificate in Azure:

* Go to the [management console](https://account.windowsazure.com/).
* Sign in using your account.
* Click on `Portal`.
* Go to Settings (bottom of the left list)
* On the bottom bar, click on `Upload` and upload your `azure-certificate.cer` file.

You may want to use [Windows Azure Command-Line Tool](http://www.windowsazure.com/en-us/develop/nodejs/how-to-guides/command-line-tools/):

* Install [NodeJS](https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager), for example using
homebrew on MacOS X:

```sh
brew install node
```

* Install Azure tools:

```sh
sudo npm install azure-cli -g
```

* Download and import your azure settings:

```sh
# This will open a browser and will download a .publishsettings file
azure account download

# Import this file (we have downloaded it to /tmp)
# Note, it will create needed files in ~/.azure. You can remove azure.publishsettings when done.
azure account import /tmp/azure.publishsettings
```

### Creating your first instance

You need to have a storage account available. Check [Azure Blob Storage documentation](http://www.windowsazure.com/en-us/develop/net/how-to-guides/blob-storage/#create-account)
for more information.

You will need to choose the operating system you want to run on. To get a list of official available images, run:

```sh
azure vm image list
```

Let's say we are going to deploy an Ubuntu image on an extra small instance in West Europe:

* Azure cluster name: `azure-elasticsearch-cluster`
* Image: `b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-13_10-amd64-server-20130808-alpha3-en-us-30GB`
* VM Name: `myesnode1`
* VM Size: `extrasmall`
* Location: `West Europe`
* Login: `elasticsearch`
* Password: `password1234!!`

Using command line:

```sh
azure vm create azure-elasticsearch-cluster \
                b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-13_10-amd64-server-20130808-alpha3-en-us-30GB \
                --vm-name myesnode1 \
                --location "West Europe" \
                --vm-size extrasmall \
                --ssh 22 \
                --ssh-cert /tmp/azure-certificate.pem \
                elasticsearch password1234!!
```

You should see something like:

```
info:    Executing command vm create
+ Looking up image
+ Looking up cloud service
+ Creating cloud service
+ Retrieving storage accounts
+ Configuring certificate
+ Creating VM
info:    vm create command OK
```

Now, your first instance is started. You need to install Elasticsearch on it.

> **Note on SSH**
>
> You need to give the private key and username each time you log on your instance:
>
>```sh
>ssh -i ~/.ssh/azure-private.key elasticsearch@myescluster.cloudapp.net
>```
>
> But you can also define it once in `~/.ssh/config` file:
> 
>```
>Host *.cloudapp.net
>  User elasticsearch
>  StrictHostKeyChecking no
>  UserKnownHostsFile=/dev/null
>  IdentityFile ~/.ssh/azure-private.key
>```


```sh
# First, copy your keystore on this machine
scp /tmp/azurekeystore.pkcs12 azure-elasticsearch-cluster.cloudapp.net:/home/elasticsearch

# Then, connect to your instance using SSH
ssh azure-elasticsearch-cluster.cloudapp.net
```

Once connected, install Elasticsearch:

```sh
# Install Latest OpenJDK
# If you would like to use Oracle JDK instead, read the following:
# http://www.webupd8.org/2012/01/install-oracle-java-jdk-7-in-ubuntu-via.html
sudo apt-get update
sudo apt-get install openjdk-7-jre-headless

# Download Elasticsearch
curl -s https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.0.0.deb -o elasticsearch-1.0.0.deb

# Prepare Elasticsearch installation
sudo dpkg -i elasticsearch-1.0.0.deb
```

Check that elasticsearch is running:

```sh
curl http://localhost:9200/
```

This command should give you a JSON result:

```javascript
{
  "status" : 200,
  "name" : "Living Colossus",
  "version" : {
    "number" : "1.0.0",
    "build_hash" : "a46900e9c72c0a623d71b54016357d5f94c8ea32",
    "build_timestamp" : "2014-02-12T16:18:34Z",
    "build_snapshot" : false,
    "lucene_version" : "4.6"
  },
  "tagline" : "You Know, for Search"
}
```

### Install elasticsearch cloud azure plugin

```sh
# Stop elasticsearch
sudo service elasticsearch stop

# Install the plugin
sudo /usr/share/elasticsearch/bin/plugin -install elasticsearch/elasticsearch-cloud-azure/2.0.0

# Configure it
sudo vi /etc/elasticsearch/elasticsearch.yml
```

And add the following lines:

```yaml
# If you don't remember your account id, you may get it with `azure account list`
cloud:
    azure:
        keystore: /home/elasticsearch/azurekeystore.pkcs12
        password: your_password_for_keystore
        subscription_id: your_azure_subscription_id
        service_name: your_azure_cloud_service_name
discovery:
        type: azure

# Recommended
path.data: /mnt/resource/elasticsearch/data
```

Restart elasticsearch:

```sh
sudo service elasticsearch start
```

If anything goes wrong, check your logs in `/var/log/elasticsearch`.


Scaling Out!
------------

You need first to create an image of your previous machine.
Disconnect from your machine and run locally the following commands:

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
                elasticsearch password1234!!
```

> **Note:** It could happen that azure changes the endpoint public IP address.
> DNS propagation could take some minutes before you can connect again using
> name. You can get from azure the IP address if needed, using:
>
> ```sh
> # Look at Network `Endpoints 0 Vip`
> azure vm show myesnode1
> ```

Let's start more instances!

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
		                elasticsearch password1234!!
	done
```

If you want to remove your running instances:

```
azure vm delete myesnode1
```

Azure Repository
================

To enable Azure repositories, you have first to set your azure storage settings:

```
cloud:
    azure:
        storage_account: your_azure_storage_account
        storage_key: your_azure_storage_key
```

The Azure repository supports following settings:

* `container`: Container name. Defaults to `elasticsearch-snapshots`
* `base_path`: Specifies the path within container to repository data. Defaults to empty (root directory).
* `concurrent_streams`: Throttles the number of streams (per node) preforming snapshot operation. Defaults to `5`.
* `chunk_size`: Big files can be broken down into chunks during snapshotting if needed. The chunk size can be specified
in bytes or by using size value notation, i.e. `1g`, `10m`, `5k`. Defaults to `64m` (64m max)
* `compress`: When set to `true` metadata files are stored in compressed format. This setting doesn't affect index
files that are already compressed by default. Defaults to `false`.

Some examples, using scripts:

```sh
# The simpliest one
$ curl -XPUT 'http://localhost:9200/_snapshot/my_backup1' -d '{
    "type": "azure"
}'

# With some settings
$ curl -XPUT 'http://localhost:9200/_snapshot/my_backup2' -d '{
    "type": "azure",
    "settings": {
        "container": "backup_container",
        "base_path": "backups",
        "concurrent_streams": 2,
        "chunk_size": "32m",
        "compress": true
    }
}'
```

Example using Java:

```java
client.admin().cluster().preparePutRepository("my_backup3")
        .setType("azure").setSettings(ImmutableSettings.settingsBuilder()
                .put(AzureStorageService.Fields.CONTAINER, "backup_container")
                .put(AzureStorageService.Fields.CHUNK_SIZE, new ByteSizeValue(32, ByteSizeUnit.MB))
        ).get();
```


Testing
-------

Integrations tests in this plugin require working Azure configuration and therefore disabled by default.
To enable tests prepare a config file elasticsearch.yml with the following content:

```
cloud:
  azure:
      account: "YOUR-AZURE-STORAGE-NAME"
      key: "YOUR-AZURE-STORAGE-KEY"
```

Replaces `account`, `key` with your settings. Please, note that the test will delete all snapshot/restore related files in the specified bucket.

To run test:

```sh
mvn -Dtests.azure=true -Des.config=/path/to/config/file/elasticsearch.yml clean test
```


License
-------

This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
