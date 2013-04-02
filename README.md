Azure Cloud Plugin for ElasticSearch
====================================

The Azure Cloud plugin allows to use Azure API for the unicast discovery mechanism.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-cloud-azure/1.0.0`.

    -----------------------------------------
    | Azure Cloud Plugin | ElasticSearch    |
    -----------------------------------------
    | master             | 0.90 -> master   |
    -----------------------------------------
    | 1.0.0              | 0.20.6           |
    -----------------------------------------


Azure Virtual Machine Discovery
===============================

Azure VM discovery allows to use the azure APIs to perform automatic discovery (similar to multicast in non hostile
multicast environments). Here is a simple sample configuration:

```
    cloud:
        azure:
            private_key: /path/to/private.key
            certificate: /path/to/azure.certficate
            password: your_password_for_pk
            subscription_id: your_azure_subscription_id
    discovery:
            type: azure
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

Here is a description on how to generate this using `openssl`:

```sh
# You may want to use another dir than /tmp
cd /tmp
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout azure-private.key -out azure-certificate.pem
chmod 600 azure-private.key
openssl x509 -outform der -in azure-certificate.pem -out azure-certificate.cer
```

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
# Note, it will create needed files in ~/.azure
azure account import /tmp/azure.publishsettings
```

### Creating your first instance

You need to have a storage account available. Check [Azure Blob Storage documentation](http://www.windowsazure.com/en-us/develop/net/how-to-guides/blob-storage/#create-account)
for more information.

You will need to choose the operating system you want to run on. To get a list of official available images, run:

```sh
azure vm list
```

Let's say we are going to deploy an Ubuntu image on an extra small instance in West Europe:

* Azure cluster name: `azure-elasticsearch-cluster`
* Image: `b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-13_10-amd64-server-20130808-alpha3-en-us-30GB`
* VM Name: `myesnode1`
* VM Size: `extrasmall`
* Location: `West Europe`
* Login: `elasticsearch`

Using command line:

```sh
azure vm create azure-elasticsearch-cluster \
                b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-13_10-amd64-server-20130808-alpha3-en-us-30GB \
                --vm-name myesnode1 \
                --location "West Europe" \
                --vm-size extrasmall \
                --ssh 22 \
                --ssh-cert /tmp/azure-certificate.pem \
                elasticsearch password
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
# First, copy your azure certificate on this machine
scp /tmp/azure.publishsettings azure-elasticsearch-cluster.cloudapp.net:/tmp

# Then, connect to your instance using SSH
ssh azure-elasticsearch-cluster.cloudapp.net
```

Once connected, install Elasticsearch:

```sh
# Install Latest JDK
sudo apt-get update
sudo apt-get install openjdk-7-jre-headless

# Download Elasticsearch
curl -s https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.3.deb -o elasticsearch-0.90.3.deb

# Prepare Elasticsearch installation
sudo dpkg -i elasticsearch-0.90.3.deb
```

Check that elasticsearch is running:

```sh
curl http://localhost:9200/
```

This command should give you a JSON result:

```javascript
{
  "ok" : true,
  "status" : 200,
  "name" : "Mandarin",
  "version" : {
    "number" : "0.90.3",
    "build_hash" : "5c38d6076448b899d758f29443329571e2522410",
    "build_timestamp" : "2013-08-06T13:18:31Z",
    "build_snapshot" : false,
    "lucene_version" : "4.4"
  },
  "tagline" : "You Know, for Search"
}
```

### Install nodejs and Azure tools

*TODO: check if there is a downloadable version of NodeJS*

```sh
# Install node (aka download and compile source)
sudo apt-get update
sudo apt-get install python-software-properties python g++ make
sudo add-apt-repository ppa:chris-lea/node.js
sudo apt-get update
sudo apt-get install nodejs

# Install Azure tools
sudo npm install azure-cli -g

# Install your azure certficate
azure account import /tmp/azure.publishsettings

# Remove tmp file
rm /tmp/azure.publishsettings
```

### Generate private keys for this instance

```sh
openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout azure-private.key -out azure-certificate.pem
chmod 600 azure-private.key
openssl x509 -outform der -in azure-certificate.pem -out azure-certificate.cer
# Transform private key to PEM format
openssl pkcs8 -topk8 -nocrypt -in azure-private.key -inform PEM -out azure-pk.pem -outform PEM
# Transform certificate to PEM format
openssl x509 -inform der -in azure-certificate.cer -out azure-cert.pem
```

Upload the generated key to Azure platform

```sh
azure service cert create azure-elasticsearch-cluster azure-certificate.cer
```

### Install elasticsearch cloud azure plugin

```sh
# Stop elasticsearch
sudo service elasticsearch stop

# Install the plugin (TODO : USE THE RIGHT VERSION NUMBER)
sudo /usr/share/elasticsearch/bin/plugin -install elasticsearch/elasticsearch-cloud-azure/0.1.0-SNAPSHOT

# Configure it
sudo vi /etc/elasticsearch/elasticsearch.yml
```

And add the following lines:

```yaml
# If you don't remember your account id, you may get it with `azure account list`
    cloud:
        azure:
            private_key: /home/elasticsearch/azure-pk.pem
            certificate: /home/elasticsearch/azure-cert.pem
            subscription_id: your_azure_subscription_id
    discovery:
            type: azure
```

Restart elasticsearch:

```sh
sudo service elasticsearch start
```

If anything goes wrong, check your logs in `/var/log/elasticsearch`.


TODO: Ask pierre for Azure commands

Cloning your existing machine:

```sh
azure ....
```


Add a new machine:

```sh
azure vm create -c myescluster --vm-name myesnode2 b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-13_04-amd64-server-20130501-en-us-30GB -l "West Europe" --vm-size extrasmall --ssh 22 elasticsearch fantastic0!
```

Add you certificate for this new instance.

```sh
# Add certificate for this instance
azure service cert create myescluster1 azure-certificate.cer
```


License
-------

This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2013 ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
