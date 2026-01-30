---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-azure-classic-long.html
---

# Setup process for Azure Discovery [discovery-azure-classic-long]

We will expose here one strategy which is to hide our Elasticsearch cluster from outside.

With this strategy, only VMs behind the same virtual port can talk to each other. That means that with this mode, you can use Elasticsearch unicast discovery to build a cluster, using the Azure API to retrieve information about your nodes.

## Prerequisites [discovery-azure-classic-long-prerequisites]

Before starting, you need to have:

* A [Windows Azure account](https://azure.microsoft.com/en-us/)
* OpenSSL that isn’t from MacPorts, specifically `OpenSSL 1.0.1f 6 Jan 2014` doesn’t seem to create a valid keypair for ssh. FWIW, `OpenSSL 1.0.1c 10 May 2012` on Ubuntu 14.04 LTS is known to work.
* SSH keys and certificate

    You should follow [this guide](http://azure.microsoft.com/en-us/documentation/articles/linux-use-ssh-key/) to learn how to create or use existing SSH keys. If you have already done it, you can skip the following.

    Here is a description on how to generate SSH keys using `openssl`:

    ```sh
    # You may want to use another dir than /tmp
    cd /tmp
    openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout azure-private.key -out azure-certificate.pem
    chmod 600 azure-private.key azure-certificate.pem
    openssl x509 -outform der -in azure-certificate.pem -out azure-certificate.cer
    ```

    Generate a keystore which will be used by the plugin to authenticate with a certificate all Azure API calls.

    ```sh
    # Generate a keystore (azurekeystore.pkcs12)
    # Transform private key to PEM format
    openssl pkcs8 -topk8 -nocrypt -in azure-private.key -inform PEM -out azure-pk.pem -outform PEM
    # Transform certificate to PEM format
    openssl x509 -inform der -in azure-certificate.cer -out azure-cert.pem
    cat azure-cert.pem azure-pk.pem > azure.pem.txt
    # You MUST enter a password!
    openssl pkcs12 -export -in azure.pem.txt -out azurekeystore.pkcs12 -name azure -noiter -nomaciter
    ```

    Upload the `azure-certificate.cer` file both in the Elasticsearch Cloud Service (under `Manage Certificates`), and under `Settings -> Manage Certificates`.

    ::::{important}
    When prompted for a password, you need to enter a non empty one.
    ::::


    See this [guide](http://www.windowsazure.com/en-us/manage/linux/how-to-guides/ssh-into-linux/) for more details about how to create keys for Azure.

    Once done, you need to upload your certificate in Azure:

    * Go to the [management console](https://account.windowsazure.com/).
    * Sign in using your account.
    * Click on `Portal`.
    * Go to Settings (bottom of the left list)
    * On the bottom bar, click on `Upload` and upload your `azure-certificate.cer` file.

    You may want to use [Windows Azure Command-Line Tool](http://www.windowsazure.com/en-us/develop/nodejs/how-to-guides/command-line-tools/):

* Install [NodeJS](https://github.com/joyent/node/wiki/Installing-Node.js-via-package-manager), for example using homebrew on MacOS X:

    ```sh
    brew install node
    ```

* Install Azure tools

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



## Creating your first instance [discovery-azure-classic-long-instance]

You need to have a storage account available. Check [Azure Blob Storage documentation](http://www.windowsazure.com/en-us/develop/net/how-to-guides/blob-storage/#create-account) for more information.

You will need to choose the operating system you want to run on. To get a list of official available images, run:

```sh
azure vm image list
```

Let’s say we are going to deploy an Ubuntu image on an extra small instance in West Europe:

Azure cluster name
:   `azure-elasticsearch-cluster`

Image
:   `b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-13_10-amd64-server-20130808-alpha3-en-us-30GB`

VM Name
:   `myesnode1`

VM Size
:   `extrasmall`

Location
:   `West Europe`

Login
:   `elasticsearch`

Password
:   `password1234!!`

Using command line:

```sh
azure vm create azure-elasticsearch-cluster \
                b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-13_10-amd64-server-20130808-alpha3-en-us-30GB \
                --vm-name myesnode1 \
                --location "West Europe" \
                --vm-size extrasmall \
                --ssh 22 \
                --ssh-cert /tmp/azure-certificate.pem \
                elasticsearch password1234\!\!
```

You should see something like:

```text
info:    Executing command vm create
+ Looking up image
+ Looking up cloud service
+ Creating cloud service
+ Retrieving storage accounts
+ Configuring certificate
+ Creating VM
info:    vm create command OK
```

Now, your first instance is started.

::::{admonition} Working with SSH
:class: tip

You need to give the private key and username each time you log on your instance:

```sh
ssh -i ~/.ssh/azure-private.key elasticsearch@myescluster.cloudapp.net
```

But you can also define it once in `~/.ssh/config` file:

```text
Host *.cloudapp.net
 User elasticsearch
 StrictHostKeyChecking no
 UserKnownHostsFile=/dev/null
 IdentityFile ~/.ssh/azure-private.key
```

::::


Next, you need to install Elasticsearch on your new instance. First, copy your keystore to the instance, then connect to the instance using SSH:

```sh
scp /tmp/azurekeystore.pkcs12 azure-elasticsearch-cluster.cloudapp.net:/home/elasticsearch
ssh azure-elasticsearch-cluster.cloudapp.net
```

Once connected,  [install {{es}}](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md).


## Install Elasticsearch cloud azure plugin [discovery-azure-classic-long-plugin]

```sh
# Install the plugin
sudo /usr/share/elasticsearch/bin/elasticsearch-plugin install discovery-azure-classic

# Configure it
sudo vi /etc/elasticsearch/elasticsearch.yml
```

And add the following lines:

```yaml
# If you don't remember your account id, you may get it with `azure account list`
cloud:
    azure:
        management:
             subscription.id: your_azure_subscription_id
             cloud.service.name: your_azure_cloud_service_name
             keystore:
                   path: /home/elasticsearch/azurekeystore.pkcs12
                   password: your_password_for_keystore

discovery:
    type: azure

# Recommended (warning: non durable disk)
# path.data: /mnt/resource/elasticsearch/data
```

Start Elasticsearch:

```sh
sudo systemctl start elasticsearch
```

If anything goes wrong, check your logs in `/var/log/elasticsearch`.


