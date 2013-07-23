Google Compute Engine Cloud Plugin for Elasticsearch
====================================================

The GCE Cloud plugin allows to use GCE API for the unicast discovery mechanism.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-cloud-gce/1.0.0`.

    -------------------------------------------------------------
    |     GCE Cloud Plugin    | Elasticsearch    | Release date |
    -------------------------------------------------------------
    | 1.1.0-SNAPSHOT (master) | 0.90 -> master   |              |
    -------------------------------------------------------------
    | 1.0.0                   | 0.90 -> master   | 2013-07-23   |
    -------------------------------------------------------------



Google Compute Engine Virtual Machine Discovery
===============================

Google Compute Engine VM discovery allows to use the google APIs to perform automatic discovery (similar to multicast in non hostile
multicast environments). Here is a simple sample configuration:

```yaml
  cloud:
      gce:
          project_id: <your-google-project-id>
          zone: <your-zone>
  discovery:
          type: gce
```

How to start (short story)
--------------------------

* Create Google Compute Engine instance
* Install Elasticsearch
* Install Google Compute Engine Cloud plugin
* Modify `elasticsearch.yml` file
* Start Elasticsearch

How to start (long story)
--------------------------

### Prerequisites

Before starting, you should have:

* Your project ID. Let's say here `es-cloud`. Get it from [Google APIS Console](https://code.google.com/apis/console/).
* [GCUtil](https://developers.google.com/compute/docs/gcutil/#install)


### Creating your first instance


```sh
gcutil --project=es-cloud addinstance myesnode1 \
       --service_account_scope=compute-rw,storage-full \
       --persistent_boot_disk
```

You will be asked to open a link in your browser. Login and allow access to listed services.
You will get back a verification code. Copy and paste it in your terminal.

You should get `Authentication successful.` message.

Then, choose your zone. Let's say here that we choose `europe-west1-a`.

Choose your compute instance size. Let's say `f1-micro`.

Choose your OS. Let's say `projects/debian-cloud/global/images/debian-7-wheezy-v20130617`.

You may be asked to create a ssh key. Follow instructions to create one.

When done, a report like this one should appears:

```sh
Table of resources:

+-----------+--------------+-------+---------+--------------+----------------+----------------+----------------+---------+----------------+
|   name    | machine-type | image | network |  network-ip  |  external-ip   |     disks      |      zone      | status  | status-message |
+-----------+--------------+-------+---------+--------------+----------------+----------------+----------------+---------+----------------+
| myesnode1 | f1-micro     |       | default | 10.240.20.57 | 192.158.29.199 | boot-myesnode1 | europe-west1-a | RUNNING |                |
+-----------+--------------+-------+---------+--------------+----------------+----------------+----------------+---------+----------------+
```

You can now connect to your machine using the external IP address in order to install Elasticsearch:

```
ssh -i ~/.ssh/google_compute_engine 192.158.29.199
```

Once connected, install Elasticsearch:

```sh
sudo apt-get update

# Install curl if needed
sudo apt-get install curl

# Download Elasticsearch
curl https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-0.90.2.deb -o elasticsearch-0.90.2.deb

# Prepare Java installation
sudo apt-get install java7-runtime-headless

# Prepare Elasticsearch installation
sudo dpkg -i elasticsearch-0.90.2.deb

# Check that elasticsearch is running:
curl http://localhost:9200/
```

This command should give you a JSON result:

```javascript
{
  "ok" : true,
  "status" : 200,
  "name" : "Lunatica",
  "version" : {
    "number" : "0.90.2",
    "snapshot_build" : false
  },
  "tagline" : "You Know, for Search"
}
```

### Install elasticsearch cloud gce plugin

Install the plugin:

```sh
# Use Plugin Manager to install it
sudo /usr/share/elasticsearch/bin/plugin
     --install elasticsearch/elasticsearch-cloud-gce/1.0.0

# Configure it:
sudo vi /etc/elasticsearch/elasticsearch.yml
```

And add the following lines:

```yaml
  cloud:
      gce:
          project_id: es-cloud
          zone: europe-west1-a
  discovery:
          type: gce
```


Restart elasticsearch:

```sh
sudo /etc/init.d/elasticsearch restart
```

If anything goes wrong, you should check logs:

```sh
tail -f /var/log/elasticsearch/elasticsearch.log
```

If needed, you can change log level to `TRACE` by modifying `sudo vi /etc/elasticsearch/logging.yml`:

```yaml
  # discovery
  discovery.gce: TRACE
```



### Cloning your existing machine

In order to build a cluster on many nodes, you can clone your configured instance to new nodes.
You won't have to reinstall everything!

First create an image of your running instance and upload it to Google Cloud Storage:

```sh
# Create an image of yur current instance
sudo python /usr/share/imagebundle/image_bundle.py \
  -r / -o /tmp/ --log_file=/tmp/abc.log

# An image has been created in `/tmp` directory:
ls /tmp
e4686d7f5bf904a924ae0cfeb58d0827c6d5b966.image.tar.gz

# Upload your image to Google Cloud Storage:
# Launch this command and follow instructions to give your instance an access to your storage
gsutil config

# Create a bucket to hold your image, let's say `esimage`:
gsutil mb gs://esimage

# Copy your image to this bucket:
gsutil cp /tmp/e4686d7f5bf904a924ae0cfeb58d0827c6d5b966.image.tar.gz gs://esimage

# Then add your image to images collection:
gcutil listkernels --project es-cloud
+----------------------------------------------+--------------------------------------------------+-------------+
|                     name                     |                   description                    | deprecation |
+----------------------------------------------+--------------------------------------------------+-------------+
| projects/google/global/kernels/gce-20120621  | 2.6.39-gcg built 2012-03-29 01:07:00             | DEPRECATED  |
| projects/google/global/kernels/gce-v20130603 | SCSI-enabled 3.3.8-gcg built 2013-05-29 01:04:00 |             |
+----------------------------------------------+--------------------------------------------------+-------------+
# Note the kernel you prefer to use and add your image to your catalog:
gcutil --project=es-cloud addimage elasticsearch-0-90-2 \
       gs://esimage/e4686d7f5bf904a924ae0cfeb58d0827c6d5b966.image.tar.gz \
       --preferred_kernel=projects/google/global/kernels/gce-v20130603

# If the previous command did not work for you, logout from your instance
# and launch the same command from your local machine.
```

### Start new instances

As you have now an image, you can create as many instances as you need:

```sh
# Just change node name (here myesnode2)
gcutil --project=es-cloud addinstance --image=elasticsearch-0-90-2 \
       --kernel=projects/google/global/kernels/gce-v20130603 myesnode2 \
       --zone europe-west1-a --machine_type f1-micro --service_account_scope=compute-rw \
       --persistent_boot_disk
```

### Remove an instance (aka shut it down)

You can use [Google Cloud Console](https://cloud.google.com/console) or CLI to manage your instances:

```sh
# Stopping and removing instances
gcutil --project=es-cloud deleteinstance myesnode1 myesnode2 \
       --zone=europe-west1-a

# Consider removing disk as well if you don't need them anymore
gcutil --project=es-cloud deletedisk boot-myesnode1 boot-myesnode2  \
       --zone=europe-west1-a
```

Filtering by tags
-----------------

The GCE discovery can also filter machines to include in the cluster based on tags using `discovery.gce.tags` settings.
For example, setting `discovery.gce.tags` to `dev` will only filter instances having a tag set to `dev`. Several tags
set will require all of those tags to be set for the instance to be included.

One practical use for tag filtering is when an GCE cluster contains many nodes that are not running
elasticsearch. In this case (particularly with high ping_timeout values) there is a risk that a new node's discovery
phase will end before it has found the cluster (which will result in it declaring itself master of a new cluster
with the same name - highly undesirable). Adding tag on elasticsearch GCE nodes and then filtering by that
tag will resolve this issue.

Add your tag when building the new instance:

```sh
gcutil --project=es-cloud addinstance myesnode1 \
       --service_account_scope=compute-rw \
       --persistent_boot_disk \
       --tags=elasticsearch,dev
```

Then, define it in `elasticsearch.yml`:

```yaml
  cloud:
      gce:
          project_id: es-cloud
          zone: europe-west1-a
  discovery:
          type: gce
          gce:
                tags: elasticsearch, dev
```

Changing default transport port
-------------------------------

By default, elasticsearch GCE plugin assumes that you run elasticsearch on 9300 default port.
But you can specify the port value elasticsearch is meant to use using google compute engine metadata `es_port`:

### When creating instance

Add `--metadata=es_port:9301` option:

```sh
# when creating first instance
gcutil --project=es-cloud addinstance myesnode1 \
       --service_account_scope=compute-rw,storage-full \
       --persistent_boot_disk \
       --metadata=es_port:9301

# when creating an instance from an image
gcutil --project=es-cloud addinstance --image=elasticsearch-0-90-2 \
       --kernel=projects/google/global/kernels/gce-v20130603 myesnode2 \
       --zone europe-west1-a --machine_type f1-micro --service_account_scope=compute-rw \
       --persistent_boot_disk --metadata=es_port:9301
```

### On a running instance

```sh
# Get metadata fingerprint
gcutil --project=es-cloud getinstance myesnode1 \
       --zone=europe-west1-a
+------------------------+---------------------------------------------------------------------------------------------------------+
|        property        |                                                  value                                                  |
+------------------------+---------------------------------------------------------------------------------------------------------+
| metadata               |                                                                                                         |
| fingerprint            | 42WmSpB8rSM=                                                                                            |
+------------------------+---------------------------------------------------------------------------------------------------------+

# Use that fingerprint
gcutil --project=es-cloud setinstancemetadata myesnode1 \
       --zone=europe-west1-a \
       --metadata=es_port:9301 \
       --fingerprint=42WmSpB8rSM=
```


Tips
----

If you don't want to repeat the project id each time, you can save it in `~/.gcutil.flags` file using:

```sh
gcutil getproject --project=es-cloud --cache_flag_values
```

`~/.gcutil.flags` file now contains:

```
--project=es-cloud
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
