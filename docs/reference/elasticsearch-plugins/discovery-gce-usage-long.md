---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage-long.html
---

# Setting up GCE Discovery [discovery-gce-usage-long]

## Prerequisites [discovery-gce-usage-long-prerequisites]

Before starting, you need:

* Your project ID, e.g. `es-cloud`. Get it from [Google API Console](https://code.google.com/apis/console/).
* To install [Google Cloud SDK](https://developers.google.com/cloud/sdk/)

If you did not set it yet, you can define your default project you will work on:

```sh
gcloud config set project es-cloud
```


## Login to Google Cloud [discovery-gce-usage-long-login]

If you haven’t already, login to Google Cloud

```sh
gcloud auth login
```

This will open your browser. You will be asked to sign-in to a Google account and authorize access to the Google Cloud SDK.


## Creating your first instance [discovery-gce-usage-long-first-instance]

```sh
gcloud compute instances create myesnode1 \
       --zone <your-zone> \
       --scopes compute-rw
```

When done, a report like this one should appears:

```text
Created [https://www.googleapis.com/compute/v1/projects/es-cloud-1070/zones/us-central1-f/instances/myesnode1].
NAME      ZONE          MACHINE_TYPE  PREEMPTIBLE INTERNAL_IP   EXTERNAL_IP   STATUS
myesnode1 us-central1-f n1-standard-1             10.240.133.54 104.197.94.25 RUNNING
```

You can now connect to your instance:

```sh
# Connect using google cloud SDK
gcloud compute ssh myesnode1 --zone europe-west1-a

# Or using SSH with external IP address
ssh -i ~/.ssh/google_compute_engine 192.158.29.199
```

::::{admonition} Service Account Permissions
:class: important

It’s important when creating an instance that the correct permissions are set. At a minimum, you must ensure you have:

```text
scopes=compute-rw
```

Failing to set this will result in unauthorized messages when starting Elasticsearch. See [Machine Permissions](/reference/elasticsearch-plugins/discovery-gce-usage-tips.md#discovery-gce-usage-tips-permissions).

::::


Once connected,  [install {{es}}](docs-content://deploy-manage/deploy/self-managed/installing-elasticsearch.md).


## Install Elasticsearch discovery gce plugin [discovery-gce-usage-long-install-plugin]

Install the plugin:

```sh
# Use Plugin Manager to install it
sudo bin/elasticsearch-plugin install discovery-gce
```

Open the `elasticsearch.yml` file:

```sh
sudo vi /etc/elasticsearch/elasticsearch.yml
```

And add the following lines:

```yaml
cloud:
  gce:
      project_id: es-cloud
      zone: europe-west1-a
discovery:
      seed_providers: gce
```

Start Elasticsearch:

```sh
sudo systemctl start elasticsearch
```

If anything goes wrong, you should check logs:

```sh
tail -f /var/log/elasticsearch/elasticsearch.log
```

If needed, you can change log level to `trace` by opening `log4j2.properties`:

```sh
sudo vi /etc/elasticsearch/log4j2.properties
```

and adding the following line:

```yaml
# discovery
logger.discovery_gce.name = discovery.gce
logger.discovery_gce.level = trace
```


