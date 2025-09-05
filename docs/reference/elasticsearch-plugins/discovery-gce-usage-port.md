---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage-port.html
---

# Changing default transport port [discovery-gce-usage-port]

By default, Elasticsearch GCE plugin assumes that you run Elasticsearch on 9300 default port. But you can specify the port value Elasticsearch is meant to use using google compute engine metadata `es_port`:

## When creating instance [discovery-gce-usage-port-create]

Add `--metadata es_port=9301` option:

```sh
# when creating first instance
gcloud compute instances create myesnode1 \
       --scopes=compute-rw,storage-full \
       --metadata es_port=9301

# when creating an instance from an image
gcloud compute instances create myesnode2 --image=elasticsearch-1-0-0-RC1 \
       --zone europe-west1-a --machine-type f1-micro --scopes=compute-rw \
       --metadata es_port=9301
```


## On a running instance [discovery-gce-usage-port-run]

```sh
gcloud compute instances add-metadata myesnode1 \
       --zone europe-west1-a \
       --metadata es_port=9301
```


