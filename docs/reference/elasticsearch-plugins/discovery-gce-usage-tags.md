---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage-tags.html
---

# Filtering by tags [discovery-gce-usage-tags]

The GCE discovery can also filter machines to include in the cluster based on tags using `discovery.gce.tags` settings. For example, setting `discovery.gce.tags` to `dev` will only filter instances having a tag set to `dev`. Several tags set will require all of those tags to be set for the instance to be included.

One practical use for tag filtering is when a GCE cluster contains many nodes that are not master-eligible {{es}} nodes. In this case, tagging the GCE instances that *are* running the master-eligible {{es}} nodes, and then filtering by that tag, will help discovery to run more efficiently.

Add your tag when building the new instance:

```sh
gcloud compute instances create myesnode1 --project=es-cloud \
       --scopes=compute-rw \
       --tags=elasticsearch,dev
```

Then, define it in `elasticsearch.yml`:

```yaml
cloud:
  gce:
    project_id: es-cloud
    zone: europe-west1-a
discovery:
  seed_providers: gce
    gce:
      tags: elasticsearch, dev
```

