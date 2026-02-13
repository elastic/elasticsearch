---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage-zones.html
---

# Using GCE zones [discovery-gce-usage-zones]

`cloud.gce.zone` helps to retrieve instances running in a given zone. It should be one of the [GCE supported zones](https://developers.google.com/compute/docs/zones#available).

The GCE discovery can support multi zones although you need to be aware of network latency between zones. To enable discovery across more than one zone, just enter add your zone list to `cloud.gce.zone` setting:

```yaml
cloud:
  gce:
    project_id: <your-google-project-id>
    zone: ["<your-zone1>", "<your-zone2>"]
discovery:
  seed_providers: gce
```

