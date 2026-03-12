---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage-testing.html
---

# Testing GCE [discovery-gce-usage-testing]

Integrations tests in this plugin require working GCE configuration and therefore disabled by default. To enable tests prepare a config file elasticsearch.yml with the following content:

```yaml
cloud:
  gce:
      project_id: es-cloud
      zone: europe-west1-a
discovery:
      seed_providers: gce
```

Replace `project_id` and `zone` with your settings.

To run test:

```sh
mvn -Dtests.gce=true -Dtests.config=/path/to/config/file/elasticsearch.yml clean test
```

