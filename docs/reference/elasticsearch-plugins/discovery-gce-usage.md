---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-gce-usage.html
---

# GCE Virtual Machine discovery [discovery-gce-usage]

Google Compute Engine VM discovery allows to use the google APIs to perform automatic discovery of seed hosts. Here is a simple sample configuration:

```yaml
cloud:
  gce:
    project_id: <your-google-project-id>
    zone: <your-zone>
discovery:
  seed_providers: gce
```

The following gce settings (prefixed with `cloud.gce`) are supported:

`project_id`
:   Your Google project id. By default the project id will be derived from the instance metadata.

    ```
    Note: Deriving the project id from system properties or environment variables
    (`GOOGLE_CLOUD_PROJECT` or `GCLOUD_PROJECT`) is not supported.
    ```


`zone`
:   helps to retrieve instances running in a given zone. It should be one of the [GCE supported zones](https://developers.google.com/compute/docs/zones#available). By default the zone will be derived from the instance metadata. See also [Using GCE zones](/reference/elasticsearch-plugins/discovery-gce-usage-zones.md).

`retry`
:   If set to `true`, client will use [ExponentialBackOff](https://developers.google.com/api-client-library/java/google-http-java-client/backoff) policy to retry the failed http request. Defaults to `true`.

`max_wait`
:   The maximum elapsed time after the client instantiating retry. If the time elapsed goes past the `max_wait`, client stops to retry. A negative value means that it will wait indefinitely. Defaults to `0s` (retry indefinitely).

`refresh_interval`
:   How long the list of hosts is cached to prevent further requests to the GCE API. `0s` disables caching. A negative value will cause infinite caching. Defaults to `0s`.

::::{admonition} Binding the network host
:class: important

It’s important to define `network.host` as by default it’s bound to `localhost`.

You can use [core network host settings](/reference/elasticsearch/configuration-reference/networking-settings.md) or [gce specific host settings](/reference/elasticsearch-plugins/discovery-gce-network-host.md):

::::


