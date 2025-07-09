---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/manage-plugins-using-configuration-file.html
applies_to:
  deployment:
    self: ga
---

# Manage plugins using a configuration file [manage-plugins-using-configuration-file]

::::{admonition} Docker only
:class: important

This feature is only available for [official {{es}} Docker images](https://www.docker.elastic.co/). Other {{es}} distributions will not start with a plugin configuration file.

::::


If you run {{es}} using Docker, you can manage plugins using a declarative configuration file. When {{es}} starts up, it will compare the plugins in the file with those that are currently installed, and add or remove plugins as required. {{es}} will also upgrade official plugins when you upgrade {{es}} itself.

The file is called `elasticsearch-plugins.yml`, and must be placed in the Elasticsearch configuration directory, alongside `elasticsearch.yml`. Here is an example:

```yaml
plugins:
  - id: analysis-icu
  - id: repository-azure
  - id: custom-mapper
    location: https://example.com/archive/custom-mapper-1.0.0.zip
```

This example installs the official `analysis-icu` and `repository-azure` plugins, and one unofficial plugin. Every plugin must provide an `id`. Unofficial plugins must also provide a `location`. This is typically a URL, but Maven coordinates are also supported. The downloaded plugin’s name must match the ID in the configuration file.

While {{es}} will respect the [standard Java proxy system properties](https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.md) when downloading plugins, you can also configure an HTTP proxy to use explicitly in the configuration file. For example:

```yaml
plugins:
  - id: custom-mapper
    location: https://example.com/archive/custom-mapper-1.0.0.zip
proxy: proxy.example.com:8443
```

