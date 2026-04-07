---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mandatory-plugins.html
applies_to:
  deployment:
    self: ga
---

# Mandatory plugins [mandatory-plugins]

If you rely on some plugins, you can define mandatory plugins by adding `plugin.mandatory` setting to the `config/elasticsearch.yml` file, for example:

```yaml
plugin.mandatory: analysis-icu,lang-js
```

For safety reasons, a node will not start if it is missing a mandatory plugin.

