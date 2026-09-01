```yaml {applies_to}
serverless: ga
stack: preview =9.3, ga 9.4+
```

The `SET` directive can be used to specify query settings that modify the behavior of an {{esql}} query.

## Syntax

```esql
SET setting_name = setting_value[, ..., settingN = valueN]; <query>
```

Multiple SET directives can be included in a single query, separated by semicolons.
If the same setting is defined multiple times, the last definition takes precedence.

## Cluster-wide defaults
```yaml {applies_to}
stack: ga 9.6+
```

Some settings can also be given a cluster-wide default, so that every query on the cluster uses it without
having to specify it. Set `esql.query.settings.<setting_name>` in `elasticsearch.yml` or through the
[cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings).
The key is dynamic and takes effect on the next query without a restart.

| Setting | Cluster setting |
|---|---|
| `time_zone` | `esql.query.settings.time_zone` |
| `unmapped_fields` | `esql.query.settings.unmapped_fields` |

A cluster default replaces the built-in default, and any per-query value still overrides it. The full order of
precedence, from weakest to strongest, is:

1. the setting's built-in default
2. the cluster-wide default, if one is configured
3. the value supplied in the request body
4. the value supplied with `SET`

Settings not listed above have no cluster setting: `esql.query.settings.<name>` is rejected as unknown for them,
so a typo fails instead of being silently ignored.

## Allowed settings

:::{include} ../../generated/x-pack-esql/commands/settings/toc.md
:::
