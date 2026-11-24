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

## Where to set a query setting [esql-set-sources]

Besides the `SET` directive, a query setting can come from the request body, from a cluster setting, and, for a few settings, from a view definition. The section of each setting in [Allowed settings](#allowed-settings) says which of these it supports, and since which version.

When the same setting is set in more than one place, the most specific one wins. From highest to lowest precedence:

1. The `SET` directive in the query.
2. The `settings` object in the [request body](/reference/query-languages/esql/esql-rest.md#esql-rest-query-settings).
3. {applies_to}`serverless: unavailable` The cluster setting, which sets a default for every query on the cluster. Set it in `elasticsearch.yml` or with the [cluster update settings API]({{es-apis}}operation/operation-cluster-put-settings).
4. The built-in default of the setting.

A setting with a map value can combine the values from several places instead of replacing them. Refer to the documentation of the setting.

Some settings can also be set in a view definition. How the settings of a view, of the calling query, and of the request body apply inside the view depends on the version. For details, refer to [Settings across view boundaries](/reference/query-languages/esql/esql-views.md#settings-across-view-boundaries).

## Allowed settings

:::{include} ../../generated/x-pack-esql/commands/settings/toc.md
:::
