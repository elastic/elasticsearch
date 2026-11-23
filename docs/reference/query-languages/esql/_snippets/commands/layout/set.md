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

Besides the `SET` directive, a query setting can come from the request body, from a cluster setting, and, for a few settings, from a view definition. The section of each setting in [Allowed settings](#allowed-settings) says which of these it supports.

When the same setting is set in more than one place, the most specific one wins. From highest to lowest precedence:

1. The `SET` directive in the query.
2. {applies_to}`stack: ga 9.5+` The `settings` object in the [request body](/reference/query-languages/esql/esql-rest.md#esql-rest-query-settings).
3. {applies_to}`stack: ga 9.6+` {applies_to}`serverless: unavailable` The cluster setting, which sets a default for every query on the cluster. Set it in `elasticsearch.yml` or with the [cluster update settings API]({{es-apis}}operation/operation-cluster-put-settings).
4. The built-in default of the setting.

A setting with a map value can combine the values from several places instead of replacing them. Refer to the documentation of the setting.

{applies_to}`stack: ga 9.6+` A view definition can use `SET` for some settings. These settings apply only inside that view. For these settings, a value from the calling query or the request body doesn't enter the view, but the cluster setting applies both inside and outside views. For details, refer to [Settings across view boundaries](/reference/query-languages/esql/esql-views.md#settings-across-view-boundaries).

## Allowed settings

:::{include} ../../generated/x-pack-esql/commands/settings/toc.md
:::
