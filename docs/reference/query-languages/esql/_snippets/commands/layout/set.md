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

Settings can also be supplied in the request body, and some can be given a cluster-wide default. A value set with
`SET` overrides both. Refer to [{{esql}} query settings](/reference/query-languages/esql/esql-query-settings.md) for
the places a setting can come from and the full order of precedence.

## Allowed settings

:::{include} ../../generated/x-pack-esql/commands/settings/toc.md
:::
