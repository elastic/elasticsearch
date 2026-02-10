```yaml {applies_to}
serverless: preview 9.3.0
```

The `SET` directive can be used to specify query settings that modify the behavior of an {{esql}} query.

**Syntax**

```esql
SET setting_name = setting_value[, ..., settingN = valueN]; <query>
```

Multiple SET directives can be included in a single query, separated by semicolons.
If the same setting is defined multiple times, the last definition takes precedence.

**Allowed settings**

:::{include} ../settings/toc.md
:::
