```yaml {applies_to}
serverless: ga
stack: ga
```

The `SET` directive can be used to specify query settings that modify the behavior of an {{esql}} query.

**Syntax**

```esql
SET setting_name = setting_value[, ..., settingN = valueN]; <query>
```

Multiple SET directives can be included in a single query, separated by semicolons.
If the same setting is defined multiple times, the last definition takes precedence.

**Examples**

Use SET to define the query time zone:

:::{include} ../examples/tbucket.csv-spec/set-timezone-example.md
:::

**Allowed settings**

:::{include} ../settings/toc.md
:::
