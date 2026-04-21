```yaml {applies_to}
serverless: preview
stack: preview 9.4
```

The `USER_AGENT` processing command parses a user-agent string and extracts its components (name, version, OS, device) into new columns.

::::{note}
This command doesn't support multi-value inputs.
::::


## Syntax

```esql
USER_AGENT prefix = expression [WITH { option = value [, ...] }]
```

## Parameters

`prefix`
:   The prefix for the output columns. The extracted components are available as `prefix.component`.

`expression`
:   The string expression containing the user-agent string to parse.

## WITH options

`regex_file`
:   The name of the parser configuration to use. Default: `_default_`, which uses the built-in regexes from [uap-core](https://github.com/ua-parser/uap-core). To use a custom regex file, place a `.yml` file in the `config/user-agent` directory on each node before starting Elasticsearch. The file must be present at node startup; changes or new files added while the node is running have no effect. Pass the filename (including the `.yml` extension) as the value. Custom regex files are typically variants of the default, either a more recent uap-core release or a customized version.

`extract_device_type`
:   When `true`, extracts device type (e.g., Desktop, Phone, Tablet) on a best-effort basis and includes `prefix.device.type` in the output. Default: `false`.

`properties`
:   List of property groups to include in the output. Each value expands to one or more columns: `name` → `prefix.name`; `version` → `prefix.version`; `os` → `prefix.os.name`, `prefix.os.version`, `prefix.os.full`; `device` → `prefix.device.name` (and `prefix.device.type` when `extract_device_type` is `true`). Default: `["name", "version", "os", "device"]`. You can pass a subset to reduce output columns.

## Using a custom regex file

To use a custom regex file instead of the built-in uap-core patterns:

1. Place a `.yml` file in the `config/user-agent` directory on each node.
2. Create the directory and file before starting Elasticsearch.
3. Pass the filename (including the `.yml` extension) as the `regex_file` option.

Files must be present at node startup. Changes to existing files or new files added while the node is running have no effect until the node is restarted.

::::{note}
Before version 9.4, this directory was named `config/ingest-user-agent`. The old directory name is still supported as a fallback but is deprecated.
::::

Custom regex files are typically variants of the default [uap-core regexes.yaml](https://github.com/ua-parser/uap-core/blob/master/regexes.yaml), either a more recent release or a customized version for specific user-agent patterns. Use a custom file when you need to support newer user-agent formats before they are available in the built-in patterns, or to parse specialized or non-standard user-agent strings.

## Description

The `USER_AGENT` command parses a user-agent string and extracts its parts into new columns.
The new columns are prefixed with the specified `prefix` followed by a dot (`.`).

This command is the query-time equivalent of the [User-Agent ingest processor](/reference/enrich-processor/user-agent-processor.md).

The following columns may be created (depending on `properties` and `extract_device_type`):

`prefix.name`
:   The user-agent name (e.g., Chrome, Firefox).

`prefix.version`
:   The user-agent version.

`prefix.os.name`
:   The operating system name.

`prefix.os.version`
:   The operating system version.

`prefix.os.full`
:   The full operating system string.

`prefix.device.name`
:   The device name.

`prefix.device.type`
:   The device type (e.g., Desktop, Phone). Only present when `extract_device_type` is `true`.

If a component is missing or the input is not a valid user-agent string, the corresponding column contains `null`.
If the expression evaluates to `null` or blank, all output columns are `null`.

## Examples

The following example parses a user-agent string and extracts its parts:

:::{include} ../examples/user_agent.csv-spec/basic.md
:::

To limit output to specific properties or include device type, use the `properties` and `extract_device_type` options:

```esql
ROW ua_str = "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15"
| USER_AGENT ua = ua_str WITH { "properties": ["name", "version", "device"], "extract_device_type": true }
| KEEP ua.*
```

To use a custom regex file (e.g. `my-regexes.yml` in `config/user-agent`), pass the filename including the extension:

```esql
FROM web_logs
| USER_AGENT ua = user_agent WITH { "regex_file": "my-regexes.yml" }
| KEEP ua.name, ua.version
```

You can use the extracted parts in subsequent commands, for example to filter by browser:

```esql
FROM web_logs
| USER_AGENT ua = user_agent
| WHERE ua.name == "Firefox"
| STATS COUNT(*) BY ua.version
```
