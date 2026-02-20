```yaml {applies_to}
serverless: preview
stack: preview
```

The `URI_PARTS` processing command parses a Uniform Resource Identifier (URI) string and extracts its components into new columns.

::::{note}
This command doesn't support multi-value inputs.
::::


**Syntax**

```esql
URI_PARTS prefix = expression
```

**Parameters**

`prefix`
:   The prefix for the output columns. The extracted components are available as `prefix.component`.

`expression`
:   The string expression containing the URI to parse.

**Description**

The `URI_PARTS` command parses a URI string and extracts its components into new columns.
The new columns are prefixed with the specified `prefix` followed by a dot (`.`).

This command is the query-time equivalent of the [URI parts ingest processor](/reference/enrich-processor/uri-parts-processor.md).

The following columns are created:

`prefix.domain`
:   The host part of the URI.

`prefix.fragment`
:   The fragment part of the URI (the part after `#`).

`prefix.path`
:   The path part of the URI.

`prefix.extension`
:   The file extension extracted from the path.

`prefix.port`
:   The port number as an integer.

`prefix.query`
:   The query string part of the URI (the part after `?`).

`prefix.scheme`
:   The scheme (protocol) of the URI (e.g., `http`, `https`, `ftp`).

`prefix.user_info`
:   The user information part of the URI.

`prefix.username`
:   The username extracted from the user information.

`prefix.password`
:   The password extracted from the user information.

If a component is missing from the URI, the corresponding column contains `null`.
If the expression evaluates to `null`, all output columns are `null`.
If the expression is not a valid URI, a warning is issued and all output columns are `null`.

**Examples**

The following example parses a URI and extracts its parts:

:::{include} ../examples/uri_parts.csv-spec/basic.md
:::

You can use the extracted parts in subsequent commands, for example to filter by domain:

```esql
FROM web_logs
| URI_PARTS p = uri
| WHERE p.domain == "www.example.com"
| STATS COUNT(*) BY p.path
```
