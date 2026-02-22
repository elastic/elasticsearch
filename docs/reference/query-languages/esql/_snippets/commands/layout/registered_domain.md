```yaml {applies_to}
serverless: preview
stack: preview 9.4
```

The `REGISTERED_DOMAIN` processing command parses a fully qualified domain name (FQDN) string and extracts its parts (domain, registered domain, top-level domain, subdomain) into new columns using the public suffix list.

::::{note}
This command doesn't support multi-value inputs.
::::


**Syntax**

```esql
REGISTERED_DOMAIN prefix = expression
```

**Parameters**

`prefix`
:   The prefix for the output columns. The extracted parts are available as `prefix.part_name`.

`expression`
:   The string expression containing the FQDN to parse.

**Description**

The `REGISTERED_DOMAIN` command parses an FQDN string and extracts its parts into new columns.
The new columns are prefixed with the specified `prefix` followed by a dot (`.`).

The following columns are created:

`prefix.domain`
:   The full domain name (the input FQDN).

`prefix.registered_domain`
:   The registered domain (e.g. `example.co.uk` for `www.example.co.uk`).

`prefix.top_level_domain`
:   The effective top-level domain ([eTLD](https://developer.mozilla.org/en-US/docs/Glossary/eTLD)), e.g. `co.uk`, `com`.

`prefix.subdomain`
:   The subdomain part, if any (e.g. `www` for `www.example.co.uk`).

If a part is missing or the input is not a valid FQDN, the corresponding column contains `null`.
If the expression evaluates to `null` or blank, all output columns are `null`.

**Examples**

The following example parses an FQDN and extracts its parts:

:::{include} ../examples/registered_domain.csv-spec/basic.md
:::

You can use the extracted parts in subsequent commands, for example to filter by registered domain:

```esql
FROM web_logs
| REGISTERED_DOMAIN rd = domain
| WHERE rd.registered_domain == "elastic.co"
| STATS COUNT(*) BY rd.subdomain
```
