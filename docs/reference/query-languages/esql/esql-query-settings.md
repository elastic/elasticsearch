---
navigation_title: "Query settings"
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# {{esql}} query settings [esql-query-settings]

Query settings are typed knobs that change how an {{esql}} query behaves — the timezone it evaluates dates in, how it treats unmapped fields, and so on. The same setting can be supplied from more than one place, and this page explains where each one can be set and which value wins.

For the list of available settings and what each does, refer to the [`SET` directive](/reference/query-languages/esql/directives/set.md).

## Where a setting can be supplied

A setting can be supplied from up to three places. Not every setting accepts all three; the per-setting reference says which apply.

`SET` directive
:   In the query itself, before the query body. Available for every setting. Refer to [`SET`](/reference/query-languages/esql/directives/set.md).

    ```esql
    SET time_zone = "Europe/Paris"; FROM employees | LIMIT 10
    ```

Request body
:   Under `settings` in the `_query` request body, for tooling that builds requests programmatically rather than splicing strings into the query. Refer to the [REST API](/reference/query-languages/esql/esql-rest.md).

    ```console
    POST /_query
    {
      "query": "FROM employees | LIMIT 10",
      "settings": { "time_zone": "Europe/Paris" }
    }
    ```

Cluster setting
:   ```yaml {applies_to}
    stack: ga 9.6+
    serverless: unavailable
    ```

    A cluster-wide default, so every query on the cluster uses it without having to ask for it. Set `esql.query.settings.<setting_name>` in `elasticsearch.yml` or through the [cluster update settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings). The key is dynamic and takes effect on the next query without a restart.

    ```console
    PUT /_cluster/settings
    {
      "persistent": { "esql.query.settings.time_zone": "Europe/Paris" }
    }
    ```

    These settings currently support a cluster-wide default:

    | Setting | Cluster setting |
    |---|---|
    | `time_zone` | `esql.query.settings.time_zone` |
    | `unmapped_fields` | `esql.query.settings.unmapped_fields` |

    For any other setting, `esql.query.settings.<name>` is rejected as an unknown key, so a typo fails instead of being silently ignored.

## Precedence

When a setting arrives from more than one place, the more specific source wins. From weakest to strongest:

1. the setting's built-in default
2. the cluster-wide default, if one is configured
3. the value supplied in the request body
4. the value supplied with `SET`

So a cluster-wide default replaces the built-in default for every query, and any individual query can still override it. If the same setting is given twice with `SET`, the last one wins.

An invalid cluster-wide default is rejected when it is set — the cluster update settings request fails, and a node carrying a bad value in `elasticsearch.yml` does not start — rather than failing queries later.
