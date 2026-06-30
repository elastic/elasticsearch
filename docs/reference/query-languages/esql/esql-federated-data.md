---
navigation_title: "Federated data"
description: "Overview of querying data stored outside Elasticsearch using {{esql}}, including key concepts, supported data sources, and file formats."
applies_to:
  stack: preview =9.5
  serverless: preview
products:
  - id: elasticsearch
---

# {{esql}} federated data

You can query data stored in compatible external data sources, using the same syntax you use for native indices and other index abstractions, without any ingestion into {{es}}.

## Why use federated data

Many organizations store large volumes of data in cloud object storage for cost and compliance reasons. Querying that data typically requires a separate tool like Apache Spark, Amazon Athena, or Trino, which means managing extra infrastructure and switching between query languages.

{{esql}} federated data enables you to query this data directly from {{es}}, with several advantages:

- **One language for all your data.** Use the same {{esql}} syntax for both indexed data and external data. No context-switching, no second query engine.
- **No extra infrastructure.** Query external data natively in {{es}} without deploying or managing additional compute services, catalogs, or connectors.
- **Progressive acceleration.** Start by querying raw data directly in object storage. When specific datasets need faster performance, promote them into {{es}} for indexed search. Both tiers stay queryable with the same `FROM` syntax.

## How it works

Federated data separates connection details from reading and parsing details:

- A **[data source](esql-federated-data-reference.md#data-source-api)** defines the connection to an external storage system. It stores the connection type, region, endpoint, and credentials. A data source defines how to connect, not what data to query.
- A **[dataset](esql-federated-data-reference.md#dataset-api)** is what to read. For file-based data sources, this is a resource path or glob pattern over files. A dataset is the entity you query by name, just like a regular index.
- **Compression** is how bytes are unwrapped before text files are parsed. For example, `logs.ndjson.gz` is recognized as a gzip-compressed NDJSON resource.
- A **format reader** is how records are parsed after the bytes are available. Parquet, CSV, TSV, and NDJSON each have their own reader behavior and settings.

In practice, you create a data source first, then create one or more datasets that reference it. The dataset selects the resource to read and, when needed, overrides format-reader settings such as CSV delimiters or schema sampling. Compression is inferred from the resource extension for supported text formats.

One data source can serve many datasets. When credentials rotate, you update the data source in one place without touching the datasets that reference it.

Datasets share the same namespace as indices, aliases, and views. A dataset cannot have the same name as an existing index.

Once a dataset exists, you query it with `FROM`:

```esql
FROM access_logs
| WHERE status_code >= 400
| STATS error_count = COUNT(*) BY service_name
| SORT error_count DESC
| LIMIT 20
```

There is no special syntax for federated data. `FROM access_logs` works the same way whether `access_logs` is a native {{es}} index or a dataset pointing at external storage.

## Supported data source types

The following data source types are supported:

:::{include} _snippets/federated-data/supported-data-source-types.md
:::

## Supported file formats

Federated data sources can read the following file formats:

:::{include} _snippets/federated-data/supported-file-formats.md
:::

The format is detected automatically from the file extension. You can override this in the dataset settings if needed.

For details on type-specific settings and format options, refer to the [federated data reference](esql-federated-data-reference.md).

## Capabilities and limitations

Most {{esql}} processing commands and functions work on datasets. The execution engine is the same one used for native indices. Some capabilities that depend on Lucene or specialized data structures are not currently available.

For the full list, refer to the [federated data reference](esql-federated-data-reference.md#functionality-that-does-not-work-or-works-differently-on-datasets).

## Next steps

- To set up and query your first data source, refer to [](esql-federated-data-quickstart.md).
- For detailed settings, format options, and the full capabilities list, refer to the [federated data reference](esql-federated-data-reference.md).
