---
navigation_title: "Processor reference"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/processors.html
---

# Ingest processor reference [processors]

:::{note}
This section provides detailed **reference information** for ingest processors.

Refer to [Transform and enrich data](docs-content://manage-data/ingest/transform-enrich.md) in the **Manage data** section for overview and conceptual information.
:::

An [ingest pipeline](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md) is made up of a sequence of processors that are applied to documents as they are ingested into an index. Each processor performs a specific task, such as filtering, transforming, or enriching data.

Each successive processor depends on the output of the previous processor, so the order of processors is important. The modified documents are indexed into {{es}} after all processors are applied.

{{es}} includes over 40 configurable processors. The subpages in this section contain reference documentation for each processor. To get a list of available processors, use the [nodes info](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-info) API.

```console
GET _nodes/ingest?filter_path=nodes.*.ingest.processors
```


## Ingest processors by category [ingest-processors-categories]

We’ve categorized the available processors on this page and summarized their functions. This will help you find the right processor for your use case.

* [Data enrichment processors](#ingest-process-category-data-enrichment)
* [Data transformation processors](#ingest-process-category-data-transformation)
* [Data filtering processors](#ingest-process-category-data-filtering)
* [Pipeline handling processors](#ingest-process-category-pipeline-handling)
* [Array/JSON handling processors](#ingest-process-category-array-json-handling)


## Data enrichment processors [ingest-process-category-data-enrichment]


### General outcomes [ingest-process-category-data-enrichment-general]

[`append` processor](/reference/enrich-processor/append-processor.md)
:   Appends a value to a field.

[`date_index_name` processor](/reference/enrich-processor/date-index-name-processor.md)
:   Points documents to the right time-based index based on a date or timestamp field.

[`enrich` processor](/reference/enrich-processor/enrich-processor.md)
:   Enriches documents with data from another index.

::::{tip}
Refer to [Enrich your data](docs-content://manage-data/ingest/transform-enrich/data-enrichment.md) for detailed examples of how to use the `enrich` processor to add data from your existing indices to incoming documents during ingest.

::::


[`inference` processor](/reference/enrich-processor/inference-processor.md)
:   Uses {{ml}} to classify and tag text fields.


### Specific outcomes [ingest-process-category-data-enrichment-specific]

[`attachment` processor](/reference/enrich-processor/attachment.md)
:   Parses and indexes binary data, such as PDFs and Word documents.

[`circle` processor](/reference/enrich-processor/ingest-circle-processor.md)
:   Converts a location field to a Geo-Point field.

[`community_id` processor](/reference/enrich-processor/community-id-processor.md)
:   Computes the Community ID for network flow data.

[`fingerprint` processor](/reference/enrich-processor/fingerprint-processor.md)
:   Computes a hash of the document’s content.

[`geo_grid` processor](/reference/enrich-processor/ingest-geo-grid-processor.md)
:   Converts geo-grid definitions of grid tiles or cells to regular bounding boxes or polygons which describe their shape.

[`geoip` processor](/reference/enrich-processor/geoip-processor.md)
:   Adds information about the geographical location of an IPv4 or IPv6 address from a Maxmind database.

[`ip_location` processor](/reference/enrich-processor/ip-location-processor.md)
:   Adds information about the geographical location of an IPv4 or IPv6 address from an ip geolocation database.

[`network_direction` processor](/reference/enrich-processor/network-direction-processor.md)
:   Calculates the network direction given a source IP address, destination IP address, and a list of internal networks.

[`normalize_for_stream` processor](/reference/enrich-processor/normalize-for-stream.md)
:   Normalizes non-OpenTelemetry documents to be OpenTelemetry-compliant.

[`registered_domain` processor](/reference/enrich-processor/registered-domain-processor.md)
:   Extracts the registered domain (also known as the effective top-level domain or eTLD), sub-domain, and top-level domain from a fully qualified domain name (FQDN).

[`set_security_user` processor](/reference/enrich-processor/ingest-node-set-security-user-processor.md)
:   Sets user-related details (such as `username`,  `roles`, `email`, `full_name`,`metadata`, `api_key`, `realm` and `authentication_type`) from the current authenticated user.

[`uri_parts` processor](/reference/enrich-processor/uri-parts-processor.md)
:   Parses a Uniform Resource Identifier (URI) string and extracts its components as an object.

[`urldecode` processor](/reference/enrich-processor/urldecode-processor.md)
:   URL-decodes a string.

[`user_agent` processor](/reference/enrich-processor/user-agent-processor.md)
:   Parses user-agent strings to extract information about web clients.


## Data transformation processors [ingest-process-category-data-transformation]


### General outcomes [ingest-process-category-data-transformation-general]

[`convert` processor](/reference/enrich-processor/convert-processor.md)
:   Converts a field in the currently ingested document to a different type, such as converting a string to an integer.

[`dissect` processor](/reference/enrich-processor/dissect-processor.md)
:   Extracts structured fields out of a single text field within a document. Unlike the [grok processor](/reference/enrich-processor/grok-processor.md), dissect does not use regular expressions. This makes the dissect’s a simpler and often faster alternative.

[`grok` processor](/reference/enrich-processor/grok-processor.md)
:   Extracts structured fields out of a single text field within a document, using the [Grok](docs-content://explore-analyze/scripting/grok.md) regular expression dialect that supports reusable aliased expressions.

[`gsub` processor](/reference/enrich-processor/gsub-processor.md)
:   Converts a string field by applying a regular expression and a replacement.

[`redact` processor](/reference/enrich-processor/redact-processor.md)
:   Uses the [Grok](docs-content://explore-analyze/scripting/grok.md) rules engine to obscure text in the input document matching the given Grok patterns.

[`rename` processor](/reference/enrich-processor/rename-processor.md)
:   Renames an existing field.

[`set` processor](/reference/enrich-processor/set-processor.md)
:   Sets a value on a field.


### Specific outcomes [ingest-process-category-data-transformation-specific]

[`bytes` processor](/reference/enrich-processor/bytes-processor.md)
:   Converts a human-readable byte value to its value in bytes (for example `1kb` becomes `1024`).

[`csv` processor](/reference/enrich-processor/csv-processor.md)
:   Extracts a single line of CSV data from a text field.

[`date` processor](/reference/enrich-processor/date-processor.md)
:   Extracts and converts date fields.

[`dot_expand` processor](/reference/enrich-processor/dot-expand-processor.md)
:   Expands a field with dots into an object field.

[`html_strip` processor](/reference/enrich-processor/htmlstrip-processor.md)
:   Removes HTML tags from a field.

[`join` processor](/reference/enrich-processor/join-processor.md)
:   Joins each element of an array into a single string using a separator character between each element.

[`kv` processor](/reference/enrich-processor/kv-processor.md)
:   Parse messages (or specific event fields) containing key-value pairs.

[`lowercase` processor](/reference/enrich-processor/lowercase-processor.md) and [`uppercase` processor](/reference/enrich-processor/uppercase-processor.md)
:   Converts a string field to lowercase or uppercase.

[`split` processor](/reference/enrich-processor/split-processor.md)
:   Splits a field into an array of values.

[`trim` processor](/reference/enrich-processor/trim-processor.md)
:   Trims whitespace from field.


## Data filtering processors [ingest-process-category-data-filtering]

[`drop` processor](/reference/enrich-processor/drop-processor.md)
:   Drops the document without raising any errors.

[`remove` processor](/reference/enrich-processor/remove-processor.md)
:   Removes fields from documents.


## Pipeline handling processors [ingest-process-category-pipeline-handling]

[`fail` processor](/reference/enrich-processor/fail-processor.md)
:   Raises an exception. Useful for when you expect a pipeline to fail and want to relay a specific message to the requester.

[`pipeline` processor](/reference/enrich-processor/pipeline-processor.md)
:   Executes another pipeline.

[`reroute` processor](/reference/enrich-processor/reroute-processor.md)
:   Reroutes documents to another target index or data stream.

[`terminate` processor](/reference/enrich-processor/terminate-processor.md)
:   Terminates the current ingest pipeline, causing no further processors to be run.


## Array/JSON handling processors [ingest-process-category-array-json-handling]

[`for_each` processor](/reference/enrich-processor/foreach-processor.md)
:   Runs an ingest processor on each element of an array or object.

[`json` processor](/reference/enrich-processor/json-processor.md)
:   Converts a JSON string into a structured JSON object.

[`script` processor](/reference/enrich-processor/script-processor.md)
:   Runs an inline or stored [script](docs-content://explore-analyze/scripting.md) on incoming documents. The script runs in the [painless `ingest` context](/reference/scripting-languages/painless/painless-ingest-processor-context.md).

[`sort` processor](/reference/enrich-processor/sort-processor.md)
:   Sorts the elements of an array in ascending or descending order.


## Add additional processors [ingest-process-plugins]

You can install additional processors as [plugins](/reference/elasticsearch-plugins/index.md).

You must install any plugin processors on all nodes in your cluster. Otherwise, {{es}} will fail to create pipelines containing the processor.

Mark a plugin as mandatory by setting `plugin.mandatory` in `elasticsearch.yml`. A node will fail to start if a mandatory plugin is not installed.

```yaml
plugin.mandatory: my-ingest-plugin
```














































