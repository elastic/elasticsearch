```yaml {applies_to}
serverless: preview
stack: preview 9.6+
```

The `DENSE_VECTOR` command generates vector embeddings from one or more `text`
or `keyword` columns. It embeds each row independently through an inference
endpoint and appends the resulting `dense_vector` columns to the query output.
You can embed indexed or computed text, or base64-encoded image data.

## Syntax

```esql
DENSE_VECTOR <input_column> [, <input_column>, ...] [WITH { <options> }]
DENSE_VECTOR <output_column> = <input_column> [WITH { <options> }]
DENSE_VECTOR suffix = "<suffix>" ON <input_column> [, <input_column>, ...] [WITH { <options> }]
```

## Parameters

`input_column`
:   (Required) One or more comma-separated `text` or `keyword` columns to
    embed. For image input, the column must contain a base64-encoded image data
    URI. A non-string column is rejected before execution. If a value is `null`,
    the generated vector is also `null`.

`output_column`
:   (Optional) The name of the generated column. You can specify an output
    column name only when embedding one input column. If the name matches an
    existing column, the generated column replaces it.

`suffix`
:   (Optional) A quoted string appended to each input column name. For example,
    `DENSE_VECTOR suffix = "_dv" ON title, body` produces `title_dv` and
    `body_dv`. You can use a suffix with one or more input columns. Without a
    naming clause, output columns use the name `<input_column>_dense_vector`.

## WITH options

`inference_id`
:   (Optional) The ID of the
    [inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md)
    used to embed the input. If omitted, `DENSE_VECTOR` selects a default text
    embedding endpoint. See [Inference endpoints](#inference-endpoints).

`type`
:   (Optional) The input modality. Accepts `text` (default) or `image`. An `image`
    input is a base64 data URI embedded through a multimodal endpoint.

`timeout`
:   (Optional) Timeout for the inference request (for example, `"30s"`, `"1m"`).
    If not specified, the default inference timeout applies.

## Description

`DENSE_VECTOR` adds one `dense_vector` output column for each input column and
preserves the other columns in the result. You can embed columns loaded from an
index or columns computed earlier in the query, for example with `EVAL`.

Use the generated vectors with vector similarity functions such as
[`V_COSINE`](/reference/query-languages/esql/functions-operators/dense-vector-functions.md)
or commands such as [`MMR`](/reference/query-languages/esql/commands/mmr.md).

:::{tip}
Use `DENSE_VECTOR` to embed values that vary between rows. To embed a single
constant value, such as query text, use the
[`TEXT_EMBEDDING`](/reference/query-languages/esql/functions-operators/dense-vector-functions/text_embedding.md)
function.
:::

Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).

## Requirements

### Inference endpoints

`DENSE_VECTOR` uses an
[inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put)
to embed its input. The endpoint must support the selected input type:

- For text input, the endpoint must use the `text_embedding` or multimodal
  `embedding` task type. If you omit `inference_id`, `DENSE_VECTOR` uses
  `esql.command.dense_vector.default_inference_id` when configured. Otherwise,
  it selects an available built-in text embedding endpoint.
- For image input, the endpoint must use the multimodal `embedding` task type.
  No default image endpoint is available, so you must specify `inference_id`.

If no endpoint resolves, for example on a deployment that has neither built-in
endpoint, the query fails with an error naming each candidate and why it was
rejected. Specify `inference_id` to select an endpoint explicitly.

Embeddings produced by different models generally do not share the same vector
space. Specify `inference_id` when generated vectors must remain comparable
across queries or deployments.

In a [cross-cluster query](/reference/query-languages/esql/esql-cross-clusters.md#ccq-inference-endpoints),
`DENSE_VECTOR` runs on the cluster that receives the query. The inference
endpoint must exist on that cluster, even when the documents come from a remote
cluster.

## Resource controls

Use the following dynamic cluster settings to control `DENSE_VECTOR` resource
usage and availability:

| Setting | Default | Purpose |
| --- | --- | --- |
| `esql.command.dense_vector.enabled` | `true` | Controls whether the command is available. |
| `esql.command.dense_vector.limit` | `1000` | Sets the maximum number of input rows processed by the command. |
| `esql.command.dense_vector.batch_size` | `20` | Sets the maximum number of inputs combined in one inference request. Accepts values from `1` to `1000`. |
| `esql.command.dense_vector.default_inference_id` | Not set | Selects a default endpoint when a query omits `inference_id`. |

## Limitations

For a multivalued input column, `DENSE_VECTOR` embeds only the first value and
reports a warning for the discarded values. The first value depends on how the
column is loaded. To select the value explicitly, reduce the column first with a
function such as
[`MV_FIRST`](/reference/query-languages/esql/functions-operators/mv-functions/mv_first.md).

## Examples

The following examples use columns from a `books` index, unless noted otherwise.

### Embed a column

Embed a text column and add an output column named `<input_column>_dense_vector`:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorSingleFieldForDocs.md
:::

### Embed multiple columns

List multiple input columns to embed them with one command:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorMultipleFieldsForDocs.md
:::

### Specify an output column name

Use `output_column = input_column` to replace the default output name:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorNamedOutputForDocs.md
:::

### Apply a suffix to output column names

Use `suffix = "..." ON` to replace the default `_dense_vector` suffix for
each listed column:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorSuffixForDocs.md
:::

### Embed a computed column

Embed a `text` or `keyword` column created earlier in the query. In this example,
`EVAL` creates the value that `DENSE_VECTOR` embeds:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorComputedColumnForDocs.md
:::

### Diversify results with `MMR`

Pass the generated column to [`MMR`](/reference/query-languages/esql/commands/mmr.md)
to remove near-duplicate rows. `MMR` can use the runtime `dense_vector`, so this
workflow does not require an indexed vector field:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorThenMmrForDocs.md
:::

### Embed image data

Set `"type": "image"` to embed a base64-encoded image data URI through a
multimodal endpoint. This example uses `ROW` because the input is a literal data
URI rather than an indexed column:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorImageForDocs.md
:::
