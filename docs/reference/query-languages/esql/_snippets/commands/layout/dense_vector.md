```yaml {applies_to}
serverless: preview
stack: preview 9.6+
```

The `DENSE_VECTOR` command generates a `dense_vector` embedding for one or more
text or keyword fields, within ES|QL queries. For each input field it
calls an inference model and appends a new `dense_vector` column to every row. By
default, it embeds the field's text; set the `type` option to embed images instead.
Each row is embedded independently, so you can vectorize
an indexed or computed column as part of a query.

## Syntax

```esql
DENSE_VECTOR [column = | suffix = "<suffix>" ON] field [, field, ...] [WITH { "inference_id" : "my_inference_endpoint" [, "type" : "text" | "image"] [, "timeout" : "<timeout_duration>"] }]
```

## Parameters

`field`
:   One or more comma-separated columns to embed. Fields must be `text` or
    `keyword`. To embed images, store a base64 image data URI in one of these string
    fields and set the `type` option to `image`. A non-string field is rejected
    before execution. If a field's value is `null`, its generated vector is `null`.

`column`
:   (Optional) Names the single generated column outright, for example
    `DENSE_VECTOR vector = title`. Valid only when embedding one field, since one
    name cannot serve several. If the name matches an existing column, the existing
    column is replaced.

`suffix`
:   (Optional) A quoted string appended to each input field's name to build its
    output column, for example `DENSE_VECTOR suffix = "_dv" ON title, body`
    produces `title_dv` and `body_dv`. Works for any number of fields. With no
    naming clause at all, each field defaults to `<field>_dense_vector`.

## WITH options

`inference_id`
:   (Optional) The ID of the
    [inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md)
    to use. For `text` input the endpoint must have the `text_embedding` or a
    multimodal `embedding` task type; for `image` input it must be a multimodal
    `embedding` endpoint. If not specified, `DENSE_VECTOR` uses a preconfigured
    default endpoint (see [Requirements](#requirements)).

`type`
:   (Optional) The input modality. Accepts `text` (default) or `image`. An `image`
    input is a base64 data URI embedded through a multimodal endpoint.

`timeout`
:   (Optional) Timeout for the inference request (for example, `"30s"`, `"1m"`).
    If not specified, the default inference timeout applies.

## Description

Use `DENSE_VECTOR` to embed text or images into vectors as part of a query, for
example to feed vector similarity functions such as
[`V_COSINE`](/reference/query-languages/esql/functions-operators/dense-vector-functions.md)
or to diversify results with [`MMR`](/reference/query-languages/esql/commands/mmr.md).

:::{tip}
`DENSE_VECTOR` embeds a **field**, once per row, to vectorize the data flowing through
your query. To embed a single **constant** value instead, such as a query string, use the
[`TEXT_EMBEDDING`](/reference/query-languages/esql/functions-operators/dense-vector-functions/text_embedding.md)
function.
:::

`DENSE_VECTOR` **adds** columns: it produces a new `dense_vector` column for each
input field and keeps the existing columns. Because it
embeds each row independently, you can embed any `text` or `keyword` column,
a source field from an index, or one computed earlier in the query (for example, with `EVAL`).

For a multivalued field, only the first value is embedded and a warning records
that the rest were discarded. Which value is "first" depends on how the field is
loaded, so it's good practice to reduce the field first, for example with
[`MV_FIRST`](/reference/query-languages/esql/functions-operators/mv-functions/mv_first.md),
to choose the value explicitly.

:::{tip}
Learn more about using [ES|QL for search use cases](docs-content://solutions/search/esql-for-search.md).
:::

In a [cross-cluster query](/reference/query-languages/esql/esql-cross-clusters.md#ccq-inference-endpoints),
`DENSE_VECTOR` runs on the cluster that receives the query, so the inference
endpoint must exist on that cluster even when the documents come from a remote.

## Requirements

`DENSE_VECTOR` calls an
[inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put)
to embed its input, and the endpoint has to match the input type. For **text**, if you
omit `inference_id` it uses a preconfigured default text embedding endpoint, so the
command works with zero configuration; change the cluster default with
`esql.command.dense_vector.default_inference_id`, or name a specific endpoint in the
`WITH` clause. For **images** (`"type": "image"`), there is no default, pass the ID of a
multimodal embedding endpoint in the `WITH` clause.

Different default endpoints produce vectors that are not comparable with each other,
so pin a specific `inference_id` if you need consistent vectors.

`DENSE_VECTOR` limits processing to **1000 rows by default** to prevent accidental
high consumption. Adjust it with the `esql.command.dense_vector.limit` cluster
setting, or disable the command entirely with
`esql.command.dense_vector.enabled`.

## Examples

These examples build on a single `FROM books` query, adding one capability at a time.

The simplest form omits `inference_id` and uses the default endpoint, no configuration needed:

```esql
FROM books
| DENSE_VECTOR title
```

### Embed a field

Read a text field and append a `dense_vector` to each row, every row is embedded
on its own. With no naming clause, the output column is named `<field>_dense_vector`:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorSingleFieldForDocs.md
:::

### Name the output column

Use `column =` to name the generated column instead of the default:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorNamedOutputForDocs.md
:::

### Embed multiple fields

List several fields to embed them all in one command:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorMultipleFieldsForDocs.md
:::

### Rename every output with a suffix

Use `suffix = "..." ON` to replace the default `_dense_vector` suffix on every
listed field:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorSuffixForDocs.md
:::

### Embed a computed field

`DENSE_VECTOR` embeds any `text` or `keyword` column, including one built earlier in the query.
Here `EVAL` composes a value that is then embedded:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorComputedColumnForDocs.md
:::

### Diversify results with MMR

Feed the generated column into [`MMR`](/reference/query-languages/esql/commands/mmr.md) to drop
near-duplicate rows. `MMR` diversifies on the runtime `dense_vector`, so no indexed
field is required:

:::{include} ../../generated/x-pack-esql/commands/examples/dense_vector_command.csv-spec/denseVectorThenMmrForDocs.md
:::
