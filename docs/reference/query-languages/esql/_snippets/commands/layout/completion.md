
```yaml {applies_to}
serverless: ga
stack: preview 9.1.0, ga 9.3.0
```

The `COMPLETION` command allows you to send prompts and context to a Large Language Model (LLM) directly within your ES|QL queries, to perform text generation tasks.

:::::{important}
**Every row processed by the COMPLETION command generates a separate API call to the LLM endpoint.**

::::{applies-switch}

:::{applies-item} stack: ga 9.3+

`COMPLETION` automatically limits processing to **100 rows by default** to prevent accidental high consumption and costs. This limit is applied before the `COMPLETION` command executes.

If you need to process more rows, you can adjust the limit using the cluster setting:
```
PUT _cluster/settings
{
  "persistent": {
    "esql.command.completion.limit": 500
  }
}
```

You can also disable the command entirely if needed:
```
PUT _cluster/settings
{
  "persistent": {
    "esql.command.completion.enabled": false
  }
}
```
:::

:::{applies-item} stack: ga 9.1-9.2

Be careful to test with small datasets first before running on production data or in automated workflows, to avoid unexpected costs.

Best practices:

1. **Start with dry runs**: Validate your query logic and row counts by running without `COMPLETION` initially. Use `| STATS count = COUNT(*)` to check result size.
2. **Filter first**: Use `WHERE` clauses to limit rows before applying `COMPLETION`.
3. **Test with `LIMIT`**: Always start with a low [`LIMIT`](/reference/query-languages/esql/commands/limit.md) and gradually increase.
4. **Monitor usage**: Track your LLM API consumption and costs.
:::

::::
:::::

## Syntax

::::{applies-switch}

:::{applies-item} stack: ga 9.5+

```esql
COMPLETION [column =] prompt WITH { "inference_id" : "my_inference_endpoint" [, "timeout" : "<timeout_duration>"] }
```

:::

:::{applies-item} stack: ga 9.2+

```esql
COMPLETION [column =] prompt WITH { "inference_id" : "my_inference_endpoint" }
```

:::

:::{applies-item} stack: ga =9.1

```esql
COMPLETION [column =] prompt WITH my_inference_endpoint
```

:::

::::

## Parameters

`column`
:   (Optional) The name of the output column containing the LLM's response.
    If not specified, the results will be stored in a column named `completion`.
    If the specified column already exists, it will be overwritten with the new results.

`prompt`
:   The input text or expression used to prompt the LLM.
    This can be a string literal or a reference to a column containing text.

`my_inference_endpoint`
:   The ID of the [inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md) to use for the task.
    The inference endpoint must be configured with the `completion` task type.

`timeout_duration` {applies_to}`stack: ga 9.5+` {applies_to}`serverless: ga`
:   (Optional) Timeout for the inference request (for example, `"30s"`, `"1m"`).
    If not specified, the default inference timeout applies. Use this to set a
    per-call timeout independent of the inference task timeout.

## Description

The `COMPLETION` command provides a general-purpose interface for
text generation tasks using a Large Language Model (LLM) in ES|QL.

`COMPLETION` supports a wide range of text generation tasks. Depending on your
prompt and the model you use, you can perform arbitrary text generation tasks
including:

- Question answering
- Summarization
- Translation
- Content rewriting
- Creative generation

## Requirements

To use this command, you must deploy your LLM model in Elasticsearch as
an [inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) with the
task type `completion`.

### Handling timeouts

`COMPLETION` commands may time out when processing large datasets or complex prompts.

::::{applies-switch}

:::{applies-item} {"stack": "ga 9.5", "serverless": "ga"}

The default timeout is 120 seconds.

You can set per-call timeout using the `"timeout"` option in the `WITH` clause:
```esql
COMPLETION answer = question WITH { "inference_id": "my_inference_endpoint", "timeout": "1m" }
```
:::

:::{applies-item} {"stack": "preview 9.1.0, ga 9.4.0"}

The timeout is 30 seconds by default.

:::

::::

If you can't modify your timeout limits, try the following:

* Reduce data volume with `LIMIT` or more selective filters before the `COMPLETION` command
* Split complex operations into multiple simpler queries
* Configure your HTTP client's response timeout (Refer to [HTTP client configuration](/reference/elasticsearch/configuration-reference/networking-settings.md#_http_client_configuration))


## Examples

The following examples show common `COMPLETION` patterns.

### Use the default output column name

If no column name is specified, the response is stored in `completion`:

```esql
ROW question = "What is Elasticsearch?"
| COMPLETION question WITH { "inference_id" : "my_inference_endpoint" }
| KEEP question, completion
```

| question:keyword       | completion:keyword                        |
|------------------------|-------------------------------------------|
| What is Elasticsearch? | A distributed search and analytics engine |

### Specify the output column name

Use `column =` to assign the response to a named column:

```esql
ROW question = "What is Elasticsearch?"
| COMPLETION answer = question WITH { "inference_id" : "my_inference_endpoint" }
| KEEP question, answer
```

| question:keyword | answer:keyword |
| --- | --- |
| What is Elasticsearch? | A distributed search and analytics engine |

### Summarize documents with a prompt

Use `CONCAT` to build a prompt from field values before calling `COMPLETION`:

```esql
FROM movies
| SORT rating DESC
| LIMIT 10
| EVAL prompt = CONCAT(
   "Summarize this movie using the following information: \n",
   "Title: ", title, "\n",
   "Synopsis: ", synopsis, "\n",
   "Actors: ", MV_CONCAT(actors, ", "), "\n",
  )
| COMPLETION summary = prompt WITH { "inference_id" : "my_inference_endpoint" }
| KEEP title, summary, rating
```

| title:keyword | summary:keyword | rating:double |
| --- | --- | --- |
| The Shawshank Redemption | A tale of hope and redemption in prison. | 9.3 |
| The Godfather | A mafia family's rise and fall. | 9.2 |
| The Dark Knight | Batman battles the Joker in Gotham. | 9.0 |
| Pulp Fiction | Interconnected crime stories with dark humor. | 8.9 |
| Fight Club | A man starts an underground fight club. | 8.8 |
| Inception | A thief steals secrets through dreams. | 8.8 |
| The Matrix | A hacker discovers reality is a simulation. | 8.7 |
| Parasite | Class conflict between two families. | 8.6 |
| Interstellar | A team explores space to save humanity. | 8.6 |
| The Prestige | Rival magicians engage in dangerous competition. | 8.5 |
