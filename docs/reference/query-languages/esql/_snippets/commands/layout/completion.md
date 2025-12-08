
```yaml {applies_to}
serverless: preview
stack: preview 9.1.0
```

The `COMPLETION` command allows you to send prompts and context to a Large Language Model (LLM) directly within your ES|QL queries, to perform text generation tasks.

:::{important}
**Every row processed by the COMPLETION command generates a separate API call to the LLM endpoint.**

::::{tab-set}

:::{tab-item} 9.3.0+

Starting in version 9.3.0, `COMPLETION` automatically limits processing to **100 rows by default** to prevent accidental high consumption and costs. This limit is applied before the `COMPLETION` command executes.

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

:::{tab-item} 9.1.x - 9.2.x

Be careful to test with small datasets first before running on production data or in automated workflows, to avoid unexpected costs.

Best practices:

1. **Start with dry runs**: Validate your query logic and row counts by running without `COMPLETION` initially. Use `| STATS count = COUNT(*)` to check result size.
2. **Filter first**: Use `WHERE` clauses to limit rows before applying `COMPLETION`.
3. **Test with `LIMIT`**: Always start with a low [`LIMIT`](/reference/query-languages/esql/commands/limit.md) and gradually increase.
4. **Monitor usage**: Track your LLM API consumption and costs.
:::

::::
:::

**Syntax**

::::{tab-set}

:::{tab-item} 9.2.0+

```esql
COMPLETION [column =] prompt WITH { "inference_id" : "my_inference_endpoint" }
```

:::

:::{tab-item} 9.1.x only

```esql
COMPLETION [column =] prompt WITH my_inference_endpoint
```

:::

::::

**Parameters**

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

**Description**

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

**Requirements**

To use this command, you must deploy your LLM model in Elasticsearch as
an [inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) with the
task type `completion`.

#### Handling timeouts

`COMPLETION` commands may time out when processing large datasets or complex prompts. The default timeout is 10 minutes, but you can increase this limit if necessary.

How you increase the timeout depends on your deployment type:

::::{tab-set}
:::{tab-item} {{ech}}
* You can adjust {{es}} settings in the [Elastic Cloud Console](docs-content://deploy-manage/deploy/elastic-cloud/edit-stack-settings.md)
* You can also adjust the `search.default_search_timeout` cluster setting using [Kibana's Advanced settings](kibana://reference/advanced-settings.md#kibana-search-settings)
:::

:::{tab-item} Self-managed
* You can configure at the cluster level by setting `search.default_search_timeout` in `elasticsearch.yml` or updating via [Cluster Settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)
* You can also adjust the `search:timeout` setting using [Kibana's Advanced settings](kibana://reference/advanced-settings.md#kibana-search-settings)
* Alternatively, you can add timeout parameters to individual queries
:::

:::{tab-item} {{serverless-full}}
* Requires a manual override from Elastic Support because you cannot modify timeout settings directly
:::
::::

If you don't want to increase the timeout limit, try the following:

* Reduce data volume with `LIMIT` or more selective filters before the `COMPLETION` command
* Split complex operations into multiple simpler queries
* Configure your HTTP client's response timeout (Refer to [HTTP client configuration](/reference/elasticsearch/configuration-reference/networking-settings.md#_http_client_configuration))


**Examples**

Use the default column name (results stored in `completion` column):

```esql
ROW question = "What is Elasticsearch?"
| COMPLETION question WITH { "inference_id" : "my_inference_endpoint" }
| KEEP question, completion
```

| question:keyword       | completion:keyword                        |
|------------------------|-------------------------------------------|
| What is Elasticsearch? | A distributed search and analytics engine |

Specify the output column (results stored in `answer` column):

```esql
ROW question = "What is Elasticsearch?"
| COMPLETION answer = question WITH { "inference_id" : "my_inference_endpoint" }
| KEEP question, answer
```

| question:keyword | answer:keyword |
| --- | --- |
| What is Elasticsearch? | A distributed search and analytics engine |

Summarize the top 10 highest-rated movies using a prompt:

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
