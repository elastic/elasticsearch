## `COMPLETION` [esql-completion]

```yaml {applies_to}
serverless: preview
stack: preview 9.1.0
```

The `COMPLETION` command allows you to send prompts and context to a Large Language Model (LLM) directly within your ES|QL queries, to perform text generation tasks.

### Syntax

```esql
COMPLETION [column =] prompt WITH inference_id
```

### Parameters

`column`
:   (Optional) The name of the output column containing the LLM's response.
    If not specified, the results will be stored in a column named `completion`.
    If the specified column already exists, it will be overwritten with the new results.

`prompt`
:   The input text or expression used to prompt the LLM.
    This can be a string literal or a reference to a column containing text.

`inference_id`
:   The ID of the [inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md) to use for the task.
    The inference endpoint must be configured with the `completion` task type.

### Description

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

### Requirements

To use this command, you must deploy your LLM model in Elasticsearch as
an [inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) with the
task type `completion`.

#### Handling timeouts

`COMPLETION` commands may time out when processing large datasets or complex prompts. To increase timeout limits for these operations, the configuration methods vary by deployment type:

::::{tab-set}
:::{tab-item} {{ess}}
* Timeouts can be increased through {{es}} settings in the [Elastic Cloud Console](docs-content://deploy-manage/deploy/elastic-cloud/edit-stack-settings.md)
* Adjust the `search.default_search_timeout` cluster setting
:::

:::{tab-item} Self-managed
* Configure at the cluster level:
  * Set `search.default_search_timeout` in `elasticsearch.yml`
  * Or update via Cluster Settings API
* Alternatively, add timeout parameters to individual queries
:::

:::{tab-item} {{serverless-full}}
Requires a manual override from Elastic Support as users cannot modify timeout settings directly.
:::
::::

**Alternative workarounds:**

* Reduce data volume with `LIMIT` or more selective filters before the `COMPLETION` command
* Split complex operations into multiple simpler queries 
* Configure your HTTP client's response timeout (Refer to [HTTP client configuration](/reference/elasticsearch/configuration-reference/networking-settings.md#_http_client_configuration)

**Examples**

Use the default column name (results stored in `completion` column):

```esql
ROW question = "What is Elasticsearch?"
| COMPLETION question WITH test_completion_model
| KEEP question, completion
```

| question:keyword       | completion:keyword                        |
|------------------------|-------------------------------------------|
| What is Elasticsearch? | A distributed search and analytics engine |

Specify the output column (results stored in `answer` column):

```esql
ROW question = "What is Elasticsearch?"
| COMPLETION answer = question WITH test_completion_model
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
| COMPLETION summary = prompt WITH test_completion_model
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
