## `RERANK` [esql-rerank]

```yaml {applies_to}
serverless: preview
stack: preview 9.2.0
```

The `RERANK` command uses an inference model to compute a new relevance score
for an initial set of documents, directly within your ES|QL queries.

**Syntax**

```esql
RERANK [column =] query ON field [, field, ...] (WITH { "inference_id" : "my_inference_endpoint" })
```

**Parameters**

`column`
:   (Optional) The name of the output column containing the reranked scores.
If not specified, the results will be stored in a column named `_score`.
If the specified column already exists, it will be overwritten with the new
results.

`query`
:   The query text used to rerank the documents. This is typically the same
query used in the initial search.

`field`
:   One or more fields to use for reranking. These fields should contain the
text that the reranking model will evaluate.

`my_inference_endpoint`
:   The ID of
the [inference endpoint](docs-content://explore-analyze/elastic-inference/inference-api.md)
to use for the task.
The inference endpoint must be configured with the `rerank` task type.

**Description**

The `RERANK` command uses an inference model to compute a new relevance score
for an initial set of documents, directly within your ES|QL queries.

Typically, you first use a `WHERE` clause with a function like `MATCH` to
retrieve an initial set of documents. This set is often sorted by `_score` and
reduced to the top results (for example, 100) using `LIMIT`. The `RERANK`
command then processes this smaller, refined subset, which is a good balance
between performance and accuracy.

**Requirements**

To use this command, you must deploy your reranking model in Elasticsearch as
an [inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put)
with the
task type `rerank`.

#### Handling timeouts

`RERANK` commands may time out when processing large datasets or complex
queries. The default timeout is 10 minutes, but you can increase this limit if
necessary.

How you increase the timeout depends on your deployment type:

::::{tab-set}
:::{tab-item} {{ech}}

* You can adjust {{es}} settings in
  the [Elastic Cloud Console](docs-content://deploy-manage/deploy/elastic-cloud/edit-stack-settings.md)
* You can also adjust the `search.default_search_timeout` cluster setting
  using [Kibana's Advanced settings](kibana://reference/advanced-settings.md#kibana-search-settings)
  :::

:::{tab-item} Self-managed

* You can configure at the cluster level by setting
  `search.default_search_timeout` in `elasticsearch.yml` or updating
  via [Cluster Settings API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-put-settings)
* You can also adjust the `search:timeout` setting
  using [Kibana's Advanced settings](kibana://reference/advanced-settings.md#kibana-search-settings)
* Alternatively, you can add timeout parameters to individual queries
  :::

:::{tab-item} {{serverless-full}}

* Requires a manual override from Elastic Support because you cannot modify
  timeout settings directly
  :::
  ::::

If you don't want to increase the timeout limit, try the following:

* Reduce data volume with `LIMIT` or more selective filters before the `RERANK`
  command
* Split complex operations into multiple simpler queries
* Configure your HTTP client's response timeout (Refer
  to [HTTP client configuration](/reference/elasticsearch/configuration-reference/networking-settings.md#_http_client_configuration))

**Examples**

Rerank search results using a simple query and a single field:

```esql
FROM books
| WHERE MATCH(title, "science fiction")
| SORT _score DESC
| LIMIT 100
| RERANK "science fiction" ON (title) WITH { "inference_id" : "my_reranker" }
| LIMIT 3
| KEEP title, _score
```

| title:keyword | _score:double |
|---------------|---------------|
| Neuromancer   | 0.98          |
| Dune          | 0.95          |
| Foundation    | 0.92          |

Rerank search results using a query and multiple fields, and store the new score
in a column named `rerank_score`:

```esql
FROM movies
| WHERE MATCH(title, "dystopian future") OR MATCH(synopsis, "dystopian future")
| SORT _score DESC
| LIMIT 100
| RERANK rerank_score = "dystopian future" ON (title, synopsis) WITH { "inference_id" : "my_reranker" }
| SORT rerank_score DESC
| LIMIT 5
| KEEP title, _score, rerank_score
```

| title:keyword   | _score:double | rerank_score:double |
|-----------------|---------------|---------------------|
| Blade Runner    | 8.75          | 0.99                |
| The Matrix      | 9.12          | 0.97                |
| Children of Men | 8.50          | 0.96                |
| Akira           | 8.99          | 0.94                |
| Gattaca         | 8.65          | 0.91                |

Combine the original score with the reranked score:

```esql
FROM movies
| WHERE MATCH(title, "dystopian future") OR MATCH(synopsis, "dystopian future")
| SORT _score DESC
| LIMIT 100
| RERANK rerank_score = "dystopian future" ON (title, synopsis) WITH { "inference_id" : "my_reranker" }
| EVAL original_score = _score, _score = rerank_score + original_score
| SORT _score DESC
| LIMIT 2
| KEEP title, original_score, rerank_score, _score
```

| title:keyword | original_score:double | rerank_score:double | _score:double |
|---------------|-----------------------|---------------------|---------------|
| The Matrix    | 9.12                  | 0.97                | 10.09         |
| Akira         | 8.99                  | 0.94                | 9.93          |


