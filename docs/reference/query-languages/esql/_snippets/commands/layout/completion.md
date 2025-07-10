## `COMPLETION` [esql-completion]

```yaml {applies_to}
serverless: preview
stack: preview 9.1.0
```

The `COMPLETION` processing command generates text completions using a specified LLM (Large Language Model).

**Syntax**

```esql
COMPLETION [column =] prompt WITH inference_id
```

**Parameters**

`column`
:   (Optional) The name of the output column that will contain the completion results.
    If not specified, the results will be stored in a column named `completion`.
    If the specified column already exists, it will be overwritten with the new completion results.

`prompt`
:   The input text or expression that will be used as the prompt for the completion.
    This can be a string literal or a reference to a column containing text.

`inference_id`
:   The ID of the inference endpoint to use for text completion.
    The inference endpoint must be configured with the `completion` task type.

**Description**

The `COMPLETION` command uses a machine learning model to generate text completions based on the provided prompt.

The command works with any LLM deployed to
the [Elasticsearch inference API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put)
and can be chained with other ES|QL commands for further processing.

**Examples**

Basic completion with an inline prompt:

```esql
ROW question = "What is Elasticsearch?"
| COMPLETION answer = question WITH test_completion_model
| KEEP question, answer
```

| question:keyword | answer:keyword |
| --- | --- |
| What is Elasticsearch? | A distributed search and analytics engine |


Summarizing the top 10 highest-rated movies using a prompt:

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

