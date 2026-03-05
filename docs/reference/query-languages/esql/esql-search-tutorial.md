---
applies_to:
  stack:
  serverless:
navigation_title: "ES|QL for search"
---

# {{esql}} for search tutorial

This hands-on tutorial covers full-text search, semantic search, hybrid search, vector search, and AI-powered search capabilities using {{esql}}.

In this scenario, we're implementing search for a cooking blog. The blog contains recipes with various attributes including textual content, categorical data, and numerical ratings.

:::{note}
This tutorial uses a small dataset for learning purposes. The goal is to demonstrate search concepts and {{esql}} syntax.
:::

## Requirements

You need a running {{es}} cluster, together with {{kib}} to use the Dev Tools API Console. Refer to [choose your deployment type](docs-content://deploy-manage/deploy.md#choosing-your-deployment-type) for deployment options.

Want to get started quickly? Run the following command in your terminal to set up a [single-node local cluster in Docker](docs-content://deploy-manage/deploy/self-managed/local-development-installation-quickstart.md):

```sh
curl -fsSL https://elastic.co/start-local | sh
```
% NOTCONSOLE

## Running {{esql}} queries

In this tutorial, {{esql}} examples are displayed in the following format:

```esql
FROM cooking_blog
| WHERE description:"fluffy pancakes"
| LIMIT 1000
```

If you want to run these queries in the [Dev Tools Console](/reference/query-languages/esql/esql-rest.md#esql-kibana-console), you need to use the following syntax:

```console
POST /_query?format=txt
{
  "query": """
    FROM cooking_blog
    | WHERE description:"fluffy pancakes"
    | LIMIT 1000
  """
}
```

If you'd prefer to use your favorite programming language, refer to [Client libraries](docs-content://solutions/search/site-or-app/clients.md) for a list of official and community-supported clients.

## Step 1: Create an index

Create the `cooking_blog` index to get started:

```console
PUT /cooking_blog
```

Now define the mappings for the index:

```console
PUT /cooking_blog/_mapping
{
  "properties": {
    "title": {
      "type": "text",
      "analyzer": "standard", <1>
      "fields": { <2>
        "keyword": {
          "type": "keyword",
          "ignore_above": 256 <3>
        }
      }
    },
    "description": {
      "type": "text" <1>
    },
    "author": {
      "type": "text",
      "fields": {
        "keyword": {
          "type": "keyword"
        }
      }
    },
    "date": {
      "type": "date",
      "format": "yyyy-MM-dd"
    },
    "category": {
      "type": "text",
      "fields": {
        "keyword": {
          "type": "keyword"
        }
      }
    },
    "tags": {
      "type": "text",
      "fields": {
        "keyword": {
          "type": "keyword"
        }
      }
    },
    "rating": {
      "type": "float"
    }
  }
}
```

1. The `standard` analyzer is used by default for `text` fields. It's included here explicitly for demonstration purposes. To know more about analyzers, refer to [Anatomy of an analyzer](docs-content://manage-data/data-store/text-analysis/anatomy-of-an-analyzer.md).
2. [Multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md) enable both full-text search and exact matching/filtering on the same field. The `title`, `author`, `category`, and `tags` fields are declared with both `text` and `keyword` data types. The `text` version allows full-text search, while the `keyword` version enables exact filtering. If you use [dynamic mapping](docs-content://manage-data/data-store/mapping/dynamic-field-mapping.md), these multi-fields will be created automatically.
3. The `ignore_above` parameter prevents indexing values longer than 256 characters in the `keyword` field. This is the default value and it's included here for demonstration purposes. For more information, refer to the [ignore_above parameter](/reference/elasticsearch/mapping-reference/ignore-above.md).
4. `keyword` fields store exact values and are used for filtering, sorting, and aggregations. Unlike `text` fields, they are not analyzed. Use `keyword` when you need exact matching (e.g., filtering by category or finding a specific author name), but use `text` when you want to search within longer content like descriptions.

::::{tip}
Full-text search is powered by [text analysis](docs-content://solutions/search/full-text/text-analysis-during-search.md). Text analysis normalizes and standardizes text data so it can be efficiently stored in an inverted index and searched in near real-time. Analysis happens at both [index and search time](docs-content://manage-data/data-store/text-analysis/index-search-analysis.md). This tutorial won't cover analysis in detail, but it's important to understand how text is processed to create effective search queries.
::::

## Step 2: Add sample blog posts to your index [full-text-filter-tutorial-index-data]

Next, you’ll need to index some example blog posts using the [Bulk API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings). Note that `text` fields are analyzed and multi-fields are generated at index time.

```console
POST /cooking_blog/_bulk?refresh=wait_for
{"index":{"_id":"1"}}
{"title":"Perfect Pancakes: A Fluffy Breakfast Delight","description":"Learn the secrets to making the fluffiest pancakes, so amazing you won't believe your tastebuds. This recipe uses buttermilk and a special folding technique to create light, airy pancakes that are perfect for lazy Sunday mornings.","author":"Maria Rodriguez","date":"2023-05-01","category":"Breakfast","tags":["pancakes","breakfast","easy recipes"],"rating":4.8}
{"index":{"_id":"2"}}
{"title":"Spicy Thai Green Curry: A Vegetarian Adventure","description":"Dive into the flavors of Thailand with this vibrant green curry. Packed with vegetables and aromatic herbs, this dish is both healthy and satisfying. Don't worry about the heat - you can easily adjust the spice level to your liking.","author":"Liam Chen","date":"2023-05-05","category":"Main Course","tags":["thai","vegetarian","curry","spicy"],"rating":4.6}
{"index":{"_id":"3"}}
{"title":"Classic Beef Stroganoff: A Creamy Comfort Food","description":"Indulge in this rich and creamy beef stroganoff. Tender strips of beef in a savory mushroom sauce, served over a bed of egg noodles. It's the ultimate comfort food for chilly evenings.","author":"Emma Watson","date":"2023-05-10","category":"Main Course","tags":["beef","pasta","comfort food"],"rating":4.7}
{"index":{"_id":"4"}}
{"title":"Vegan Chocolate Avocado Mousse","description":"Discover the magic of avocado in this rich, vegan chocolate mousse. Creamy, indulgent, and secretly healthy, it's the perfect guilt-free dessert for chocolate lovers.","author":"Alex Green","date":"2023-05-15","category":"Dessert","tags":["vegan","chocolate","avocado","healthy dessert"],"rating":4.5}
{"index":{"_id":"5"}}
{"title":"Crispy Oven-Fried Chicken","description":"Get that perfect crunch without the deep fryer! This oven-fried chicken recipe delivers crispy, juicy results every time. A healthier take on the classic comfort food.","author":"Maria Rodriguez","date":"2023-05-20","category":"Main Course","tags":["chicken","oven-fried","healthy"],"rating":4.9}
```

## Step 3: Basic search operations

Full-text search involves executing text-based queries across one or more document fields. In this section, you'll start with simple text matching and build up to understanding how search results are ranked.

{{esql}} provides multiple functions for full-text search, including `MATCH`, `MATCH_PHRASE`, and `QSTR`. For basic text matching, you can use either:

1. Full [match function](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) syntax: `match(field, "search terms")`
2. Compact syntax using the [match operator "`:`"](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator): `field:"search terms"`

Both are equivalent for basic matching and can be used interchangeably. The compact syntax is more concise, while the function syntax allows for more configuration options. We use the compact syntax in most examples for brevity.

Refer to the [`MATCH` function](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) reference docs for advanced parameters available with the function syntax.

### Perform your first search query

Let's start with the simplest possible search - looking for documents that contain specific words:

```esql
FROM cooking_blog
| WHERE description:"fluffy pancakes"
| LIMIT 1000
```

This query searches the `description` field for documents containing either "fluffy" OR "pancakes" (or both). By default, {{esql}} uses OR logic between search terms, so it matches documents that contain any of the specified words.

### Control which fields appear in results

You can specify the exact fields to include in your results using the `KEEP` command:

```esql
FROM cooking_blog
| WHERE description:"fluffy pancakes"
| KEEP title, description, rating
| LIMIT 1000
```

This helps reduce the amount of data returned and focuses on the information you need.

### Understand relevance scoring

Search results can be ranked based on how well they match your query. To calculate and use relevance scores, you need to explicitly request the `_score` metadata:

```esql
FROM cooking_blog METADATA _score
| WHERE description:"fluffy pancakes"
| KEEP title, description, _score
| SORT _score DESC
| LIMIT 1000
```

Notice two important things:
1. `METADATA _score` tells {{esql}} to include relevance scores in the results
2. `SORT _score DESC` orders results by relevance (highest scores first)

If you don't include `METADATA _score` in your query, you won't see relevance scores in your results. This means you won't be able to sort by relevance or filter based on relevance scores.

Without explicit sorting, results aren't ordered by relevance even when scores are calculated. If you want the most relevant results first, you must sort by `_score`, by explicitly using `SORT _score DESC` or `SORT _score ASC`.

:::{tip}
When you include `METADATA _score`, search functions included in `WHERE` conditions contribute to the relevance score. Filtering operations (like range conditions and exact matches) don't affect the score.
:::

### Find exact matches

Sometimes you need exact matches rather than full-text search. Use the `.keyword` field for case-sensitive exact matching:

```esql
FROM cooking_blog
| WHERE category.keyword == "Breakfast"  # Exact match (case-sensitive)
| KEEP title, category, rating
| SORT rating DESC
| LIMIT 1000
```

This is fundamentally different from full-text search - it's a binary yes/no filter that doesn't affect relevance scoring.

## Step 4: Search precision control

Now that you understand basic searching, explore how to control the precision of your text matches.

### Require all search terms (`AND` logic)

By default, searches with match use OR logic between terms. To require ALL terms to match, use the function syntax with the `operator` parameter to specify AND logic:

```esql
FROM cooking_blog
| WHERE match(description, "fluffy pancakes", {"operator": "AND"})
| LIMIT 1000
```

This stricter search returns *zero hits* on our sample data, as no document contains both "fluffy" and "pancakes" in the description.

:::{note}
The `MATCH` function with AND logic doesn't require terms to be adjacent or in order. It only requires that all terms appear somewhere in the field. Use `MATCH_PHRASE` to [search for exact phrases](#search-for-exact-phrases).
:::

### Set a minimum number of terms to match

Sometimes requiring all terms is too strict, but the default OR behavior is too lenient. You can specify a minimum number of terms that must match:

```esql
FROM cooking_blog
| WHERE match(title, "fluffy pancakes breakfast", {"minimum_should_match": 2})
| LIMIT 1000
```

This query searches the title field to match at least 2 of the 3 terms: "fluffy", "pancakes", or "breakfast".

### Search for exact phrases

When you need to find documents containing an exact sequence of words, use the `MATCH_PHRASE` function:

```esql
FROM cooking_blog
| WHERE MATCH_PHRASE(description, "rich and creamy")
| KEEP title, description
| LIMIT 1000
```

This query only matches documents where the words "rich and creamy" appear exactly in that order in the description field.

## Step 5: Multi-field search and query strings

Once you're comfortable with basic search precision, learn how to search across multiple fields and use query string syntax for complex patterns.

### Use query string for complex patterns

The `QSTR` function enables powerful search patterns using a compact query language. It's ideal for when you need wildcards, fuzzy matching, and boolean logic in a single expression:

```esql
FROM cooking_blog
| WHERE QSTR("description: (fluffy AND pancak*) OR (creamy -vegan)")
| KEEP title, description
| LIMIT 1000
```

Query string syntax lets you:
- Use boolean operators: `AND`, `OR`, `-` (NOT)
- Apply wildcards: `pancak*` matches "pancake" and "pancakes"
- Enable fuzzy matching: `pancake~1` for typo tolerance
- Group terms: `(thai AND curry) OR pasta`
- Search exact phrases: `"fluffy pancakes"`
- Search across fields: `QSTR("(title:pancake OR description:pancake) AND creamy")`

### Search across multiple fields

When users enter a search query, they often don't know (or care) whether their search terms appear in a specific field. You can search across multiple fields simultaneously:

```esql
FROM cooking_blog
| WHERE title:"vegetarian curry" OR description:"vegetarian curry" OR tags:"vegetarian curry"
| LIMIT 1000
```

This query searches for "vegetarian curry" across the title, description, and tags fields. Each field is treated with equal importance.

### Weight different fields

In many cases, matches in certain fields (like the title) might be more relevant than others. You can adjust the importance of each field using boost scoring:

```esql
FROM cooking_blog METADATA _score
| WHERE MATCH(title, "vegetarian curry", {"boost": 2.0})  # Title matches are twice as important
    OR MATCH(description, "vegetarian curry")
    OR MATCH(tags, "vegetarian curry")
| KEEP title, description, tags, _score
| SORT _score DESC
| LIMIT 1000
```

## Step 6: Filtering and exact matching

Filtering allows you to narrow down your search results based on exact criteria. Unlike full-text searches, filters are binary (yes/no) and do not affect the relevance score. Filters execute faster than queries because excluded results don't need to be scored.

### Basic filtering by category

```esql
FROM cooking_blog
| WHERE category.keyword == "Breakfast"  # Exact match using keyword field
| KEEP title, author, rating, tags
| SORT rating DESC
| LIMIT 1000
```

### Date range filtering

Often users want to find content published within a specific time frame:

```esql
FROM cooking_blog
| WHERE date >= "2023-05-01" AND date <= "2023-05-31"  # Inclusive date range filter
| KEEP title, author, date, rating
| LIMIT 1000
```

### Numerical range filtering

Filter by ratings or other numerical values:

```esql
FROM cooking_blog
| WHERE rating >= 4.5  # Only highly-rated recipes
| KEEP title, author, rating, tags
| SORT rating DESC
| LIMIT 1000
```

### Exact author matching

Find recipes by a specific author:

```esql
FROM cooking_blog
| WHERE author.keyword == "Maria Rodriguez"  # Exact match on author
| KEEP title, author, rating, tags
| SORT rating DESC
| LIMIT 1000
```

## Step 7: Complex search solutions

Real-world search often requires combining multiple types of criteria. This section shows how to build sophisticated search experiences by combining the techniques you've learned.

### Combine filters with full-text search

Mix filters, full-text search, and custom scoring in a single query:

```esql
FROM cooking_blog METADATA _score
| WHERE rating >= 4.5  # Numerical filter
    AND NOT category.keyword == "Dessert"  # Exclusion filter
    AND (title:"curry spicy" OR description:"curry spicy")  # Full-text search in multiple fields
| SORT _score DESC
| KEEP title, author, rating, tags, description
| LIMIT 1000
```

### Advanced relevance scoring

For complex relevance scoring with combined criteria, you can use the `EVAL` command to calculate custom scores:

```esql
FROM cooking_blog METADATA _score
| WHERE NOT category.keyword == "Dessert"
| EVAL tags_concat = MV_CONCAT(tags.keyword, ",")  # Convert multi-value field to string
| WHERE tags_concat LIKE "*vegetarian*" AND rating >= 4.5  # Wildcard pattern matching
| WHERE MATCH(title, "curry spicy", {"boost": 2.0}) OR MATCH(description, "curry spicy")
| EVAL category_boost = CASE(category.keyword == "Main Course", 1.0, 0.0)  # Conditional boost
| EVAL date_boost = CASE(DATE_DIFF("month", date, NOW()) <= 1, 0.5, 0.0)  # Boost recent content
| EVAL custom_score = _score + category_boost + date_boost  # Combine scores
| WHERE custom_score > 0  # Filter based on custom score
| SORT custom_score DESC
| LIMIT 1000
```

## Step 8: Semantic search and hybrid search

### Index semantic content

{{es}} allows you to semantically search for documents based on the meaning of the text, rather than just the presence of specific keywords. This is useful when you want to find documents that are conceptually similar to a given query, even if they don't contain the exact search terms.

ES|QL supports semantic search when your mappings include fields of the [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) type. This example mapping update adds a new field called `semantic_description` with the type `semantic_text`:

```console
PUT /cooking_blog/_mapping
{
  "properties": {
    "semantic_description": {
      "type": "semantic_text"
    }
  }
}
```

Next, index a document with content into the new field:

```console
POST /cooking_blog/_doc
{
  "title": "Mediterranean Quinoa Bowl",
  "semantic_description": "A protein-rich bowl with quinoa, chickpeas, fresh vegetables, and herbs. This nutritious Mediterranean-inspired dish is easy to prepare and perfect for a quick, healthy dinner.",
  "author": "Jamie Oliver",
  "date": "2023-06-01",
  "category": "Main Course",
  "tags": ["vegetarian", "healthy", "mediterranean", "quinoa"],
  "rating": 4.7
}
```

### Perform semantic search

Once the document has been processed by the underlying model running on the inference endpoint, you can perform semantic searches. Use the [`MATCH` function](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match) (or the ["`:`" operator](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator) shorthand) on `semantic_text` fields to perform semantic queries:

```esql
FROM cooking_blog METADATA _score
| WHERE semantic_description:"I need plant-based meals that aren't difficult to make"
| SORT _score DESC
| LIMIT 5
```

When you use `MATCH` or "`:`" on a `semantic_text` field, {{esql}} automatically performs a semantic search rather than a lexical search. This means the query finds documents based on semantic similarity, not just keyword matching.

:::{tip}
If you'd like to test out the semantic search workflow against a large dataset, follow the [semantic-search-tutorial](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md).
:::

### Perform hybrid search

```{applies_to}
stack: preview 9.2
serverless: preview
```

Hybrid search combines different search strategies (like lexical and semantic search) to leverage the strengths of each approach. Use [`FORK`](/reference/query-languages/esql/commands/fork.md) and [`FUSE`](/reference/query-languages/esql/commands/fuse.md) to execute different search strategies in parallel, then merge and score the combined results.

`FUSE` supports different merging algorithms, namely Reciprocal Rank Fusion (RRF) and linear combination.

::::{tab-set}

:::{tab-item} RRF (default)

RRF is a ranking algorithm that combines rankings from multiple search strategies without needing to tune weights. It's effective when you want to blend different search approaches without manual score tuning.

```esql
FROM cooking_blog METADATA _id, _index, _score
| FORK (
    WHERE title:"vegetarian curry" <1>
    | SORT _score DESC
    | LIMIT 5
) (
    WHERE semantic_description:"easy vegetarian curry recipes" <2>
    | SORT _score DESC
    | LIMIT 5
)
| FUSE <3>
| KEEP title, description, rating, _score
| SORT _score DESC
| LIMIT 5
```

1. Lexical search on the title field
2. Semantic search on the semantic_description field
3. Merge results using RRF algorithm (default)
:::

:::{tab-item} Linear combination

Linear combination allows you to specify explicit weights for each search strategy. This gives you fine-grained control over how much each strategy contributes to the final score.

```esql
FROM cooking_blog METADATA _id, _index, _score
| FORK (
    WHERE title:"vegetarian curry" <1>
    | SORT _score DESC
    | LIMIT 5
) (
    WHERE semantic_description:"easy vegetarian curry recipes" <2>
    | SORT _score DESC
    | LIMIT 5
)
| FUSE LINEAR WITH { "weights": { "fork1": 0.7, "fork2": 0.3 } } <3>
| KEEP title, description, rating, _score
| SORT _score DESC
| LIMIT 5
```

1. Lexical search on the title field (70% weight)
2. Semantic search on the semantic_description field (30% weight)
3. Merge results using linear combination with explicit weights
:::

::::

## Step 9: Advanced AI-powered search

```{applies_to}
stack: preview 9.2
serverless: preview
```

{{esql}} provides commands for AI-powered search enhancements and text generation tasks. You will need to set up an inference endpoint with appropriate models to follow along with these examples.

### Semantic reranking with `RERANK`

The [`RERANK` command](/reference/query-languages/esql/commands/rerank.md) re-scores search results using inference models for improved relevance. This is particularly useful for improving the ranking of initial search results by applying more sophisticated semantic understanding.

:::{tip}
`RERANK` requires an inference endpoint configured for the `rerank` task. Refer to the [setup instructions](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-rerank.md#ml-nlp-rerank-deploy).
:::

```esql
FROM cooking_blog METADATA _score
| WHERE description:"vegetarian recipes" <1>
| SORT _score DESC
| LIMIT 100 <2>
| RERANK "healthy quick meals" ON description <3>
| LIMIT 5 <4>
| KEEP title, description, _score
```

1. Perform initial lexical search
2. Limit to top 100 results for reranking
3. Re-score using the default reranking model ([Elastic Rerank](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-rerank.md))
4. Return top 5 results after reranking

You can configure `RERANK` to use a specific reranking model. See the [`RERANK` command documentation](/reference/query-languages/esql/commands/rerank.md) for configuration options.


### Text generation with `COMPLETION`

:::{tip}
COMPLETION requires a configured inference endpoint with an LLM. Refer to the [Inference API documentation](docs-content://explore-analyze/elastic-inference/inference-api.md) for setup instructions.
:::

The [`COMPLETION` command](/reference/query-languages/esql/commands/completion.md) sends prompts to a Large Language Model (LLM) for text generation tasks like question answering, summarization, or translation.

```esql
FROM cooking_blog
| WHERE category.keyword == "Dessert"
| LIMIT 3
| EVAL prompt = CONCAT("Summarize this recipe: ", title, " - ", description)
| COMPLETION summary = prompt WITH { "inference_id": "my_llm_endpoint" }
| KEEP title, summary
```

This enables you to:
- Generate summaries of search results
- Answer questions about document content
- Translate or transform text using LLMs
- Create dynamic content based on your data

### Extract relevant snippets with `TOP_SNIPPETS`

```{applies_to}
stack: preview 9.3
serverless: preview
```

The [`TOP_SNIPPETS` function](/reference/query-languages/esql/functions-operators/search-functions.md#esql-top_snippets) is like the [`CHUNK` function](/reference/query-languages/esql/functions-operators/string-functions.md#esql-chunk) with additional relevance ranking capabilities. `TOP_SNIPPETS` ranks chunks by relevance to your query and returns only the top matches.

This is very useful for context engineering with LLMs: instead of sending entire field values, you send only the most relevant portions. This reduces token costs, helps you stay within context limits, and avoids the ["lost in the middle"](https://arxiv.org/abs/2307.03172) problem where important information is overlooked in large blocks of text.

#### Basic snippet extraction

Here's the basic syntax for extracting top snippets from a text field:

```esql
FROM cooking_blog
| WHERE description:"vegetarian curry"
| EVAL snippets = TOP_SNIPPETS(description, "vegetarian curry")
| KEEP title, snippets
| LIMIT 5
```

This query searches for "vegetarian curry" and extracts the top matching snippets from the `description` field.

::::{dropdown} Example response
:icon: code

```text
                    title                     |                                                                                                                snippets
----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Spicy Thai Green Curry: A Vegetarian Adventure|Dive into the flavors of Thailand with this vibrant green curry. Packed with vegetables and aromatic herbs, this dish is both healthy and satisfying. Don't worry about the heat - you can easily adjust the spice level to your liking.
```

Notice how `TOP_SNIPPETS` extracted the full description as the most relevant snippet for the query "vegetarian curry".
::::

#### Control snippet size and count

You can fine-tune the snippet extraction using named parameters:

```esql
FROM cooking_blog
| WHERE description:"healthy quick meals"
| EVAL snippets = TOP_SNIPPETS(description, "healthy quick meals", {"num_snippets": 3, "num_words": 25})
| KEEP title, snippets
| LIMIT 5
```

The parameters control:
- `num_snippets`: Maximum number of matching snippets to return (useful when a query matches multiple parts of a document)
- `num_words`: Maximum number of words per snippet (helps control token usage for LLM inference). This parameter automatically configures sentence-based chunking; other custom chunking configurations are not currently supported.

::::{dropdown} Example response
:icon: code

```text
                    title                     |                                                                               snippets
----------------------------------------------+----------------------------------------------------------------------------------------------------------------------------------------------------------------------
Spicy Thai Green Curry: A Vegetarian Adventure|Dive into the flavors of Thailand with this vibrant green curry. Packed with vegetables and aromatic herbs, this dish is both healthy and satisfying.
Vegan Chocolate Avocado Mousse                |Discover the magic of avocado in this rich, vegan chocolate mousse. Creamy, indulgent, and secretly healthy, it's the perfect guilt-free dessert for chocolate lovers.
```

Notice how the snippets are now limited to approximately 25 words each, compared to the basic example which returned the full description. The `num_words` parameter helps you control snippet length for more precise LLM context.

Because `TOP_SNIPPETS` uses sentence-based chunking, it prioritizes breaking at the nearest sentence boundary rather than cutting off mid-sentence, which may result in a slightly lower word count than requested.
::::

#### Combine with `COMPLETION` for efficient LLM context

The real power of `TOP_SNIPPETS` emerges when combined with `COMPLETION`. Instead of sending entire documents to an LLM, send only the most relevant snippets:

```esql
FROM cooking_blog METADATA _score
| WHERE semantic_description:"vegetarian recipes" <1>
| SORT _score DESC
| LIMIT 10 <2>
| EVAL snippets = TOP_SNIPPETS(semantic_description, "vegetarian recipes", {"num_snippets": 3, "num_words": 30}) <3>
| EVAL prompt = CONCAT("Based on these recipe snippets, suggest quick meal ideas: ", snippets) <4>
| COMPLETION answer = prompt WITH {"inference_id": "my_llm_endpoint"} <5>
| KEEP title, answer
```

1. Perform initial semantic search
2. Limit to top 10 most relevant documents
3. Extract the 3 most relevant snippets (max 30 words each) from each document
4. Build a prompt using the extracted snippets
5. Send the snippet-based prompt to the LLM

This approach significantly reduces token costs compared to sending full field values, while maintaining high-quality context for the LLM.

#### Combine with `RERANK` for precision

You can also use `TOP_SNIPPETS` with the `RERANK` command to rerank based on extracted snippets rather than entire field values:

```esql
FROM cooking_blog METADATA _score
| WHERE description:"healthy meals"
| SORT _score DESC
| LIMIT 100
| EVAL snippets = TOP_SNIPPETS(description, "quick healthy vegetarian", {"num_snippets": 3, "num_words": 25})
| RERANK "quick healthy vegetarian" ON snippets <1>
| LIMIT 5
| KEEP title, description, snippets, _score
```

1. Rerank based on the extracted snippets instead of the full description field

This can improve reranking precision for models that work better with focused text rather than long documents.

:::{tip}
To learn more about `TOP_SNIPPETS` refer to this [blog post](https://www.elastic.co/search-labs/blog/llm-chunking-snippet-extraction). You can also learn about the equivalent `chunk_rescorer` parameter in the [`text_similarity_reranker` retriever](/reference/elasticsearch/rest-apis/retrievers/text-similarity-reranker-retriever.md).
:::

### Vector search with KNN, similarity functions and TEXT_EMBEDDING

:::{note}
This subsection is for advanced users who want explicit control over vector search. For most use cases, the `semantic_text` field type covered in [Step 8](#step-8-semantic-search-and-hybrid-search) is recommended as it handles embeddings automatically.
:::

The [`KNN` function](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#esql-knn) finds the k nearest vectors to a query vector through approximate search on indexed `dense_vector` fields.

#### `KNN` with pre-computed vectors

First, add a `dense_vector` field and index a document with a pre-computed vector. This example uses a toy example for readability: the vector has only 3 dimensions.

```console
PUT /cooking_blog/_mapping
{
  "properties": {
    "recipe_vector": {
      "type": "dense_vector",
      "dims": 3
    }
  }
}

POST /cooking_blog/_doc/6
{
  "title": "Quick Vegan Stir-Fry",
  "description": "A fast and healthy vegan stir-fry with tofu and vegetables",
  "recipe_vector": [0.5, 0.8, 0.3],
  "rating": 4.6
}
```

Then search using a query vector:

```esql
FROM cooking_blog METADATA _score
| WHERE KNN(recipe_vector, [0.5, 0.8, 0.3])
| SORT _score DESC
| LIMIT 5
```

#### `KNN` with `TEXT_EMBEDDING`

```{applies_to}
stack: preview 9.3
serverless: preview
```

The [`TEXT_EMBEDDING` function](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#esql-text_embedding)  generates dense vector embeddings from text input at query time using an inference model.

:::{tip}
You'll need to set up an inference endpoint with an embedding model to use this feature. Refer to the [Inference API documentation](docs-content://explore-analyze/elastic-inference/inference-api.md) for setup instructions.
:::

```esql
FROM cooking_blog METADATA _score
| WHERE KNN(recipe_vector, TEXT_EMBEDDING("vegan recipe", "my_embedding_model"))
| SORT _score DESC
| LIMIT 5
```

This approach gives you full control over vector embeddings, unlike `semantic_text` which handles embeddings automatically.

:::{note}
The vectors in your index must be generated by the same embedding model you specify in `TEXT_EMBEDDING`, otherwise the similarity calculations will be meaningless and you won't get relevant results.
:::

#### Exact vector search with similarity functions

```{applies_to}
stack: preview 9.3
serverless: ga
```

While `KNN` performs approximate nearest neighbor search, {{esql}} also provides [vector similarity functions](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#vector-similarity-functions) for exact vector search. These functions calculate similarity over all the query vectors, guaranteeing accurate results at the cost of slower performance.

:::{tip}
**When to use exact search instead of KNN**

Use [vector similarity functions](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#vector-similarity-functions) when:
- Accuracy is more important than speed.
- Your dataset is small enough for exhaustive search, or you are applying restrictive filters. 10,000 documents is a good rule of thumb for using exact search, but results depend on your vector [element type](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization) and use of [quantization](/reference/elasticsearch/mapping-reference/dense-vector.md#dense-vector-quantization).
- You want to provide custom scoring using vector similarity.

To learn more, read this blog: [how to choose between exact and approximate kNN search in {{es}}](https://www.elastic.co/search-labs/blog/knn-exact-vs-approximate-search).
:::

**Example: Cosine similarity with threshold filtering**

```esql
FROM cooking_blog
| EVAL similarity = V_COSINE(recipe_vector, TEXT_EMBEDDING("vegan recipe", "my_embedding_model"))
| WHERE similarity > 0.8
| SORT similarity DESC
| LIMIT 10
```

This query:
1. Calculates exact cosine similarity between each document's vector and the query vector (calculated via TEXT_EMBEDDING) using the [V_COSINE function](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#esql-v_cosine)
2. Filters results to only include results with vector similarity above 0.8
3. Sorts by similarity (highest first)

**Example: Combining exact vector search with other filters**

```esql
FROM cooking_blog
| WHERE category.keyword == "Main Course" AND rating >= 4.5
| EVAL similarity = V_COSINE(recipe_vector, TEXT_EMBEDDING("vegan recipe", "my_embedding_model"))
| EVAL combined_score = similarity + (rating / 5.0)
| SORT combined_score DESC
| LIMIT 10
```

This advanced query combines:
1. Exact filters (category) and range filters (rating)
2. Exact vector similarity using the [V_COSINE function](/reference/query-languages/esql/functions-operators/dense-vector-functions.md#esql-v_dot_product)
3. Custom scoring that blends vector similarity with rating

## Learn more

### Documentation

This tutorial introduced search, filtering, and AI-powered capabilities in {{esql}}. Here are some resources once you're ready to dive deeper:

- [Search with {{esql}}](docs-content://solutions/search/esql-for-search.md): Learn about all search capabilities in {{esql}}.
- [Semantic search](docs-content://solutions/search/semantic-search.md): Understand your various options for semantic search in Elasticsearch.
  - [The `semantic_text` workflow](docs-content://solutions/search/semantic-search.md#_semantic_text_workflow): Learn how to use the `semantic_text` field type for semantic search. This is the recommended approach for most users looking to perform semantic search in {{es}}, because it abstracts away the complexity of setting up inference endpoints and models.
- [Inference API](docs-content://explore-analyze/elastic-inference/inference-api.md): Set up inference endpoints for the `RERANK`, `COMPLETION`, and `TEXT_EMBEDDING` commands.

### Related blog posts

- [{{esql}}, you know for Search](https://www.elastic.co/search-labs/blog/esql-introducing-scoring-semantic-search): Introducing scoring and semantic search
- [Introducing full text filtering in {{esql}}](https://www.elastic.co/search-labs/blog/filtering-in-esql-full-text-search-match-qstr): Overview of {{esql}}'s text filtering capabilities
