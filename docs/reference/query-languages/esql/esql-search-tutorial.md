---
applies_to:
  stack: preview 9.0, ga 9.1
  serverless: ga
navigation_title: Search and filter with ES|QL
---

# Search and filter with {{esql}}

This is a hands-on introduction to the basics of full-text search and semantic search, using {{esql}}.

In this scenario, we're implementing search for a cooking blog. The blog contains recipes with various attributes including textual content, categorical data, and numerical ratings.

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
      "type": "text",
      "fields": {
        "keyword": {
          "type": "keyword"
        }
      }
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

1. `analyzer`: Used for text analysis. If you don't specify it, the `standard` analyzer is used by default for `text` fields. It’s included here for demonstration purposes. To know more about analyzers, refer to [Anatomy of an analyzer](docs-content://manage-data/data-store/text-analysis/anatomy-of-an-analyzer.md).
2. `ignore_above`: Prevents indexing values longer than 256 characters in the `keyword` field. This is the default value and it’s included here for demonstration purposes. It helps to save disk space and avoid potential issues with Lucene’s term byte-length limit. For more information, refer [ignore_above parameter](/reference/elasticsearch/mapping-reference/ignore-above.md).
3. `description`: A field declared with both `text` and `keyword` [data types](/reference/elasticsearch/mapping-reference/field-data-types.md). Such fields are called  [Multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md). This enables both full-text search and exact matching/filtering on the same field. If you use [dynamic mapping](docs-content://manage-data/data-store/mapping/dynamic-field-mapping.md), these multi-fields will be created automatically. Other fields in the mapping like `author`, `category`, `tags` are also declared as multi-fields.

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
2. Compact syntax using the [match operator `:`](/reference/query-languages/esql/functions-operators/operators.md#esql-match-operator): `field:"search terms"`

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

### Require all search terms (AND logic)

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

## Step 5: Semantic search and hybrid search

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

Once the document has been processed by the underlying model running on the inference endpoint, you can perform semantic searches. Here's an example natural language query against the `semantic_description` field:

```esql
FROM cooking_blog
| WHERE semantic_description:"What are some easy to prepare but nutritious plant-based meals?"
| LIMIT 5 
```

:::{tip}
If you'd like to test out the semantic search workflow against a large dataset, follow the [semantic-search-tutorial](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md).
:::

### Perform hybrid search

You can combine full-text and semantic queries. In this example we combine full-text and semantic search with custom weights:

```esql
FROM cooking_blog METADATA _score
| WHERE match(semantic_description, "easy to prepare vegetarian meals", { "boost": 0.75 })
    OR match(tags, "vegetarian", { "boost": 0.25 })
| SORT _score DESC
| LIMIT 5
```

This query searches the `semantic_description` field for documents that are semantically similar to "easy to prepare vegetarian meals" with a higher weight, while also matching the `tags` field for "vegetarian" with a lower weight. The results are sorted by relevance score.

Learn how to combine these with complex criteria in [Step 8](#step-8-complex-search-solutions).

## Step 6: Advanced search features

Once you're comfortable with basic search precision, use the following advanced features for powerful search capabilities.

### Use query string for complex patterns

The `QSTR` function enables powerful search patterns using a compact query language. It's ideal for when you need wildcards, fuzzy matching, and boolean logic in a single expression:

```esql
FROM cooking_blog
| WHERE QSTR(description, "fluffy AND pancak* OR (creamy -vegan)")
| KEEP title, description
| LIMIT 1000
```

Query string syntax lets you:
- Use boolean operators: `AND`, `OR`, `-` (NOT)
- Apply wildcards: `pancak*` matches "pancake" and "pancakes"
- Enable fuzzy matching: `pancake~1` for typo tolerance
- Group terms: `(thai AND curry) OR pasta`
- Search exact phrases: `"fluffy pancakes"`
- Search across fields: `QSTR("title,description", "pancake OR (creamy AND rich)")`

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
| WHERE match(title, "vegetarian curry", {"boost": 2.0})  # Title matches are twice as important
    OR match(description, "vegetarian curry")
    OR match(tags, "vegetarian curry")
| KEEP title, description, tags, _score
| SORT _score DESC
| LIMIT 1000
```

## Step 7: Filtering and exact matching

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

## Step 8: Complex search solutions

Real-world search often requires combining multiple types of criteria. This section shows how to build sophisticated search experiences.

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
| WHERE match(title, "curry spicy", {"boost": 2.0}) OR match(description, "curry spicy")
| EVAL category_boost = CASE(category.keyword == "Main Course", 1.0, 0.0)  # Conditional boost
| EVAL date_boost = CASE(DATE_DIFF("month", date, NOW()) <= 1, 0.5, 0.0)  # Boost recent content
| EVAL custom_score = _score + category_boost + date_boost  # Combine scores
| WHERE custom_score > 0  # Filter based on custom score
| SORT custom_score DESC
| LIMIT 1000
```

## Learn more

### Documentation

This tutorial introduced the basics of search and filtering in {{esql}}. Building a real-world search experience requires understanding many more advanced concepts and techniques. Here are some resources once you're ready to dive deeper:

- [Search with {{esql}}](docs-content://solutions/search/esql-for-search.md): Learn about all the search capabilities in ES|QL, refer to Using ES|QL for search. {{esql}}.
- [{{esql}} search functions](/reference/query-languages/esql/functions-operators/search-functions.md): Explore the full list of search functions available in {{esql}}.
- [Semantic search](docs-content://solutions/search/semantic-search.md): Understand your various options for semantic search in Elasticsearch.
  - [The `semantic_text` workflow](docs-content://solutions/search/semantic-search.md#_semantic_text_workflow): Learn how to use the `semantic_text` field type for semantic search. This is the recommended approach for most users looking to perform semantic search in {{es}}, because it abstracts away the complexity of setting up inference endpoints and models.

### Related blog posts

- [{{esql}}, you know for Search](https://www.elastic.co/search-labs/blog/esql-introducing-scoring-semantic-search): Introducing scoring and semantic search
- [Introducing full text filtering in {{esql}}](https://www.elastic.co/search-labs/blog/filtering-in-esql-full-text-search-match-qstr): Overview of {{esql}}'s text filtering capabilities
