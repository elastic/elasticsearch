---
navigation_title: Get started
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/full-text-filter-tutorial.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Get started with Query DSL search and filters [full-text-filter-tutorial]

This is a hands-on introduction to the basics of full-text search with {{es}}, also known as *lexical search*, using the `_search` API and Query DSL.

In this tutorial, you'll implement a search function for a cooking blog and learn how to filter data to narrow down search results based on exact criteria.
The blog contains recipes with various attributes including textual content, categorical data, and numerical ratings.
The goal is to create search queries to:

* Find recipes based on preferred or avoided ingredients
* Explore dishes that meet specific dietary needs
* Find top-rated recipes in specific categories
* Find the latest recipes from favorite authors

To achieve these goals, you'll use different {{es}} queries to perform full-text search, apply filters, and combine multiple search criteria.

::::{tip}
The code examples are in [Console](docs-content://explore-analyze/query-filter/tools/console.md) syntax by default.
You can [convert into other programming languages](docs-content://explore-analyze/query-filter/tools/console.md#import-export-console-requests) in the Console UI.
::::

## Requirements [full-text-filter-tutorial-requirements]

You can follow these steps in any type of {{es}} deployment.
To see all deployment options, refer to [Choosing your deployment type](docs-content://deploy-manage/deploy.md#choosing-your-deployment-type).
To get started quickly, set up a [single-node local cluster in Docker](docs-content://deploy-manage/deploy/self-managed/local-development-installation-quickstart.md).

## Create an index [full-text-filter-tutorial-create-index]

Create the `cooking_blog` index to get started:

```console
PUT /cooking_blog
```
% TESTSETUP

Next, define the mappings for the index:

```console
PUT /cooking_blog/_mapping
{
  "properties": {
    "title": {
      "type": "text",
      "analyzer": "standard", <1>
      "fields": { 
        "keyword": {
          "type": "keyword",
          "ignore_above": 256 <2>
        }
      }
    },
    "description": { <3>
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
% TEST

1. `analyzer`: Used for text analysis. If you don't specify it, the `standard` analyzer is used by default for `text` fields. It's included here for demonstration purposes. To know more about analyzers, refer [Anatomy of an analyzer](https://docs-v3-preview.elastic.dev/elastic/docs-content/tree/main/manage-data/data-store/text-analysis/anatomy-of-an-analyzer).
2. `ignore_above`: Prevents indexing values longer than 256 characters in the `keyword` field. This is the default value and it's included here for demonstration purposes. It helps to save disk space and avoid potential issues with Lucene's term byte-length limit. For more information, refer to [ignore_above parameter](/reference/elasticsearch/mapping-reference/ignore-above.md).
3. `description`: A field declared with both `text` and `keyword` [data types](/reference/elasticsearch/mapping-reference/field-data-types.md). Such fields are called [multi-fields](/reference/elasticsearch/mapping-reference/multi-fields.md). This enables both full-text search and exact matching/filtering on the same field. If you use [dynamic mapping](docs-content://manage-data/data-store/mapping/dynamic-field-mapping.md), these multi-fields will be created automatically. A few other fields in the mapping like `author`, `category`, `tags` are also declared as multi-fields.



::::{tip}
Full-text search is powered by [text analysis](docs-content://solutions/search/full-text/text-analysis-during-search.md). Text analysis normalizes and standardizes text data so it can be efficiently stored in an inverted index and searched in near real-time. Analysis happens at both [index and search time](docs-content://manage-data/data-store/text-analysis/index-search-analysis.md). This tutorial won't cover analysis in detail, but it's important to understand how text is processed to create effective search queries.

::::

## Add sample blog posts to your index [full-text-filter-tutorial-index-data]

Next, index some example blog posts using the [bulk API]({{es-apis}}operation/operation-bulk). Note that `text` fields are analyzed and multi-fields are generated at index time.

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
% TEST[continued]

## Perform basic full-text searches [full-text-filter-tutorial-match-query]

Full-text search involves executing text-based queries across one or more document fields. These queries calculate a relevance score for each matching document, based on how closely the document's content aligns with the search terms. {{es}} offers various query types, each with its own method for matching text and [relevance scoring](docs-content://explore-analyze/query-filter/languages/querydsl.md#relevance-scores).

### Use `match` query [_match_query]

The [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md) query is the standard query for full-text search. The query text will be analyzed according to the analyzer configuration specified on each field (or at query time).

First, search the `description` field for "fluffy pancakes":

```console
GET /cooking_blog/_search
{
  "query": {
    "match": {
      "description": {
        "query": "fluffy pancakes" <1>
      }
    }
  }
}
```
% TEST[continued]

1. By default, the `match` query uses `OR` logic between the resulting tokens. This means it will match documents that contain either "fluffy" or "pancakes", or both, in the description field.

At search time, {{es}} defaults to the analyzer defined in the field mapping. This example uses the `standard` analyzer. Using a different analyzer at search time is an [advanced use case](docs-content://manage-data/data-store/text-analysis/index-search-analysis.md#different-analyzers).

::::{dropdown} Example response
```console-result
{
  "took": 0,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": { <1>
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.8378843, <2>
    "hits": [
      {
        "_index": "cooking_blog",
        "_id": "1",
        "_score": 1.8378843, <3>
        "_source": {
          "title": "Perfect Pancakes: A Fluffy Breakfast Delight", <4>
          "description": "Learn the secrets to making the fluffiest pancakes, so amazing you won't believe your tastebuds. This recipe uses buttermilk and a special folding technique to create light, airy pancakes that are perfect for lazy Sunday mornings.", <5>
          "author": "Maria Rodriguez",
          "date": "2023-05-01",
          "category": "Breakfast",
          "tags": [
            "pancakes",
            "breakfast",
            "easy recipes"
          ],
          "rating": 4.8
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 0/"took": "$body.took"/]
% TESTRESPONSE[s/"total": 1/"total": $body._shards.total/]
% TESTRESPONSE[s/"successful": 1/"successful": $body._shards.successful/]
% TESTRESPONSE[s/"value": 1/"value": $body.hits.total.value/]
% TESTRESPONSE[s/"max_score": 1.8378843/"max_score": $body.hits.max_score/]
% TESTRESPONSE[s/"_score": 1.8378843/"_score": $body.hits.hits.0._score/]

1. `hits`: Contains the total number of matching documents and their relation to the total.
2. `max_score`: The highest relevance score among all matching documents. In this example, there is only one matching document.
3. `_score`: The relevance score for a specific document, indicating how well it matches the query. Higher scores indicate better matches. In this example the `max_score` is the same as the `_score`, as there is only one matching document.
4. The title contains both "Fluffy" and "Pancakes", matching the search terms exactly.
5. The description includes "fluffiest" and "pancakes", further contributing to the document's relevance due to the analysis process.

::::

### Include all terms match in a query [_require_all_terms_in_a_match_query]

Specify the `and` operator to include both terms in the `description` field.
This stricter search returns *zero hits* on the sample data because no documents contain both "fluffy" and "pancakes" in the description.

```console
GET /cooking_blog/_search
{
  "query": {
    "match": {
      "description": {
        "query": "fluffy pancakes",
        "operator": "and"
      }
    }
  }
}
```
% TEST[continued]

::::{dropdown} Example response
```console-result
{
  "took": 0,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 0,
      "relation": "eq"
    },
    "max_score": null,
    "hits": []
  }
}
```
% TESTRESPONSE[s/"took": 0/"took": "$body.took"/]
::::



### Specify a minimum number of terms to match [_specify_a_minimum_number_of_terms_to_match]

Use the [`minimum_should_match`](/reference/query-languages/query-dsl/query-dsl-minimum-should-match.md) parameter to specify the minimum number of terms a document should have to be included in the search results.

Search the title field to match at least 2 of the 3 terms: "fluffy", "pancakes", or "breakfast". This is useful for improving relevance while allowing some flexibility.

```console
GET /cooking_blog/_search
{
  "query": {
    "match": {
      "title": {
        "query": "fluffy pancakes breakfast",
        "minimum_should_match": 2
      }
    }
  }
}
```
% TEST[continued]

## Search across multiple fields [full-text-filter-tutorial-multi-match]

When you enter a search query, you might not know whether the search terms appear in a specific field.
A [`multi_match`](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md) query enables you to search across multiple fields simultaneously.

Start with a basic `multi_match` query:

```console
GET /cooking_blog/_search
{
  "query": {
    "multi_match": {
      "query": "vegetarian curry",
      "fields": ["title", "description", "tags"]
    }
  }
}
```
% TEST[continued]

This query searches for "vegetarian curry" across the title, description, and tags fields. Each field is treated with equal importance.

However, in many cases, matches in certain fields (like the title) might be more relevant than others.
You can adjust the importance of each field using field boosting:

```console
GET /cooking_blog/_search
{
  "query": {
    "multi_match": {
      "query": "vegetarian curry",
      "fields": ["title^3", "description^2", "tags"] <1>
    }
  }
}
```
% TEST[continued]

1. The `^` syntax applies a boost to specific fields:

  * `title^3`: The title field is 3 times more important than an unboosted field.
  * `description^2`: The description is 2 times more important.
  * `tags`: No boost applied (equivalent to `^1`).

  These boosts help tune relevance, prioritizing matches in the title over the description and matches in the description over tags.

Learn more about fields and per-field boosting in the [`multi_match` query](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md) reference.

::::{dropdown} Example response
```console-result
{
  "took": 0,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 7.546015,
    "hits": [
      {
        "_index": "cooking_blog",
        "_id": "2",
        "_score": 7.546015,
        "_source": {
          "title": "Spicy Thai Green Curry: A Vegetarian Adventure", <1>
          "description": "Dive into the flavors of Thailand with this vibrant green curry. Packed with vegetables and aromatic herbs, this dish is both healthy and satisfying. Don't worry about the heat - you can easily adjust the spice level to your liking.", <2>
          "author": "Liam Chen",
          "date": "2023-05-05",
          "category": "Main Course",
          "tags": [
            "thai",
            "vegetarian",
            "curry",
            "spicy"
          ], <3>
          "rating": 4.6
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 0/"took": "$body.took"/]
% TESTRESPONSE[s/"_score": 7.546015/"_score": $body.hits.hits.0._score/]
% TESTRESPONSE[s/"max_score": 7.546015/"max_score": $body.hits.max_score/]

1. The title contains "Vegetarian" and "Curry", which matches the search terms. The title field has the highest boost (^3), contributing significantly to this document's relevance score.
2. The description contains "curry" and related terms like "vegetables", further increasing the document's relevance.
3. The tags include both "vegetarian" and "curry", providing an exact match for the search terms, albeit with no boost.

This result demonstrates how the `multi_match` query with field boosts helps you find relevant recipes across multiple fields.
Even though the exact phrase "vegetarian curry" doesn't appear in any single field, the combination of matches across fields produces a highly relevant result.

::::

::::{tip}
The `multi_match` query is often recommended over a single `match` query for most text search use cases because it provides more flexibility and better matches user expectations.
::::

## Filter and find exact matches [full-text-filter-tutorial-filtering]

[Filtering](docs-content://explore-analyze/query-filter/languages/querydsl.md#filter-context) enables you to narrow down your search results based on exact criteria. Unlike full-text searches, filters are binary (yes or no) and do not affect the relevance score. Filters run faster than queries because excluded results don't need to be scored.

The following [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md) query will return blog posts only in the "Breakfast" category.

```console
GET /cooking_blog/_search
{
  "query": {
    "bool": {
      "filter": [
        { "term": { "category.keyword": "Breakfast" } }  <1>
      ]
    }
  }
}
```
% TEST[continued]

1. Note the use of `category.keyword` here. This refers to the [`keyword`](/reference/elasticsearch/mapping-reference/keyword.md) multi-field of the `category` field, ensuring an exact, case-sensitive match.


::::{tip}
The `.keyword` suffix accesses the unanalyzed version of a field, enabling exact, case-sensitive matching. This works in two scenarios:

1. When using dynamic mapping for text fields. {{es}} automatically creates a `.keyword` sub-field.
2. When text fields are explicitly mapped with a `.keyword` sub-field. For example, you explicitly mapped the `category` field when you defined the mappings for the `cooking_blog` index.
::::

### Search within a date range [full-text-filter-tutorial-range-query]

To find content published within a specific time frame, use a [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md) query.
It finds documents that fall within numeric or date ranges.

```console
GET /cooking_blog/_search
{
  "query": {
    "range": {
      "date": {
        "gte": "2023-05-01", <1>
        "lte": "2023-05-31" <2>
      }
    }
  }
}
```
% TEST[continued]

1. `gte`: Greater than or equal to May 1, 2023.
2. `lte`: Less than or equal to May 31, 2023.

### Find exact matches [full-text-filter-tutorial-term-query]

Sometimes you might want to search for exact terms to eliminate ambiguity in the search results. A [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md) query searches for an exact term in a field without analyzing it. Exact, case-sensitive matches on specific terms are often referred to as "keyword" searches.

In the following example, you'll search for the author "Maria Rodriguez" in the `author.keyword` field.

```console
GET /cooking_blog/_search
{
  "query": {
    "term": {
      "author.keyword": "Maria Rodriguez" <1>
    }
  }
}
```
% TEST[continued]

1. The `term` query has zero flexibility. For example, if the `author.keyword` contains words `maria` or `maria rodriguez`, the query will have zero hits due to case sensitivity.

::::{tip}
Avoid using the `term` query for `text` fields because they are transformed by the analysis process.
::::

## Combine multiple search criteria [full-text-filter-tutorial-complex-bool]

You can use a [`bool`](/reference/query-languages/query-dsl/query-dsl-bool-query.md) query to combine multiple query clauses and create sophisticated searches.
For example, create a query that addresses the following requirements:

* Must be a vegetarian recipe
* Should contain "curry" or "spicy" in the title or description
* Should be a main course
* Must not be a dessert
* Must have a rating of at least 4.5
* Should prefer recipes published in the last month

```console
GET /cooking_blog/_search
{
  "query": {
    "bool": {
      "must": [
        { "term": { "tags": "vegetarian" } },
        {
          "range": {
            "rating": {
              "gte": 4.5
            }
          }
        }
      ],
      "should": [
        {
          "term": {
            "category.keyword": "Main Course"
          }
        },
        {
          "multi_match": {
            "query": "curry spicy",
            "fields": [
              "title^2",
              "description"
            ]
          }
        },
        {
          "range": {
            "date": {
              "gte": "now-1M/d"
            }
          }
        }
      ],
      "must_not": [ <1>
        {
          "term": {
            "category.keyword": "Dessert"
          }
        }
      ]
    }
  }
}
```
% TEST[continued]

1. `must_not`: Excludes documents that match the specified criteria. This is a powerful tool for filtering out unwanted results.

::::{dropdown} Example response
```console-result
{
  "took": 1,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 7.444513,
    "hits": [
      {
        "_index": "cooking_blog",
        "_id": "2",
        "_score": 7.444513,
        "_source": {
          "title": "Spicy Thai Green Curry: A Vegetarian Adventure", <1>
          "description": "Dive into the flavors of Thailand with this vibrant green curry. Packed with vegetables and aromatic herbs, this dish is both healthy and satisfying. Don't worry about the heat - you can easily adjust the spice level to your liking.", <2>
          "author": "Liam Chen",
          "date": "2023-05-05",
          "category": "Main Course", <3>
          "tags": [ <4>
            "thai",
            "vegetarian", <5>
            "curry",
            "spicy"
          ],
          "rating": 4.6 <6>
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/"took": 1/"took": "$body.took"/]

1. The title contains "Spicy" and "Curry", matching the should condition. With the default [best_fields](/reference/query-languages/query-dsl/query-dsl-multi-match-query.md#type-best-fields) behavior, this field contributes most to the relevance score.
2. While the description also contains matching terms, only the best matching field's score is used by default.
3. If the recipe was published within the last month, it would satisfy the recency preference.
4. The "Main Course" category satisfies another `should` condition.
5. The "vegetarian" tag satisfies a `must` condition, while "curry" and "spicy" tags align with the `should` preferences.
6. The rating of 4.6 meets the minimum rating requirement of 4.5.

::::

## Learn more [full-text-filter-tutorial-learn-more]

This tutorial introduced the basics of full-text search and filtering in {{es}}.
Building a real-world search experience requires understanding many more advanced concepts and techniques.
The following resources will help you dive deeper:

* [Full-text search](docs-content://solutions/search/full-text.md): Learn about the core components of full-text search in {{es}}.
* [{{es}} basics — Search and analyze data](docs-content://explore-analyze/query-filter.md): Understand all your options for searching and analyzing data in {{es}}.
* [Text analysis](docs-content://solutions/search/full-text/text-analysis-during-search.md): Understand how text is processed for full-text search.
* [Search your data](docs-content://solutions/search.md): Learn about more advanced search techniques using the `_search` API, including semantic search.
