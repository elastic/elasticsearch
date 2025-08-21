---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-min-should-match-context.html
products:
  - id: painless
---

# Minimum should match context [painless-min-should-match-context]

Use a Painless script to specify the [minimum](/reference/query-languages/query-dsl/query-dsl-terms-set-query.md) number of terms that a specified field needs to match with for a document to be part of the query results.

**Variables**

`params` (`Map`, read-only)
:   User-defined parameters passed in as part of the query.

`params['num_terms']` (`int`, read-only)
:   The number of terms specified to match with.

`doc` (`Map`, read-only)
:   Contains the fields of the current document where each field is a `List` of values.

**Return**

`int`
:   The minimum number of terms required to match the current document.

**API**

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

**Example**

To run this example, first follow the steps in [context examples](/reference/scripting-languages/painless/painless-context-examples.md).

Imagine that you want to find seats to performances by your favorite actors. You have a list of favorite actors in mind, and you want to find performances where the cast includes at least a certain number of them.

To achieve this result, use a `terms_set` query with `minimum_should_match_script`. To make the query request more configurable, you can define `min_actors_to_see` as a script parameter.

To ensure that the parameter `min_actors_to_see` doesn’t exceed the number of favorite actors, you can use `num_terms` to get the number of actors in the list and `Math.min` to get the lesser of the two.

```painless
Math.min(params['num_terms'], params['min_actors_to_see'])
```

The following request finds seats to performances with at least two of the three specified actors.

```console
GET seats/_search
{
  "query": {
    "terms_set": {
      "actors": {
        "terms": [
          "smith",
          "earns",
          "black"
        ],
        "minimum_should_match_script": {
          "source": "Math.min(params['num_terms'], params['min_actors_to_see'])",
          "params": {
            "min_actors_to_see": 2
          }
        }
      }
    }
  }
}
```

