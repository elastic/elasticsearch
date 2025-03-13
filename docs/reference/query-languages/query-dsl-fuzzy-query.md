---
navigation_title: "Fuzzy"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html
---

# Fuzzy query [query-dsl-fuzzy-query]


Returns documents that contain terms similar to the search term, as measured by a [Levenshtein edit distance](https://en.wikipedia.org/wiki/Levenshtein_distance).

An edit distance is the number of one-character changes needed to turn one term into another. These changes can include:

* Changing a character (**b**ox → **f**ox)
* Removing a character (**b**lack → lack)
* Inserting a character (sic → sic**k**)
* Transposing two adjacent characters (**ac**t → **ca**t)

To find similar terms, the `fuzzy` query creates a set of all possible variations, or expansions, of the search term within a specified edit distance. The query then returns exact matches for each expansion.

## Example requests [fuzzy-query-ex-request]

### Simple example [fuzzy-query-ex-simple]

```console
GET /_search
{
  "query": {
    "fuzzy": {
      "user.id": {
        "value": "ki"
      }
    }
  }
}
```


### Example using advanced parameters [fuzzy-query-ex-advanced]

```console
GET /_search
{
  "query": {
    "fuzzy": {
      "user.id": {
        "value": "ki",
        "fuzziness": "AUTO",
        "max_expansions": 50,
        "prefix_length": 0,
        "transpositions": true,
        "rewrite": "constant_score_blended"
      }
    }
  }
}
```



## Top-level parameters for `fuzzy` [fuzzy-query-top-level-params]

`<field>`
:   (Required, object) Field you wish to search.


## Parameters for `<field>` [fuzzy-query-field-params]

`value`
:   (Required, string) Term you wish to find in the provided `<field>`.

`fuzziness`
:   (Optional, string) Maximum edit distance allowed for matching. See [Fuzziness](/reference/elasticsearch/rest-apis/common-options.md#fuzziness) for valid values and more information.

`max_expansions`
:   (Optional, integer) Maximum number of variations created. Defaults to `50`.

::::{warning}
Avoid using a high value in the `max_expansions` parameter, especially if the `prefix_length` parameter value is `0`. High values in the `max_expansions` parameter can cause poor performance due to the high number of variations examined.
::::



`prefix_length`
:   (Optional, integer) Number of beginning characters left unchanged when creating expansions. Defaults to `0`.

`transpositions`
:   (Optional, Boolean) Indicates whether edits include transpositions of two adjacent characters (ab → ba). Defaults to `true`.

`rewrite`
:   (Optional, string) Method used to rewrite the query. For valid values and more information, see the [`rewrite` parameter](/reference/query-languages/query-dsl-multi-term-rewrite.md).


## Notes [fuzzy-query-notes]

Fuzzy queries will not be executed if [`search.allow_expensive_queries`](/reference/query-languages/querydsl.md#query-dsl-allow-expensive-queries) is set to false.
