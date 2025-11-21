---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
applies_to:
  stack: all
  serverless: all
---

# Highlighting [highlighting]

Highlighters enable you to retrieve the best-matching highlighted snippets from one or more fields in your search results so you can show users where the query matches are. When you request highlights, the response contains an additional `highlight` element for each search hit that includes the highlighted fields and the highlighted fragments.

::::{note}
Highlighters don’t reflect the boolean logic of a query when extracting terms to highlight. Thus, for some complex boolean queries (e.g nested boolean queries, queries using `minimum_should_match` etc.), parts of documents may be highlighted that don’t correspond to query matches.
::::


Highlighting requires the actual content of a field. If the field is not stored (the mapping does not set `store` to `true`), the actual `_source` is loaded and the relevant field is extracted from `_source`.

For example, to get highlights for the `content` field in each search hit using the default highlighter, include a `highlight` object in the request body that specifies the `content` field:

```console
GET /_search
{
  "query": {
    "match": { "content": "kimchy" }
  },
  "highlight": {
    "fields": {
      "content": {}
    }
  }
}
```
% TEST[setup:my_index]

## Highlighting types [highlighting-types]

{{es}} supports three highlighters: `unified`, `plain`, and `fvh` (fast vector highlighter) for `text` and `keyword` fields and the `semantic` highlighter for `semantic_text` fields. You can specify the highlighter `type` you want to use for each field or rely on the field type’s default highlighter.


### Unified highlighter [unified-highlighter]

The `unified` highlighter uses the Lucene Unified Highlighter. This highlighter breaks the text into sentences and uses the BM25 algorithm to score individual sentences as if they were documents in the corpus. It also supports accurate phrase and multi-term (fuzzy, prefix, regex) highlighting. The `unified` highlighter can combine matches from multiple fields into one result (see `matched_fields`).

This is the default highlighter for all `text` and `keyword` fields.


### Semantic Highlighter [semantic-highlighter]

The `semantic` highlighter is specifically designed for use with the [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) field. It identifies and extracts the most relevant fragments from the field based on semantic similarity between the query and each fragment.

By default, [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) fields use the semantic highlighter.


### Plain highlighter [plain-highlighter]

The `plain` highlighter uses the standard Lucene highlighter. It attempts to reflect the query matching logic in terms of understanding word importance and any word positioning criteria in phrase queries.

::::{note}
The `plain` highlighter works best for highlighting simple query matches in a single field. To accurately reflect query logic, it creates a tiny in-memory index and re-runs the original query criteria through Lucene’s query execution planner to get access to low-level match information for the current document. This is repeated for every field and every document that needs to be highlighted. If you want to highlight a lot of fields in a lot of documents with complex queries, we recommend using the `unified` highlighter on `postings` or `term_vector` fields.
::::

### Fast vector highlighter [fast-vector-highlighter]

The `fvh` highlighter uses the Lucene Fast Vector highlighter. This highlighter can be used on fields with `term_vector` set to `with_positions_offsets` in the mapping. The fast vector highlighter:

* Can be customized with a [`boundary_scanner`](highlighting-settings.md#boundary-scanner).
* Requires setting `term_vector` to `with_positions_offsets` which increases the size of the index
* Can combine matches from multiple fields into one result. See `matched_fields`
* Can assign different weights to matches at different positions allowing for things like phrase matches being sorted above term matches when highlighting a Boosting Query that boosts phrase matches over term matches

::::{note}
The `fvh` highlighter does not support span queries. If you need support for span queries, try an alternative highlighter, such as the `unified` highlighter.
::::


## Offsets strategy [offsets-strategy]

To create meaningful search snippets from the terms being queried, the highlighter needs to know the start and end character offsets of each word in the original text. These offsets can be obtained from:

* The postings list. If `index_options` is set to `offsets` in the mapping, the `unified` highlighter uses this information to highlight documents without re-analyzing the text. It re-runs the original query directly on the postings and extracts the matching offsets from the index, limiting the collection to the highlighted documents. This is important if you have large fields because it doesn’t require reanalyzing the text to be highlighted. It also requires less disk space than using `term_vectors`.
* Term vectors. If `term_vector` information is provided by setting `term_vector` to `with_positions_offsets` in the mapping, the `unified` highlighter automatically uses the `term_vector` to highlight the field. It’s fast especially for large fields (> `1MB`) and for highlighting multi-term queries like `prefix` or `wildcard` because it can access the dictionary of terms for each document. The `fvh` highlighter always uses term vectors.
* Plain highlighting. This mode is used by the `unified` when there is no other alternative. It creates a tiny in-memory index and re-runs the original query criteria through Lucene’s query execution planner to get access to low-level match information on the current document. This is repeated for every field and every document that needs highlighting. The `plain` highlighter always uses plain highlighting.

::::{note}
Plain highlighting for large texts may require substantial amount of time and memory. To protect against this, the maximum number of text characters that will be analyzed has been limited to 1000000. This default limit can be changed for a particular index with the index setting [`index.highlight.max_analyzed_offset`](/reference/elasticsearch/index-settings/index-modules.md#index-max-analyzed-offset).
::::