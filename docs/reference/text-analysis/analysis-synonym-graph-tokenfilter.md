---
navigation_title: "Synonym graph"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-synonym-graph-tokenfilter.html
---

# Synonym graph token filter [analysis-synonym-graph-tokenfilter]


The `synonym_graph` token filter allows to easily handle [synonyms](docs-content://solutions/search/full-text/search-with-synonyms.md), including multi-word synonyms correctly during the analysis process.

In order to properly handle multi-word synonyms this token filter creates a [graph token stream](docs-content://manage-data/data-store/text-analysis/token-graphs.md) during processing. For more information on this topic and its various complexities, please read the [Lucene’s TokenStreams are actually graphs](http://blog.mikemccandless.com/2012/04/lucenes-tokenstreams-are-actually.md) blog post.

::::{note}
:name: synonym-graph-index-note

This token filter is designed to be used as part of a search analyzer only. If you want to apply synonyms during indexing please use the standard [synonym token filter](/reference/text-analysis/analysis-synonym-tokenfilter.md).

::::



## Define synonyms sets [analysis-synonym-graph-define-synonyms]

Synonyms in a synonyms set are defined using **synonym rules**. Each synonym rule contains words that are synonyms.

You can use two formats to define synonym rules: Solr and WordNet.


### Solr format [_solr_format_2]

This format uses two different definitions:

* Equivalent synonyms: Define groups of words that are equivalent. Words are separated by commas. Example:

    ```text
    ipod, i-pod, i pod
    computer, pc, laptop
    ```

* Explicit synonyms: Matches a group of words to other words. Words on the left hand side of the rule definition are expanded into all the possibilities described on the right hand side. Example:

    ```text
    personal computer => pc
    sea biscuit, sea biscit => seabiscuit
    ```



### WordNet format [_wordnet_format_2]

[WordNet](https://wordnet.princeton.edu/) defines synonyms sets spanning multiple lines. Each line contains the following information:

* Synonyms set numeric identifier
* Ordinal of the synonym in the synonyms set
* Synonym word
* Word type identifier: Noun (n), verb (v), adjective (a) or adverb (b).
* Depth of the word in the synonym net

The following example defines a synonym set for the words "come", "advance" and "approach":

```text
s(100000002,1,'come',v,1,0).
s(100000002,2,'advance',v,1,0).
s(100000002,3,'approach',v,1,0).""";
```


## Configure synonyms sets [analysis-synonym-graph-configure-sets]

Synonyms can be configured using the [synonyms API](docs-content://solutions/search/full-text/search-with-synonyms.md#synonyms-store-synonyms-api), a [synonyms file](docs-content://solutions/search/full-text/search-with-synonyms.md#synonyms-store-synonyms-file), or directly [inlined](docs-content://solutions/search/full-text/search-with-synonyms.md#synonyms-store-synonyms-inline) in the token filter configuration. See [store your synonyms set](docs-content://solutions/search/full-text/search-with-synonyms.md#synonyms-store-synonyms) for more details on each option.

Use `synonyms_set` configuration option to provide a synonym set created via Synonyms Management APIs:

```JSON
  "filter": {
    "synonyms_filter": {
      "type": "synonym_graph",
      "synonyms_set": "my-synonym-set",
      "updateable": true
    }
  }
```

::::{warning}
Synonyms sets must exist before they can be added to indices. If an index is created referencing a nonexistent synonyms set, the index will remain in a partially created and inoperable state. The only way to recover from this scenario is to ensure the synonyms set exists then either delete and re-create the index, or close and re-open the index.

::::


Use `synonyms_path` to provide a synonym file :

```JSON
  "filter": {
    "synonyms_filter": {
      "type": "synonym_graph",
      "synonyms_path": "analysis/synonym-set.txt"
    }
  }
```

The above configures a `synonym` filter, with a path of `analysis/synonym-set.txt` (relative to the `config` location).

Use `synonyms` to define inline synonyms:

```JSON
  "filter": {
    "synonyms_filter": {
      "type": "synonym_graph",
      "synonyms": ["pc => personal computer", "computer, pc, laptop"]
    }
  }
```

Additional settings are:

* `updateable` (defaults to `false`). If `true` allows [reloading](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-reload-search-analyzers) search analyzers to pick up changes to synonym files. Only to be used for search analyzers.
* `expand` (defaults to `true`). Expands definitions for equivalent synonym rules. See [expand equivalent synonyms](#synonym-graph-tokenizer-expand-equivalent-synonyms).
* `lenient` (defaults to the value of the `updateable` setting). If `true` ignores errors while parsing the synonym rules. It is important to note that only those synonym rules which cannot get parsed are ignored. See [synonyms and stop token filters](#synonym-graph-tokenizer-stop-token-filter) for an example of `lenient` behaviour for invalid synonym rules.


### `expand` equivalent synonym rules [synonym-graph-tokenizer-expand-equivalent-synonyms]

The `expand` parameter controls whether to expand equivalent synonym rules. Consider a synonym defined like:

`foo, bar, baz`

Using `expand: true`, the synonym rule would be expanded into:

```
foo => foo
foo => bar
foo => baz
bar => foo
bar => bar
bar => baz
baz => foo
baz => bar
baz => baz
```

When `expand` is set to `false`, the synonym rule is not expanded and the first synonym is treated as the canonical representation. The synonym would be equivalent to:

```
foo => foo
bar => foo
baz => foo
```

The `expand` parameter does not affect explicit synonym rules, like `foo, bar => baz`.


### `tokenizer` and `ignore_case` are deprecated [synonym-graph-tokenizer-ignore_case-deprecated]

The `tokenizer` parameter controls the tokenizers that will be used to tokenize the synonym, this parameter is for backwards compatibility for indices that created before 6.0. The `ignore_case` parameter works with `tokenizer` parameter only.


## Configure analyzers with synonym graph token filters [analysis-synonym-graph-analizers-configure]

To apply synonyms, you will need to include a synonym graph token filter into an analyzer:

```JSON
      "analyzer": {
        "my_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["stemmer", "synonym_graph"]
        }
      }
```


### Token filters ordering [analysis-synonym-graph-token-order]

Order is important for your token filters. Text will be processed first through filters preceding the synonym filter before being processed by the synonym filter.

{{es}} will also use the token filters preceding the synonym filter in a tokenizer chain to parse the entries in a synonym file or synonym set. In the above example, the synonyms graph token filter is placed after a stemmer. The stemmer will also be applied to the synonym entries.

Because entries in the synonym map cannot have stacked positions, some token filters may cause issues here. Token filters that produce multiple versions of a token may choose which version of the token to emit when parsing synonyms. For example, `asciifolding` will only produce the folded version of the token. Others, like `multiplexer`, `word_delimiter_graph` or `ngram` will throw an error.

If you need to build analyzers that include both multi-token filters and synonym filters, consider using the [multiplexer](/reference/text-analysis/analysis-multiplexer-tokenfilter.md) filter, with the multi-token filters in one branch and the synonym filter in the other.


### Synonyms and `stop` token filters [synonym-graph-tokenizer-stop-token-filter]

Synonyms and [stop token filters](/reference/text-analysis/analysis-stop-tokenfilter.md) interact with each other in the following ways:


#### Stop token filter **before** synonym token filter [_stop_token_filter_before_synonym_token_filter_2]

Stop words will be removed from the synonym rule definition. This can can cause errors on the synonym rule.

::::{warning}
If `lenient` is set to `false`, invalid synonym rules can cause errors when applying analyzer changes. For reloadable analyzers, this prevents reloading and applying changes. You must correct errors in the synonym rules and reload the analyzer.

When `lenient` is set to `false`, an index with invalid synonym rules cannot be reopened, making it inoperable when:

* A node containing the index starts
* The index is opened from a closed state
* A node restart occurs (which reopens the node assigned shards)

::::


For **explicit synonym rules** like `foo, bar => baz` with a stop filter that removes `bar`:

* If `lenient` is set to `false`, an error will be raised as `bar` would be removed from the left hand side of the synonym rule.
* If `lenient` is set to `true`, the rule `foo => baz` will be added and `bar => baz` will be ignored.

If the stop filter removed `baz` instead:

* If `lenient` is set to `false`, an error will be raised as `baz` would be removed from the right hand side of the synonym rule.
* If `lenient` is set to `true`, the synonym will have no effect as the target word is removed.

For **equivalent synonym rules** like `foo, bar, baz` and `expand: true, with a stop filter that removes `bar`:

* If `lenient` is set to `false`, an error will be raised as `bar` would be removed from the synonym rule.
* If `lenient` is set to `true`, the synonyms added would be equivalent to the following synonym rules, which do not contain the removed word:

```
foo => foo
foo => baz
baz => foo
baz => baz
```


#### Stop token filter **after** synonym token filter [_stop_token_filter_after_synonym_token_filter_2]

The stop filter will remove the terms from the resulting synonym expansion.

For example, a synonym rule like `foo, bar => baz` and a stop filter that removes `baz` will get no matches for `foo` or `bar`, as both would get expanded to `baz` which is removed by the stop filter.

If the stop filter removed `foo` instead, then searching for `foo` would get expanded to `baz`, which is not removed by the stop filter thus potentially providing matches for `baz`.

