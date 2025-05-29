---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters.html
navigation_title: Suggesters
applies_to:
  stack: all
---
# Suggester examples [search-suggesters]

The suggest feature suggests similar looking terms based on a provided text by using a suggester. The suggest request part is defined alongside the query part in a `_search` request. If the query part is left out, only suggestions are returned.

::::{note}
For the most up-to-date details, refer to [Search APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-search).
::::

Several suggestions can be specified per request. Each suggestion is identified with an arbitrary name. In the example below two suggestions are requested. Both `my-suggest-1` and `my-suggest-2` suggestions use the `term` suggester, but have a different `text`.

```console
POST _search
{
  "suggest": {
    "my-suggest-1" : {
      "text" : "tring out Elasticsearch",
      "term" : {
        "field" : "message"
      }
    },
    "my-suggest-2" : {
      "text" : "kmichy",
      "term" : {
        "field" : "user.id"
      }
    }
  }
}
```
% TEST[setup:messages]
% TEST[s/^/PUT my-index-000001/_mapping\n{"properties":{"user":{"properties":{"id":{"type":"keyword"}}}}}\n/]

The following suggest response example includes the suggestion response for `my-suggest-1` and `my-suggest-2`. Each suggestion part contains entries. Each entry is effectively a token from the suggest text and contains the suggestion entry text, the original start offset and length in the suggest text and if found an arbitrary number of options.

```console-result
{
  "_shards": ...
  "hits": ...
  "took": 2,
  "timed_out": false,
  "suggest": {
    "my-suggest-1": [ {
      "text": "tring",
      "offset": 0,
      "length": 5,
      "options": [ {"text": "trying", "score": 0.8, "freq": 1 } ]
    }, {
      "text": "out",
      "offset": 6,
      "length": 3,
      "options": []
    }, {
      "text": "elasticsearch",
      "offset": 10,
      "length": 13,
      "options": []
    } ],
    "my-suggest-2": ...
  }
}
```
% TESTRESPONSE[s/"_shards": \.\.\./"_shards": "$body._shards",/]
% TESTRESPONSE[s/"hits": …​/"hits": "$body.hits",/]
% TESTRESPONSE[s/"took": 2,/"took": "$body.took",/]
% TESTRESPONSE[s/"my-suggest-2": \.\.\./"my-suggest-2": "$body.suggest.my-suggest-2"/]

Each options array contains an option object that includes the suggested text, its document frequency and score compared to the suggest entry text. The meaning of the score depends on the used suggester. The term suggester's score is based on the edit distance.


## Global suggest text [global-suggest] 

To avoid repetition of the suggest text, it is possible to define a global text. In the following example the suggest text is defined globally and applies to the `my-suggest-1` and `my-suggest-2` suggestions.

```console
POST _search
{
  "suggest": {
    "text" : "tring out Elasticsearch",
    "my-suggest-1" : {
      "term" : {
        "field" : "message"
      }
    },
    "my-suggest-2" : {
       "term" : {
        "field" : "user"
       }
    }
  }
}
```

The suggest text can in the above example also be specified as suggestion specific option. The suggest text specified on suggestion level override the suggest text on the global level.

## Term suggester [term-suggester]

The `term` suggester suggests terms based on edit distance. The provided suggest text is analyzed before terms are suggested. The suggested terms are provided per analyzed suggest text token. The `term` suggester doesn't take the query into account that is part of request.

$$$_common_suggest_options$$$
Common suggest options include:

`text`
:   The suggest text. The suggest text is a required option that needs to be set globally or per suggestion.

`field`
:   The field to fetch the candidate suggestions from. This is a required option that either needs to be set globally or per suggestion.

`analyzer`
:   The analyzer to analyse the suggest text with. Defaults to the search analyzer of the suggest field.

`size`
:   The maximum corrections to be returned per suggest text token.

`sort`
:   Defines how suggestions should be sorted per suggest text term. Two possible values:

    * `score`:     Sort by score first, then document frequency and then the term itself.
    * `frequency`: Sort by document frequency first, then similarity score and then the term itself.

`suggest_mode`
:   The suggest mode controls what suggestions are included or controls for what suggest text terms, suggestions should be suggested. Three possible values can be specified:

    * `missing`:  Only provide suggestions for suggest text terms that are not in the index (default).
    * `popular`:  Only suggest suggestions that occur in more docs than the original suggest text term.
    * `always`:   Suggest any matching suggestions based on terms in the suggest text.

% TBD: If these are covered in the API reference, we can cover only the 'common' ones here
% ### Other term suggest options: [_other_term_suggest_options]
% 
% `max_edits`
% :   The maximum edit distance candidate suggestions can have in order to be considered as a suggestion. Can % only be a value between 1 and 2. Any other value results in a bad request error being thrown. Defaults to 2.
% 
% `prefix_length`
% :   The number of minimal prefix characters that must match in order be a candidate for suggestions. Defaults to 1. Increasing this number improves spellcheck performance. Usually misspellings don't occur in the beginning of terms.
% 
% `min_word_length`
% :   The minimum length a suggest text term must have in order to be included. Defaults to `4`.
% 
% `shard_size`
% :   Sets the maximum number of suggestions to be retrieved from each individual shard. During the reduce phase only the top N suggestions are returned based on the `size` option. Defaults to the `size` option. Setting this to a value higher than the `size` can be useful in order to get a more accurate document frequency for spelling corrections at the cost of performance. Due to the fact that terms are partitioned amongst shards, the shard level document frequencies of spelling corrections may not be precise. Increasing this will make these document frequencies more precise.
% 
% `max_inspections`
% :   A factor that is used to multiply with the `shard_size` in order to inspect more candidate spelling corrections on the shard level. Can improve accuracy at the cost of performance. Defaults to 5.
% 
% `min_doc_freq`
% :   The minimal threshold in number of documents a suggestion should appear in. This can be specified as an absolute number or as a relative percentage of number of documents. This can improve quality by only suggesting high frequency terms. Defaults to 0f and is not enabled. If a value higher than 1 is specified, then the number cannot be fractional. The shard level document frequencies are used for this option.
%
% `max_term_freq`
% :   The maximum threshold in number of documents in which a suggest text token can exist in order to be included. Can be a relative percentage number (e.g., 0.4) or an absolute number to represent document frequencies. If a value higher than 1 is specified, then fractional can not be specified. Defaults to 0.01f. This can be used to exclude high frequency terms — which are usually spelled correctly — from being spellchecked. This also improves the spellcheck performance. The shard level document frequencies are used for this option.
% 
% `string_distance`
% :   Which string distance implementation to use for comparing how similar suggested terms are. Five possible values can be specified:
% 
%     * `internal`: The default based on damerau_levenshtein but highly optimized for comparing string distance for terms inside the index.
%     * `damerau_levenshtein`: String distance algorithm based on Damerau-Levenshtein algorithm.
%     * `levenshtein`: String distance algorithm based on Levenshtein edit distance algorithm.
%     * `jaro_winkler`: String distance algorithm based on Jaro-Winkler algorithm.
%     * `ngram`: String distance algorithm based on character n-grams. -->

## Phrase suggester [phrase-suggester]

The `term` suggester provides a very convenient API to access word alternatives on a per token basis within a certain string distance. The API allows accessing each token in the stream individually while suggest-selection is left to the API consumer. Yet, often pre-selected suggestions are required in order to present to the end-user. The `phrase` suggester adds additional logic on top of the `term` suggester to select entire corrected phrases instead of individual tokens weighted based on `ngram-language` models. In practice this suggester will be able to make better decisions about which tokens to pick based on co-occurrence and frequencies.

In general the `phrase` suggester requires special mapping up front to work. The `phrase` suggester examples on this page need the following mapping to work. The `reverse` analyzer is used only in the last example.

```console
PUT test
{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "analysis": {
        "analyzer": {
          "trigram": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["lowercase","shingle"]
          },
          "reverse": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": ["lowercase","reverse"]
          }
        },
        "filter": {
          "shingle": {
            "type": "shingle",
            "min_shingle_size": 2,
            "max_shingle_size": 3
          }
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "fields": {
          "trigram": {
            "type": "text",
            "analyzer": "trigram"
          },
          "reverse": {
            "type": "text",
            "analyzer": "reverse"
          }
        }
      }
    }
  }
}
POST test/_doc?refresh=true
{"title": "noble warriors"}
POST test/_doc?refresh=true
{"title": "nobel prize"}
```
% TESTSETUP

Once you have the analyzers and mappings set up you can use the `phrase` suggester in the same spot you'd use the `term` suggester:

```console
POST test/_search
{
  "suggest": {
    "text": "noble prize",
    "simple_phrase": {
      "phrase": {
        "field": "title.trigram",
        "size": 1,
        "gram_size": 3,
        "direct_generator": [ {
          "field": "title.trigram",
          "suggest_mode": "always"
        } ],
        "highlight": {
          "pre_tag": "<em>",
          "post_tag": "</em>"
        }
      }
    }
  }
}
```

The response contains suggestions scored by the most likely spelling correction first. In this case we received the expected correction "nobel prize".

```console-result
{
  "_shards": ...
  "hits": ...
  "timed_out": false,
  "took": 3,
  "suggest": {
    "simple_phrase" : [
      {
        "text" : "noble prize",
        "offset" : 0,
        "length" : 11,
        "options" : [ {
          "text" : "nobel prize",
          "highlighted": "<em>nobel</em> prize",
          "score" : 0.48614594
        }]
      }
    ]
  }
}
```
% TESTRESPONSE[s/"_shards": …​/"_shards": "$body._shards",/]
% TESTRESPONSE[s/"hits": …​/"hits": "$body.hits",/]
% TESTRESPONSE[s/"took": 3,/"took": "$body.took",/]

$$$_basic_phrase_suggest_api_parameters$$$
Basic phrase suggest API parameters include:

`field`
:   The name of the field used to do n-gram lookups for the language model, the suggester will use this field to gain statistics to score corrections. This field is mandatory.

`gram_size`
:   Sets max size of the n-grams (shingles) in the `field`. If the field doesn't contain n-grams (shingles), this should be omitted or set to `1`. Note that Elasticsearch tries to detect the gram size based on the specified `field`. If the field uses a `shingle` filter, the `gram_size` is set to the `max_shingle_size` if not explicitly set.

`real_word_error_likelihood`
:   The likelihood of a term being misspelled even if the term exists in the dictionary. The default is `0.95`, meaning 5% of the real words are misspelled.

`confidence`
:   The confidence level defines a factor applied to the input phrases score which is used as a threshold for other suggest candidates. Only candidates that score higher than the threshold will be included in the result. For instance a confidence level of `1.0` will only return suggestions that score higher than the input phrase. If set to `0.0` the top N candidates are returned. The default is `1.0`.

`max_errors`
:   The maximum percentage of the terms considered to be misspellings in order to form a correction. This method accepts a float value in the range `[0..1)` as a fraction of the actual query terms or a number `>=1` as an absolute number of query terms. The default is set to `1.0`, meaning only corrections with at most one misspelled term are returned. Note that setting this too high can negatively impact performance. Low values like `1` or `2` are recommended; otherwise the time spend in suggest calls might exceed the time spend in query execution.

`separator`
:   The separator that is used to separate terms in the bigram field. If not set the whitespace character is used as a separator.

`size`
:   The number of candidates that are generated for each individual query term. Low numbers like `3` or `5` typically produce good results. Raising this can bring up terms with higher edit distances. The default is `5`.

`analyzer`
:   Sets the analyzer to analyze to suggest text with. Defaults to the search analyzer of the suggest field passed via `field`.

`shard_size`
:   Sets the maximum number of suggested terms to be retrieved from each individual shard. During the reduce phase, only the top N suggestions are returned based on the `size` option. Defaults to `5`.

`text`
:   Sets the text / query to provide suggestions for.

`highlight`
:   Sets up suggestion highlighting. If not provided then no `highlighted` field is returned. If provided must contain exactly `pre_tag` and `post_tag`, which are wrapped around the changed tokens. If multiple tokens in a row are changed the entire phrase of changed tokens is wrapped rather than each token.

`collate`
:   Checks each suggestion against the specified `query` to prune suggestions for which no matching docs exist in the index. The collate query for a suggestion is run only on the local shard from which the suggestion has been generated from. The `query` must be specified and it can be templated. Refer to [Search templates](docs-content://solutions/search/search-templates.md). The current suggestion is automatically made available as the `{{suggestion}}` variable, which should be used in your query. You can still specify your own template `params` — the `suggestion` value will be added to the variables you specify. Additionally, you can specify a `prune` to control if all phrase suggestions will be returned; when set to `true` the suggestions will have an additional option `collate_match`, which will be `true` if matching documents for the phrase was found, `false` otherwise. The default value for `prune` is `false`.

```console
POST test/_search
{
  "suggest": {
    "text" : "noble prize",
    "simple_phrase" : {
      "phrase" : {
        "field" :  "title.trigram",
        "size" :   1,
        "direct_generator" : [ {
          "field" :            "title.trigram",
          "suggest_mode" :     "always",
          "min_word_length" :  1
        } ],
        "collate": {
          "query": { <1>
            "source" : {
              "match": {
                "{{field_name}}" : "{{suggestion}}" <2>
              }
            }
          },
          "params": {"field_name" : "title"}, <3>
          "prune": true <4>
        }
      }
    }
  }
}
```

1. This query will be run once for every suggestion.
2. The `{{suggestion}}` variable will be replaced by the text of each suggestion.
3. An additional `field_name` variable has been specified in `params` and is used by the `match` query.
4. All suggestions will be returned with an extra `collate_match` option indicating whether the generated phrase matched any document.

### Smoothing models [_smoothing_models]

The `phrase` suggester supports multiple smoothing models to balance weight between infrequent grams (grams (shingles) are not existing in the index) and frequent grams (appear at least once in the index). The smoothing model can be selected by setting the `smoothing` parameter to one of the following options. Each smoothing model supports specific properties that can be configured.

`stupid_backoff`
:   A simple backoff model that backs off to lower order n-gram models if the higher order count is `0` and discounts the lower order n-gram model by a constant factor. The default `discount` is `0.4`. Stupid Backoff is the default model.

`laplace`
:   A smoothing model that uses an additive smoothing where a constant (typically `1.0` or smaller) is added to all counts to balance weights. The default `alpha` is `0.5`.

`linear_interpolation`
:   A smoothing model that takes the weighted mean of the unigrams, bigrams, and trigrams based on user supplied weights (lambdas). Linear Interpolation doesn't have any default values. All parameters (`trigram_lambda`, `bigram_lambda`, `unigram_lambda`) must be supplied.

```console
POST test/_search
{
  "suggest": {
    "text" : "obel prize",
    "simple_phrase" : {
      "phrase" : {
        "field" : "title.trigram",
        "size" : 1,
        "smoothing" : {
          "laplace" : {
            "alpha" : 0.7
          }
        }
      }
    }
  }
}
```


### Candidate generators [_candidate_generators]

The `phrase` suggester uses candidate generators to produce a list of possible terms per term in the given text. A single candidate generator is similar to a `term` suggester called for each individual term in the text. The output of the generators is subsequently scored in combination with the candidates from the other terms for suggestion candidates.

Currently only one type of candidate generator is supported, the `direct_generator`. The phrase suggest API accepts a list of generators under the key `direct_generator`; each of the generators in the list is called per term in the original text.


### Direct generators [_direct_generators]

The parameters that direct generators support include:

`field`
:   The field to fetch the candidate suggestions from. This is a required option that either needs to be set globally or per suggestion.

`size`
:   The maximum corrections to be returned per suggest text token.

`suggest_mode`
:   The suggest mode controls what suggestions are included on the suggestions generated on each shard. All values other than `always` can be thought of as an optimization to generate fewer suggestions to test on each shard and are not rechecked when combining the suggestions generated on each shard. Thus `missing` will generate suggestions for terms on shards that do not contain them even if other shards do contain them. Those should be filtered out using `confidence`. Three possible values can be specified:

    * `missing`: Only generate suggestions for terms that are not in the shard. This is the default.
    * `popular`: Only suggest terms that occur in more docs on the shard than the original term.
    * `always`: Suggest any matching suggestions based on terms in the suggest text.

`max_edits`
:   The maximum edit distance candidate suggestions can have in order to be considered as a suggestion. Can only be a value between 1 and 2. Any other value results in a bad request error being thrown. Defaults to 2.

`prefix_length`
:   The number of minimal prefix characters that must match in order be a candidate suggestions. Defaults to 1. Increasing this number improves spellcheck performance. Usually misspellings don't occur in the beginning of terms.

`min_word_length`
:   The minimum length a suggest text term must have in order to be included. Defaults to 4.

`max_inspections`
:   A factor that is used to multiply with the `shard_size` in order to inspect more candidate spelling corrections on the shard level. Can improve accuracy at the cost of performance. Defaults to 5.

`min_doc_freq`
:   The minimal threshold in number of documents a suggestion should appear in. This can be specified as an absolute number or as a relative percentage of number of documents. This can improve quality by only suggesting high frequency terms. Defaults to 0f and is not enabled. If a value higher than 1 is specified, then the number cannot be fractional. The shard level document frequencies are used for this option.

`max_term_freq`
:   The maximum threshold in number of documents in which a suggest text token can exist in order to be included. Can be a relative percentage number (e.g., 0.4) or an absolute number to represent document frequencies. If a value higher than 1 is specified, then fractional can not be specified. Defaults to 0.01f. This can be used to exclude high frequency terms — which are usually spelled correctly — from being spellchecked. This also improves the spellcheck performance. The shard level document frequencies are used for this option.

`pre_filter`
:   A filter (analyzer) that is applied to each of the tokens passed to this candidate generator. This filter is applied to the original token before candidates are generated.

`post_filter`
:   A filter (analyzer) that is applied to each of the generated tokens before they are passed to the actual phrase scorer.

The following example shows a `phrase` suggest call with two generators: the first one is using a field containing ordinary indexed terms, and the second one uses a field that uses terms indexed with a `reverse` filter (tokens are index in reverse order). This is used to overcome the limitation of the direct generators to require a constant prefix to provide high-performance suggestions. The `pre_filter` and `post_filter` options accept ordinary analyzer names.

```console
POST test/_search
{
  "suggest": {
    "text" : "obel prize",
    "simple_phrase" : {
      "phrase" : {
        "field" : "title.trigram",
        "size" : 1,
        "direct_generator" : [ {
          "field" : "title.trigram",
          "suggest_mode" : "always"
        }, {
          "field" : "title.reverse",
          "suggest_mode" : "always",
          "pre_filter" : "reverse",
          "post_filter" : "reverse"
        } ]
      }
    }
  }
}
```

`pre_filter` and `post_filter` can also be used to inject synonyms after candidates are generated. For instance for the query `captain usq` we might generate a candidate `usa` for the term `usq`, which is a synonym for `america`. This allows us to present `captain america` to the user if this phrase scores high enough.

## Completion suggester [completion-suggester]

The `completion` suggester provides auto-complete/search-as-you-type functionality. This is a navigational feature to guide users to relevant results as they are typing, improving search precision. It is not meant for spell correction or did-you-mean functionality like the `term` or `phrase` suggesters.

Ideally, auto-complete functionality should be as fast as a user types to provide instant feedback relevant to what a user has already typed in. Hence, `completion` suggester is optimized for speed. The suggester uses data structures that enable fast lookups, but are costly to build and are stored in-memory.

### Mapping [completion-suggester-mapping]

To use the [`completion` suggester](search-suggesters.md#completion-suggester), map the field from which you want to generate suggestions as type `completion`. This indexes the field values for fast completions.

```console
PUT music
{
  "mappings": {
    "properties": {
      "suggest": {
        "type": "completion"
      }
    }
  }
}
```

$$$_parameters_for_completion_fields_2$$$
The parameters that are accepted by `completion` fields include:

[`analyzer`](/reference/elasticsearch/mapping-reference/analyzer.md)
:   The index analyzer to use, defaults to `simple`.

[`search_analyzer`](/reference/elasticsearch/mapping-reference/search-analyzer.md)
:   The search analyzer to use, defaults to value of `analyzer`.

`preserve_separators`
:   Preserves the separators, defaults to `true`. If disabled, you could find a field starting with `Foo Fighters`, if you suggest for `foof`.

`preserve_position_increments`
:   Enables position increments, defaults to `true`. If disabled and using stopwords analyzer, you could get a field starting with `The Beatles`, if you suggest for `b`. **Note**: You could also achieve this by indexing two inputs, `Beatles` and `The Beatles`, no need to change a simple analyzer, if you are able to enrich your data.

`max_input_length`
:   Limits the length of a single input, defaults to `50` UTF-16 code points. This limit is only used at index time to reduce the total number of characters per input string in order to prevent massive inputs from bloating the underlying datastructure. Most use cases won't be influenced by the default value since prefix completions seldom grow beyond prefixes longer than a handful of characters.

### Indexing [indexing]

You index suggestions like any other field. A suggestion is made of an `input` and an optional `weight` attribute. An `input` is the expected text to be matched by a suggestion query and the `weight` determines how the suggestions will be scored. Indexing a suggestion is as follows:

```console
PUT music/_doc/1?refresh
{
  "suggest" : {
    "input": [ "Nevermind", "Nirvana" ],
    "weight" : 34
  }
}
```
% TEST

The supported parameters include:

`input`
:   The input to store, this can be an array of strings or just a string. This field is mandatory.

    ::::{note} 
    This value cannot contain the following UTF-16 control characters:

    * `\u0000` (null)
    * `\u001f` (information separator one)
    * `\u001e` (information separator two)

    ::::


`weight`
:   A positive integer or a string containing a positive integer, which defines a weight and allows you to rank your suggestions. This field is optional.

You can index multiple suggestions for a document as follows:

```console
PUT music/_doc/1?refresh
{
  "suggest": [
    {
      "input": "Nevermind",
      "weight": 10
    },
    {
      "input": "Nirvana",
      "weight": 3
    }
  ]
}
```
% TEST[continued]

You can use the following shorthand form. Note that you can not specify a weight with suggestion(s) in the shorthand form.

```console
PUT music/_doc/1?refresh
{
  "suggest" : [ "Nevermind", "Nirvana" ]
}
```
% TEST[continued]


### Querying [querying]

Suggesting works as usual, except that you have to specify the suggest type as `completion`. Suggestions are near real-time, which means new suggestions can be made visible by [refresh](https://www.elastic.co/docs/api/doc/elasticsearch/v8/operation/operation-indices-refresh) and documents once deleted are never shown. This request:

```console
POST music/_search?pretty
{
  "suggest": {
    "song-suggest": {
      "prefix": "nir",        <1>
      "completion": {         <2>
          "field": "suggest"  <3>
      }
    }
  }
}
```
% TEST[continued]

1. Prefix used to search for suggestions
2. Type of suggestions
3. Name of the field to search for suggestions in

It returns this response:

```console-result
{
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits": ...
  "took": 2,
  "timed_out": false,
  "suggest": {
    "song-suggest" : [ {
      "text" : "nir",
      "offset" : 0,
      "length" : 3,
      "options" : [ {
        "text" : "Nirvana",
        "_index": "music",
        "_id": "1",
        "_score": 1.0,
        "_source": {
          "suggest": ["Nevermind", "Nirvana"]
        }
      } ]
    } ]
  }
}
```
% TESTRESPONSE[s/"hits": …​/"hits": "$body.hits",/]
% TESTRESPONSE[s/"took": 2,/"took": "$body.took",/]

::::{important} 
`_source` metadata field must be enabled, which is the default behavior, to enable returning `_source` with suggestions.
::::

The configured weight for a suggestion is returned as `_score`. The `text` field uses the `input` of your indexed suggestion. Suggestions return the full document `_source` by default. The size of the `_source` can impact performance due to disk fetch and network transport overhead. To save some network overhead, filter out unnecessary fields from the `_source` using source filtering to minimize `_source` size. Note that the _suggest endpoint doesn't support source filtering but using suggest on the `_search` endpoint does:

```console
POST music/_search
{
  "_source": "suggest",     <1>
  "suggest": {
    "song-suggest": {
      "prefix": "nir",
      "completion": {
        "field": "suggest", <2>
        "size": 5           <3>
      }
    }
  }
}
```
% TEST[continued]

1. Filter the source to return only the `suggest` field
2. Name of the field to search for suggestions in
3. Number of suggestions to return


Which should look like:

```console-result
{
  "took": 6,
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
  },
  "suggest": {
    "song-suggest": [ {
        "text": "nir",
        "offset": 0,
        "length": 3,
        "options": [ {
            "text": "Nirvana",
            "_index": "music",
            "_id": "1",
            "_score": 1.0,
            "_source": {
              "suggest": [ "Nevermind", "Nirvana" ]
            }
          } ]
      } ]
  }
}
```
% TESTRESPONSE[s/"took": 6,/"took": $body.took,/]

The supported parameters for a basic completion suggester query include:

`field`
:   The name of the field on which to run the query (required).

`size`
:   The number of suggestions to return (defaults to `5`).

`skip_duplicates`
:   Whether duplicate suggestions should be filtered out (defaults to `false`).

::::{note} 
The completion suggester considers all documents in the index. See [Context suggester](search-suggesters.md#context-suggester) for an explanation of how to query a subset of documents instead.
::::


::::{note} 
In case of completion queries spanning more than one shard, the suggest is executed in two phases, where the last phase fetches the relevant documents from shards, implying executing completion requests against a single shard is more performant due to the document fetch overhead when the suggest spans multiple shards. To get best performance for completions, it is recommended to index completions into a single shard index. In case of high heap usage due to shard size, it is still recommended to break index into multiple shards instead of optimizing for completion performance.
::::

### Skip duplicate suggestions [skip_duplicates]

Queries can return duplicate suggestions coming from different documents. It is possible to modify this behavior by setting `skip_duplicates` to true. When set, this option filters out documents with duplicate suggestions from the result.

```console
POST music/_search?pretty
{
  "suggest": {
    "song-suggest": {
      "prefix": "nor",
      "completion": {
        "field": "suggest",
        "skip_duplicates": true
      }
    }
  }
}
```

::::{warning} 
When set to true, this option can slow down search because more suggestions need to be visited to find the top N.
::::

### Fuzzy queries [fuzzy]

The completion suggester also supports fuzzy queries — this means you can have a typo in your search and still get results back.

```console
POST music/_search?pretty
{
  "suggest": {
    "song-suggest": {
      "prefix": "nor",
      "completion": {
        "field": "suggest",
        "fuzzy": {
          "fuzziness": 2
        }
      }
    }
  }
}
```

Suggestions that share the longest prefix to the query `prefix` will be scored higher.

The fuzzy query can take specific fuzzy parameters. For example:

`fuzziness`
:   The fuzziness factor, defaults to `AUTO`. See  [Fuzziness](common-options.md#fuzziness) for allowed settings.

`transpositions`
:   If set to `true`, transpositions are counted as one change instead of two, defaults to `true`

`min_length`
:   Minimum length of the input before fuzzy suggestions are returned, defaults `3`

`prefix_length`
:   Minimum length of the input, which is not checked for fuzzy alternatives, defaults to `1`

`unicode_aware`
:   If `true`, all measurements (like fuzzy edit distance, transpositions, and lengths) are measured in Unicode code points instead of in bytes. This is slightly slower than raw bytes, so it is set to `false` by default.

::::{note} 
If you want to stick with the default values, but still use fuzzy, you can either use `fuzzy: {}` or `fuzzy: true`.
::::

### Regex queries [regex]

The completion suggester also supports regex queries meaning you can express a prefix as a regular expression

```console
POST music/_search?pretty
{
  "suggest": {
    "song-suggest": {
      "regex": "n[ever|i]r",
      "completion": {
        "field": "suggest"
      }
    }
  }
}
```

The regex query can take specific regex parameters. For example:

`flags`
:   Possible flags are `ALL` (default), `ANYSTRING`, `COMPLEMENT`, `EMPTY`, `INTERSECTION`, `INTERVAL`, or `NONE`. See [regexp-syntax](/reference/query-languages/query-dsl/query-dsl-regexp-query.md) for their meaning

`max_determinized_states`
:   Regular expressions are dangerous because it's easy to accidentally create an innocuous looking one that requires an exponential number of internal determinized automaton states (and corresponding RAM and CPU) for Lucene to execute. Lucene prevents these using the `max_determinized_states` setting (defaults to 10000). You can raise this limit to allow more complex regular expressions to execute.

## Context suggester [context-suggester]

The completion suggester considers all documents in the index, but it is often desirable to serve suggestions filtered and/or boosted by some criteria. For example, you want to suggest song titles filtered by certain artists or you want to boost song titles based on their genre.

To achieve suggestion filtering and/or boosting, you can add context mappings while configuring a completion field. You can define multiple context mappings for a completion field. Every context mapping has a unique name and a type. There are two types: `category` and `geo`. Context mappings are configured under the `contexts` parameter in the field mapping.

::::{note} 
It is mandatory to provide a context when indexing and querying a context enabled completion field.

The maximum allowed number of completion field context mappings is 10.
::::

The following example defines types, each with two context mappings for a completion field:

```console
PUT place
{
  "mappings": {
    "properties": {
      "suggest": {
        "type": "completion",
        "contexts": [
          {                                 <1>
            "name": "place_type",
            "type": "category"
          },
          {                                 <2>
            "name": "location",
            "type": "geo",
            "precision": 4
          }
        ]
      }
    }
  }
}
PUT place_path_category
{
  "mappings": {
    "properties": {
      "suggest": {
        "type": "completion",
        "contexts": [
          {                           <3>
            "name": "place_type",
            "type": "category",
            "path": "cat"
          },
          {                           <4>
            "name": "location",
            "type": "geo",
            "precision": 4,
            "path": "loc"
          }
        ]
      },
      "loc": {
        "type": "geo_point"
      }
    }
  }
}
```
% TESTSETUP

1. Defines a `category` context named *place_type* where the categories must be sent with the suggestions.
2. Defines a `geo` context named *location* where the categories must be sent with the suggestions.
3. Defines a `category` context named *place_type* where the categories are read from the `cat` field.
4. Defines a `geo` context named *location* where the categories are read from the `loc` field.


::::{note} 
Adding context mappings increases the index size for completion field. The completion index is entirely heap resident, you can monitor the completion field index size using [index statistics](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-stats).
::::

### Category context [suggester-context-category] 

The `category` context allows you to associate one or more categories with suggestions at index time. At query time, suggestions can be filtered and boosted by their associated categories.

The mappings are set up like the `place_type` fields above. If `path` is defined then the categories are read from that path in the document, otherwise they must be sent in the suggest field like this:

```console
PUT place/_doc/1
{
  "suggest": {
    "input": [ "timmy's", "starbucks", "dunkin donuts" ],
    "contexts": {
      "place_type": [ "cafe", "food" ]                    <1>
    }
  }
}
```

1. These suggestions will be associated with *cafe* and *food* category.

If the mapping had a `path` then the following index request would be enough to add the categories:

```console
PUT place_path_category/_doc/1
{
  "suggest": ["timmy's", "starbucks", "dunkin donuts"],
  "cat": ["cafe", "food"] <1>
}
```

1. These suggestions will be associated with *cafe* and *food* category.

::::{note} 
If context mapping references another field and the categories are explicitly indexed, the suggestions are indexed with both set of categories.
::::

#### Category query [_category_query] 

Suggestions can be filtered by one or more categories. The following filters suggestions by multiple categories:

```console
POST place/_search?pretty
{
  "suggest": {
    "place_suggestion": {
      "prefix": "tim",
      "completion": {
        "field": "suggest",
        "size": 10,
        "contexts": {
          "place_type": [ "cafe", "restaurants" ]
        }
      }
    }
  }
}
```
% TEST[continued]

::::{note} 
If multiple categories or category contexts are set on the query they are merged as a disjunction. This means that suggestions match if they contain at least one of the provided context values.
::::

Suggestions with certain categories can be boosted higher than others. The following example filters suggestions by categories and additionally boosts suggestions associated with some categories:

```console
POST place/_search?pretty
{
  "suggest": {
    "place_suggestion": {
      "prefix": "tim",
      "completion": {
        "field": "suggest",
        "size": 10,
        "contexts": {
          "place_type": [                             <1>
            { "context": "cafe" },
            { "context": "restaurants", "boost": 2 }
          ]
        }
      }
    }
  }
}
```
% TEST[continued]

1. The context query filter suggestions associated with categories *cafe* and *restaurants* and boosts the suggestions associated with *restaurants* by a factor of `2`

In addition to accepting category values, a context query can be composed of multiple category context clauses.
The parameters that are supported for a `category` context clause include:

`context`
:   The value of the category to filter/boost on. This is mandatory.

`boost`
:   The factor by which the score of the suggestion should be boosted, the score is computed by multiplying the boost with the suggestion weight, defaults to `1`

`prefix`
:   Whether the category value should be treated as a prefix or not. For example, if set to `true`, you can filter category of *type1*, *type2* and so on, by specifying a category prefix of *type*. Defaults to `false`

::::{note} 
If a suggestion entry matches multiple contexts the final score is computed as the maximum score produced by any matching contexts.
::::

### Geo location context [suggester-context-geo] 

A `geo` context allows you to associate one or more geo points or geohashes with suggestions at index time. At query time, suggestions can be filtered and boosted if they are within a certain distance of a specified geo location.

Internally, geo points are encoded as geohashes with the specified precision.

#### Geo mapping [_geo_mapping] 

In addition to the `path` setting, `geo` context mapping accepts settings such as:

`precision`
:   This defines the precision of the geohash to be indexed and can be specified as a distance value (`5m`, `10km` etc.), or as a raw geohash precision (`1`..`12`). Defaults to a raw geohash precision value of `6`.

::::{note} 
The index time `precision` setting sets the maximum geohash precision that can be used at query time.
::::

#### Indexing geo contexts [_indexing_geo_contexts] 

`geo` contexts can be explicitly set with suggestions or be indexed from a geo point field in the document via the `path` parameter, similar to `category` contexts. Associating multiple geo location context with a suggestion, will index the suggestion for every geo location. The following indexes a suggestion with two geo location contexts:

```console
PUT place/_doc/1
{
  "suggest": {
    "input": "timmy's",
    "contexts": {
      "location": [
        {
          "lat": 43.6624803,
          "lon": -79.3863353
        },
        {
          "lat": 43.6624718,
          "lon": -79.3873227
        }
      ]
    }
  }
}
```

#### Geo location query [_geo_location_query] 

Suggestions can be filtered and boosted with respect to how close they are to one or more geo points. The following filters suggestions that fall within the area represented by the encoded geohash of a geo point:

```console
POST place/_search
{
  "suggest": {
    "place_suggestion": {
      "prefix": "tim",
      "completion": {
        "field": "suggest",
        "size": 10,
        "contexts": {
          "location": {
            "lat": 43.662,
            "lon": -79.380
          }
        }
      }
    }
  }
}
```
% TEST[continued]

::::{note} 
When a location with a lower precision at query time is specified, all suggestions that fall within the area will be considered.

If multiple categories or category contexts are set on the query they are merged as a disjunction. This means that suggestions match if they contain at least one of the provided context values.
::::

Suggestions that are within an area represented by a geohash can also be boosted higher than others, as shown by the following:

```console
POST place/_search?pretty
{
  "suggest": {
    "place_suggestion": {
      "prefix": "tim",
      "completion": {
        "field": "suggest",
        "size": 10,
        "contexts": {
          "location": [             <1>
                      {
              "lat": 43.6624803,
              "lon": -79.3863353,
              "precision": 2
            },
            {
              "context": {
                "lat": 43.6624803,
                "lon": -79.3863353
              },
              "boost": 2
            }
          ]
        }
      }
    }
  }
}
```
% TEST[continued]

1. The context query filters for suggestions that fall under the geo location represented by a geohash of *(43.662, -79.380)* with a precision of *2* and boosts suggestions that fall under the geohash representation of *(43.6624803, -79.3863353)* with a default precision of *6* by a factor of `2`

::::{note} 
If a suggestion entry matches multiple contexts the final score is computed as the maximum score produced by any matching contexts.
::::

In addition to accepting context values, a context query can be composed of multiple context clauses.
The parameters that are supported for a `geo` context clause include:

`context`
:   A geo point object or a geo hash string to filter or boost the suggestion by. This is mandatory.

`boost`
:   The factor by which the score of the suggestion should be boosted, the score is computed by multiplying the boost with the suggestion weight, defaults to `1`

`precision`
:   The precision of the geohash to encode the query geo point. This can be specified as a distance value (`5m`, `10km` etc.), or as a raw geohash precision (`1`..`12`). Defaults to index time precision level.

`neighbours`
:   Accepts an array of precision values at which neighbouring geohashes should be taken into account. precision value can be a distance value (`5m`, `10km` etc.) or a raw geohash precision (`1`..`12`). Defaults to generating neighbours for index time precision level.

::::{note}
The precision field does not result in a distance match. Specifying a distance value like `10km` only results in a geohash precision value that represents tiles of that size. The precision will be used to encode the search geo point into a geohash tile for completion matching. A consequence of this is that points outside that tile, even if very close to the search point, will not be matched. Reducing the precision, or increasing the distance, can reduce the risk of this happening, but not entirely remove it.
::::

## Returning the type of the suggester [return-suggesters-type]

Sometimes you need to know the exact type of a suggester in order to parse its results. The `typed_keys` parameter can be used to change the suggester's name in the response so that it will be prefixed by its type.

Considering the following example with two suggesters `term` and `phrase`:

```console
POST _search?typed_keys
{
  "suggest": {
    "text" : "some test mssage",
    "my-first-suggester" : {
      "term" : {
        "field" : "message"
      }
    },
    "my-second-suggester" : {
      "phrase" : {
        "field" : "message"
      }
    }
  }
}
```
% TEST[setup:messages]

In the response, the suggester names will be changed to respectively `term#my-first-suggester` and `phrase#my-second-suggester`, reflecting the types of each suggestion:

```console-result
{
  "suggest": {
    "term#my-first-suggester": [ <1>
      {
        "text": "some",
        "offset": 0,
        "length": 4,
        "options": []
      },
      {
        "text": "test",
        "offset": 5,
        "length": 4,
        "options": []
      },
      {
        "text": "mssage",
        "offset": 10,
        "length": 6,
        "options": [
          {
            "text": "message",
            "score": 0.8333333,
            "freq": 4
          }
        ]
      }
    ],
    "phrase#my-second-suggester": [ <2>
      {
        "text": "some test mssage",
        "offset": 0,
        "length": 16,
        "options": [
          {
            "text": "some test message",
            "score": 0.030227963
          }
        ]
      }
    ]
  },
  ...
}
```
% TESTRESPONSE[s/\.\.\./"took": "$body.took", "timed_out": false, "_shards": "$body._shards", "hits": "$body.hits"/]
% TESTRESPONSE[s/"score": 0.8333333/"score": $body.suggest.term#my-first-suggester.2.options.0.score/]
% TESTRESPONSE[s/"score": 0.030227963/"score": $body.suggest.phrase#my-second-suggester.0.options.0.score/]

1. The name `my-first-suggester` now contains the `term` prefix.
2. The name `my-second-suggester` now contains the `phrase` prefix.
