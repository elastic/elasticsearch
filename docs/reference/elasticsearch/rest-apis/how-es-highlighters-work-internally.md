---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/highlighting.html
applies_to:
  stack: all
  serverless: all
---

# How highlighters work internally [how-es-highlighters-work-internally]

Given a query and a text (the content of a document field), the goal of a highlighter is to find the best text fragments for the query, and highlight the query terms in the found fragments. For this, a highlighter needs to address several questions:

* How to break a text into fragments?
* How to find the best fragments among all fragments?
* How to highlight the query terms in a fragment?


## How to break a text into fragments [_how_to_break_a_text_into_fragments]

Relevant settings: [`fragment_size`](highlighting-settings.md#fragment_size), [`fragmenter`](highlighting-settings.md#fragmenter), [`type`](highlighting-settings.md#highlighter-type), [`boundary_chars`](highlighting-settings.md#boundary_chars), [`boundary_max_scan`](highlighting-settings.md#boundary_max_scan), [`boundary_scanner`](highlighting-settings.md#boundary-scanner), [`boundary_scanner_locale`](highlighting-settings.md#boundary_scanner_locale).

Plain highlighter begins with analyzing the text using the given analyzer, and creating a token stream from it. Plain highlighter uses a very simple algorithm to break the token stream into fragments. It loops through terms in the token stream, and every time the current term’s end_offset exceeds `fragment_size` multiplied by the number of created fragments, a new fragment is created. A little more computation is done with using `span` fragmenter to avoid breaking up text between highlighted terms. But overall, since the breaking is done only by `fragment_size`, some fragments can be quite odd, e.g. beginning with a punctuation mark.

Unified or FVH highlighters do a better job of breaking up a text into fragments by utilizing Java’s `BreakIterator`. This ensures that a fragment is a valid sentence as long as `fragment_size` allows for this.


## How to find the best fragments [_how_to_find_the_best_fragments]

Relevant settings: [`number_of_fragments`](highlighting-settings.md#number_of_fragments).

To find the best, most relevant, fragments, a highlighter needs to score each fragment in respect to the given query. The goal is to score only those terms that participated in generating the *hit* on the document. For some complex queries, this is still work in progress.

The plain highlighter creates an in-memory index from the current token stream, and re-runs the original query criteria through Lucene’s query execution planner to get access to low-level match information for the current text. For more complex queries the original query could be converted to a span query, as span queries can handle phrases more accurately. Then this obtained low-level match information is used to score each individual fragment. The scoring method of the plain highlighter is quite simple. Each fragment is scored by the number of unique query terms found in this fragment. The score of individual term is equal to its boost, which is by default is 1. Thus, by default, a fragment that contains one unique query term, will get a score of 1; and a fragment that contains two unique query terms, will get a score of 2 and so on. The fragments are then sorted by their scores, so the highest scored fragments will be output first.

FVH doesn’t need to analyze the text and build an in-memory index, as it uses pre-indexed document term vectors, and finds among them terms that correspond to the query. FVH scores each fragment by the number of query terms found in this fragment. Similarly to plain highlighter, score of individual term is equal to its boost value. In contrast to plain highlighter, all query terms are counted, not only unique terms.

Unified highlighter can use pre-indexed term vectors or pre-indexed terms offsets, if they are available. Otherwise, similar to Plain Highlighter, it has to create an in-memory index from the text. Unified highlighter uses the BM25 scoring model to score fragments.


## How to highlight the query terms in a fragment [_how_to_highlight_the_query_terms_in_a_fragment]

Relevant settings:  [`pre_tags`](highlighting-settings.md#pre_tags), [`post_tags`](highlighting-settings.md#post_tags).


The goal is to highlight only those terms that participated in generating the *hit* on the document. For some complex boolean queries, this is still work in progress, as highlighters don’t reflect the boolean logic of a query and only extract leaf (terms, phrases, prefix etc) queries.

Plain highlighter given the token stream and the original text, recomposes the original text to highlight only terms from the token stream that are contained in the low-level match information structure from the previous step.

FVH and unified highlighter use intermediate data structures to represent fragments in some raw form, and then populate them with actual text.

A highlighter uses `pre-tags`, `post-tags` to encode highlighted terms.


## How the unified highlighter works [_an_example_of_the_work_of_the_unified_highlighter]

In this section, we'll explore how the unified highlighter works in more detail with an example.

First, we create a index with a text field `content`, that will be indexed using `english` analyzer, and will be indexed without offsets or term vectors.

```js
PUT test_index
{
  "mappings": {
    "properties": {
      "content": {
        "type": "text",
        "analyzer": "english"
      }
    }
  }
}
```

We put the following document into the index:

```js
PUT test_index/_doc/doc1
{
  "content" : "For you I'm only a fox like a hundred thousand other foxes. But if you tame me, we'll need each other. You'll be the only boy in the world for me. I'll be the only fox in the world for you."
}
```

And we ran the following query with a highlight request:

```js
GET test_index/_search
{
  "query": {
    "match_phrase" : {"content" : "only fox"}
  },
  "highlight": {
    "type" : "unified",
    "number_of_fragments" : 3,
    "fields": {
      "content": {}
    }
  }
}
```

After `doc1` is found as a hit for this query, this hit will be passed to the unified highlighter for highlighting the field `content` of the document. Since the field `content` was not indexed either with offsets or term vectors, its raw field value will be analyzed, and in-memory index will be built from the terms that match the query:

```
{"token":"onli","start_offset":12,"end_offset":16,"position":3},
{"token":"fox","start_offset":19,"end_offset":22,"position":5},
{"token":"fox","start_offset":53,"end_offset":58,"position":11},
{"token":"onli","start_offset":117,"end_offset":121,"position":24},
{"token":"onli","start_offset":159,"end_offset":163,"position":34},
{"token":"fox","start_offset":164,"end_offset":167,"position":35}
```
Our complex phrase query will be converted to the span query: `spanNear([text:onli, text:fox], 0, true)`, meaning that we are looking for terms "onli: and "fox" within 0 distance from each other, and in the given order. The span query will be run against the created before in-memory index, to find the following match:

```
{"term":"onli", "start_offset":159, "end_offset":163},
{"term":"fox", "start_offset":164, "end_offset":167}
```
In our example, we have got a single match, but there could be several matches. Given the matches, the unified highlighter breaks the text of the field into so called "passages". Each passage must contain at least one match. The unified highlighter with the use of Java’s `BreakIterator` ensures that each passage represents a full sentence as long as it doesn’t exceed `fragment_size`. For our example, we have got a single passage with the following properties (showing only a subset of the properties here):

```
Passage:
    startOffset: 147
    endOffset: 189
    score: 3.7158387
    matchStarts: [159, 164]
    matchEnds: [163, 167]
    numMatches: 2
```
Notice how a passage has a score, calculated using the BM25 scoring formula adapted for passages. Scores allow us to choose the best scoring passages if there are more passages available than the requested by the user `number_of_fragments`. Scores also let us to sort passages by `order: "score"` if requested by the user.

As the final step, the unified highlighter will extract from the field’s text a string corresponding to each passage:

```
"I'll be the only fox in the world for you."
```
and will format with the tags <em> and </em> all matches in this string using the passages’s `matchStarts` and `matchEnds` information:

```
I'll be the <em>only</em> <em>fox</em> in the world for you.
```
This kind of formatted strings are the final result of the highlighter returned to the user.
