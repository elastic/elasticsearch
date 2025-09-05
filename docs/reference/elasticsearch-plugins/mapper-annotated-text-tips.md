---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/mapper-annotated-text-tips.html
---

# Data modelling tips [mapper-annotated-text-tips]

## Use structured and unstructured fields [_use_structured_and_unstructured_fields]

Annotations are normally a way of weaving structured information into unstructured text for higher-precision search.

`Entity resolution` is a form of document enrichment undertaken by specialist software or people where references to entities in a document are disambiguated by attaching a canonical ID. The ID is used to resolve any number of aliases or distinguish between people with the same name. The hyperlinks connecting Wikipedia’s articles are a good example of resolved entity IDs woven into text.

These IDs can be embedded as annotations in an annotated_text field but it often makes sense to include them in dedicated structured fields to support discovery via aggregations:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_unstructured_text_field": {
        "type": "annotated_text"
      },
      "my_structured_people_field": {
        "type": "text",
        "fields": {
          "keyword" : {
            "type": "keyword"
          }
        }
      }
    }
  }
}
```

Applications would then typically provide content and discover it as follows:

```console
# Example documents
PUT my-index-000001/_doc/1
{
  "my_unstructured_text_field": "[Shay](%40kimchy) created elasticsearch",
  "my_twitter_handles": ["@kimchy"] <1>
}

GET my-index-000001/_search
{
  "query": {
    "query_string": {
        "query": "elasticsearch OR logstash OR kibana",<2>
        "default_field": "my_unstructured_text_field"
    }
  },
  "aggregations": {
  	"top_people" :{
  	    "significant_terms" : { <3>
	       "field" : "my_twitter_handles.keyword"
  	    }
  	}
  }
}
```

1. Note the `my_twitter_handles` contains a list of the annotation values also used in the unstructured text. (Note the annotated_text syntax requires escaping). By repeating the annotation values in a structured field this application has ensured that the tokens discovered in the structured field can be used for search and highlighting in the unstructured field.
2. In this example we search for documents that talk about components of the elastic stack
3. We use the `my_twitter_handles` field here to discover people who are significantly associated with the elastic stack.



## Avoiding over-matching annotations [_avoiding_over_matching_annotations]

By design, the regular text tokens and the annotation tokens co-exist in the same indexed field but in rare cases this can lead to some over-matching.

The value of an annotation often denotes a *named entity* (a person, place or company). The tokens for these named entities are inserted untokenized, and differ from typical text tokens because they are normally:

* Mixed case e.g. `Madonna`
* Multiple words e.g. `Jeff Beck`
* Can have punctuation or numbers e.g. `Apple Inc.` or `@kimchy`

This means, for the most part, a search for a named entity in the annotated text field will not have any false positives e.g. when selecting `Apple Inc.` from an aggregation result you can drill down to highlight uses in the text without "over matching" on any text tokens like the word `apple` in this context:

```
the apple was very juicy
```
However, a problem arises if your named entity happens to be a single term and lower-case e.g. the company `elastic`. In this case, a search on the annotated text field for the token `elastic` may match a text document such as this:

```
they fired an elastic band
```
To avoid such false matches users should consider prefixing annotation values to ensure they don’t name clash with text tokens e.g.

```
[elastic](Company_elastic) released version 7.0 of the elastic stack today
```

