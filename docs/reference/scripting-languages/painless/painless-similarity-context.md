---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-similarity-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Similarity context [painless-similarity-context]

Use a Painless script to create a [similarity](/reference/elasticsearch/index-settings/similarity.md) equation for scoring documents in a query.

:::{tip}
This is an advanced feature for customizing how document relevance scores are calculated during search. For comprehensive information about similarity functions and their implementation, refer to the [similarity documentation](/reference/elasticsearch/index-settings/similarity.md).
:::

## Variables

`weight` (`float`, read-only)
:   The weight as calculated by a [weight script](/reference/scripting-languages/painless/painless-weight-context.md)

`query.boost` (`float`, read-only)
:   The boost value if provided by the query. If this is not provided the value is `1.0f`.

`field.docCount` (`long`, read-only)
:   The number of documents that have a value for the current field.

`field.sumDocFreq` (`long`, read-only)
:   The sum of all terms that exist for the current field. If this is not available the value is `-1`.

`field.sumTotalTermFreq` (`long`, read-only)
:   The sum of occurrences in the index for all the terms that exist in the current field. If this is not available the value is `-1`.

`term.docFreq` (`long`, read-only)
:   The number of documents that contain the current term in the index.

`term.totalTermFreq` (`long`, read-only)
:   The total occurrences of the current term in the index.

`doc.length` (`long`, read-only)
:   The number of tokens the current document has in the current field. This is decoded from the stored [norms](/reference/elasticsearch/mapping-reference/norms.md) and may be approximate for long fields

`doc.freq` (`long`, read-only)
:   The number of occurrences of the current term in the current document for the current field.

Note that the `query`, `field`, and `term` variables are also available to the [weight context](/reference/scripting-languages/painless/painless-weight-context.md). They are more efficiently used there, as they are constant for all documents.

For queries that contain multiple terms, the script is called once for each term with that term’s calculated weight, and the results are summed. Note that some terms might have a `doc.freq` value of `0` on a document, for example if a query uses synonyms.

## Return

`double`
:   The similarity score for the current document.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

The following request creates a new index named `ecommerce_rare_terms` with a custom `similarity` equation called `rare_boost`. This similarity function modifies how document relevance scores are calculated during search by giving more weight to rare terms. 

This can be used if you want to customize how documents are matched and scored against query terms, for example to boost products with unique or uncommon attributes in search results.

```json
PUT kibana_sample_data_ecommerce-rare_terms
{
  "settings": {
    "similarity": {
      "rare_boost": {
        "type": "scripted",
        "script": {
          "source": """
            double tf = Math.sqrt(doc.freq);
            double idf = Math.log((field.docCount + 1)/(term.docFreq + 1)); // If the term appears in less than 5% of the documents, it will be boosted
            double rareBoost = term.docFreq < (field.docCount * 0.05) ? 2 : 1;
            return query.boost * tf * idf * rareBoost;
          """
        }
      }
    }
  }
}
```
