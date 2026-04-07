---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-weight-context.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: painless
---

# Weight context [painless-weight-context]

Use a Painless script to create a [weight](/reference/elasticsearch/index-settings/similarity.md) for use in a [similarity script](/reference/scripting-languages/painless/painless-similarity-context.md). The weight makes up the part of the similarity calculation that is independent of the document being scored, and so can be built up front and cached.

Queries that contain multiple terms calculate a separate weight for each term.

## Variables

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

## Return

`double`
:   A scoring factor used across all documents.

## API

The standard [Painless API](https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-api-reference-shared.html) is available.

## Example

To run the example, first [install the eCommerce sample data](/reference/scripting-languages/painless/painless-context-examples.md#painless-sample-data-install).

This request creates an index with a smart search system that automatically boosts products with rare, distinctive features like "vintage", "handcrafted", or unique materials. It helps discover special items by making uncommon products rank higher in search results.

```json
PUT kibana_sample_data_ecommerce-weight
{
  "settings": {
    "similarity": {
      "rare_term_weight": {
        "type": "scripted",
        "weight_script": {
          "source": """
            double idf = Math.log((field.docCount + 1.0) / (term.docFreq + 1.0));
            
            double rarityFactor = 1.0;
            if (term.docFreq < field.docCount * 0.02) {
              rarityFactor = 3.0;  // Very rare terms get a high boost
            } else if (term.docFreq < field.docCount * 0.10) {
              rarityFactor = 1.5;  // Somewhat rare terms get a medium boost
            }
            
            return query.boost * idf * rarityFactor;
          """
        },
        "script": {
          "source": """
            double tf = Math.sqrt(doc.freq);

            return tf * weight;
          """
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "product_name": {
        "type": "text",
        "similarity": "rare_term_weight"
      },
      "description": {
        "type": "text", 
        "similarity": "rare_term_weight"
      }
    }
  }
}
```
