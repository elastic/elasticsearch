---
navigation_title: "Quickstart: Multimodal search"
description: Learn how to index images in a semantic field and search them with text, image, and PDF input.
applies_to:
  stack: preview 9.5
  serverless: preview
products:
  - id: elasticsearch
---

# Build multimodal search with a `semantic` field in {{es}} [semantic-quickstart]

In this hands-on tutorial, you'll build a small visual archive and search it with natural-language text, another image, or a PDF. It's intended for developers who are new to multimodal search and are comfortable running {{es}} API requests and terminal commands.

By the end of this tutorial, you'll be able to:

- Configure a `semantic` field with a multimodal {{infer}} endpoint.
- Download, encode, and index images with structured metadata.
- Search the same image field with text, image, and PDF input.
- Combine semantic similarity with structured filters.

## Before you begin [semantic-quickstart-before-you-begin]

You need:

- Basic familiarity with {{es}} mappings and search APIs.
- An Elasticsearch 9.5 or later deployment, or an Elasticsearch Serverless project.
- A license that supports the {{infer}} API.
- Access to a multimodal `embedding` endpoint. This tutorial uses the preconfigured `.jina-embeddings-v5-omni-small` endpoint through the [Elastic {{infer-cap}} Service (EIS)](docs-content://explore-analyze/elastic-inference/eis.md).
- `curl` and the `base64` command-line utility.

The tutorial uses four NASA-hosted images with visually distinct subjects and useful metadata:

| Document | Subject | Year | Image source |
|---|---|---|---|
| `apollo-11` | Apollo 11 lunar surface activity | 1969 | [NASA Image and Video Library](https://images.nasa.gov/details/as11-40-5874) |
| `curiosity` | Curiosity rover on Mars | 2015 | [NASA Jet Propulsion Laboratory](https://www.jpl.nasa.gov/images/pia19808-looking-up-at-mars-rover-curiosity-in-buckskin-selfie/) |
| `cassini` | Saturn and its rings | 2004 | [NASA Science](https://science.nasa.gov/image-detail/pia05380-saturn-with-rings-16x9/) |
| `atlantis` | Space Shuttle Atlantis launching | 2009 | [NASA Science](https://science.nasa.gov/image-detail/sts-125-launch-may-11-2009/) |

NASA should be acknowledged as the source of this material. Refer to the [NASA images and media usage guidelines](https://www.nasa.gov/nasa-brand-center/images-and-media/) before reusing the images outside this example.

To run the `curl` examples, set your Elasticsearch URL and API key as environment variables:

```sh
export ELASTICSEARCH_URL="https://<DEPLOYMENT_URL>"
export ELASTICSEARCH_API_KEY="<ELASTICSEARCH_API_KEY>"
```

## 1. Verify the multimodal inference endpoint [semantic-quickstart-verify-endpoint]

This example uses the preconfigured `.jina-embeddings-v5-omni-small` endpoint. Elastic recommends [Jina multimodal embeddings](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-jina.md#jina-multimodal-embeddings) for multimodal search.

Verify that the endpoint is available to your deployment:

```sh
curl --fail-with-body --silent --show-error \
  --request GET \
  --url "$ELASTICSEARCH_URL/_inference/embedding/.jina-embeddings-v5-omni-small" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY"
```

The endpoint uses the `embedding` task type and supports the text, image, and PDF inputs used in this quickstart.

The response identifies `.jina-embeddings-v5-omni-small` as an `embedding` endpoint. You can now use its ID in a `semantic` field mapping.

If `.jina-embeddings-v5-omni-small` isn't available, configure another `embedding` endpoint that supports text, images, and PDFs, and replace the endpoint ID in the remaining steps.

## 2. Create the image index [semantic-quickstart-create-index]

Create an index with ordinary metadata fields and a `semantic` field named `image`:

```sh
curl --fail-with-body --silent --show-error \
  --request PUT \
  --url "$ELASTICSEARCH_URL/nasa-mission-images" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data '
{
  "mappings": {
    "properties": {
      "title": {
        "type": "keyword"
      },
      "mission": {
        "type": "keyword"
      },
      "year": {
        "type": "integer"
      },
      "description": {
        "type": "text"
      },
      "source_url": {
        "type": "keyword",
        "index": false
      },
      "image": {
        "type": "semantic",
        "inference_id": ".jina-embeddings-v5-omni-small"
      }
    }
  }
}'
```

The create index request returns `"acknowledged": true`. The `image` field is now configured to use `.jina-embeddings-v5-omni-small` to generate dense vectors.

## 3. Index the image dataset [semantic-quickstart-index-data]

The `semantic` field accepts an image as an object containing a Base64 [data URL](https://developer.mozilla.org/en-US/docs/Web/URI/Reference/Schemes/data). The following shell function downloads and encodes each image before indexing it with its metadata:

```sh
set -euo pipefail

index_image() {
  local id="$1"
  local title="$2"
  local mission="$3"
  local year="$4"
  local description="$5"
  local image_url="$6"
  local source_url="$7"
  local image_data

  image_data=$(curl --fail-with-body --silent --show-error --location "$image_url" \
    | base64 | tr -d '\n')

  curl --fail-with-body --silent --show-error \
    --request PUT \
    --url "$ELASTICSEARCH_URL/nasa-mission-images/_doc/$id" \
    --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
    --header "Content-Type: application/json" \
    --data-binary @- <<JSON
{
  "title": "$title",
  "mission": "$mission",
  "year": $year,
  "description": "$description",
  "source_url": "$source_url",
  "image": {
    "type": "image",
    "format": "base64",
    "value": "data:image/jpeg;base64,$image_data"
  }
}
JSON
}

index_image \
  "apollo-11" \
  "Apollo 11 lunar EVA" \
  "Apollo 11" \
  1969 \
  "Astronaut Buzz Aldrin on the lunar surface beside the United States flag." \
  "https://images-assets.nasa.gov/image/as11-40-5874/as11-40-5874~medium.jpg" \
  "https://images.nasa.gov/details/as11-40-5874"

index_image \
  "curiosity" \
  "Curiosity rover selfie at Buckskin" \
  "Mars Science Laboratory" \
  2015 \
  "The Curiosity rover on the rocky surface of Mars near Mount Sharp." \
  "https://images-assets.nasa.gov/image/PIA19808/PIA19808~medium.jpg" \
  "https://www.jpl.nasa.gov/images/pia19808-looking-up-at-mars-rover-curiosity-in-buckskin-selfie/"

index_image \
  "cassini" \
  "Cassini approaches Saturn" \
  "Cassini-Huygens" \
  2004 \
  "A Cassini view of Saturn and its ring system." \
  "https://science.nasa.gov/wp-content/uploads/2023/05/pia05380-saturn-with-rings-16x9-1.jpg" \
  "https://science.nasa.gov/image-detail/pia05380-saturn-with-rings-16x9/"

index_image \
  "atlantis" \
  "Space Shuttle Atlantis launches" \
  "STS-125" \
  2009 \
  "Space Shuttle Atlantis launches from Kennedy Space Center." \
  "https://science.nasa.gov/wp-content/uploads/2023/07/27625669004-d61d5df323-o.jpg" \
  "https://science.nasa.gov/image-detail/sts-125-launch-may-11-2009/"

curl --fail-with-body --silent --show-error \
  --request POST \
  --url "$ELASTICSEARCH_URL/nasa-mission-images/_refresh" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY"
```

The images remain available in `_source`. Generated embeddings are excluded from `_source` by default.

Verify that all four documents were indexed:

```sh
curl --fail-with-body --silent --show-error \
  --request GET \
  --url "$ELASTICSEARCH_URL/nasa-mission-images/_count" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY"
```

::::{dropdown} View response

```json
{
  "count": 4
}
```

::::

A count of `4` confirms that the complete image dataset is ready to search.

## 4. Search the images with text [semantic-quickstart-text-query]

Run a `match` query against the `image` field using a natural-language description:

```sh
curl --fail-with-body --silent --show-error \
  --request POST \
  --url "$ELASTICSEARCH_URL/nasa-mission-images/_search" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data '
{
  "_source": {
    "excludes": ["image"] <1>
  },
  "query": {
    "match": {
      "image": { <2>
        "query": "a wheeled robot taking a selfie on a rocky red planet"
      }
    }
  }
}'
```

1. The `_source` exclusion omits the Base64-encoded `image` value while returning the other source fields.
2. A `match` query on a `semantic` field sends the text to the field's inference endpoint and compares the resulting embedding with the indexed image embeddings.

The Curiosity document is the highest-ranked result even though its caption is stored in a separate field and the query targets only `image`.

::::{dropdown} View response

```json
{
  "total": {
    "value": 4,
    "relation": "eq"
  },
  "max_score": 0.6938014,
  "hits": [
    {
      "_index": "nasa-mission-images",
      "_id": "curiosity",
      "_score": 0.6938014, <1>
      "_source": { <2>
        "title": "Curiosity rover selfie at Buckskin",
        "mission": "Mars Science Laboratory",
        "year": 2015,
        "description": "The Curiosity rover on the rocky surface of Mars near Mount Sharp.",
        "source_url": "https://www.jpl.nasa.gov/images/pia19808-looking-up-at-mars-rover-curiosity-in-buckskin-selfie/"
      }
    },
    {
      "_index": "nasa-mission-images",
      "_id": "apollo-11",
      "_score": 0.64529085,
      "_source": {
        "title": "Apollo 11 lunar EVA",
        "mission": "Apollo 11",
        "year": 1969,
        "description": "Astronaut Buzz Aldrin on the lunar surface beside the United States flag.",
        "source_url": "https://images.nasa.gov/details/as11-40-5874"
      }
    }
  ]
}
```

1. Curiosity ranks first based on the embedding generated from the indexed image. The `description` field is returned as metadata but does not affect this query. Exact scores can vary.
2. The `_source` exclusion omits the Base64-encoded image from the response. The image remains stored in `_source`.

::::

### Filter semantic image search results [semantic-quickstart-filter-results]

You can combine semantic image search with structured metadata filters. The following query limits results to images from 2000 or later:

```sh
curl --fail-with-body --silent --show-error \
  --request POST \
  --url "$ELASTICSEARCH_URL/nasa-mission-images/_search" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data '
{
  "_source": {
    "excludes": ["image"]
  },
  "query": {
    "bool": {
      "must": { <1>
        "match": {
          "image": {
            "query": "a spacecraft launching through fire and smoke"
          }
        }
      },
      "filter": { <2>
        "range": {
          "year": {
            "gte": 2000
          }
        }
      }
    }
  }
}'
```

1. The `must` clause performs semantic similarity search and contributes to each result's score.
2. The `filter` clause restricts the matching documents without changing their semantic relevance scores.

The Atlantis launch is the highest-ranked result.

## 5. Search the images with another image [semantic-quickstart-image-query]

For image-to-image search, use a `knn` query with the `embedding` query vector builder. The following request downloads and encodes [a different Curiosity selfie](https://images.nasa.gov/details/PIA19807) to use as the query image:

```sh
query_image=$(curl --fail-with-body --silent --show-error --location \
  "https://images-assets.nasa.gov/image/PIA19807/PIA19807~medium.jpg" \
  | base64 | tr -d '\n')

curl --fail-with-body --silent --show-error \
  --request POST \
  --url "$ELASTICSEARCH_URL/nasa-mission-images/_search" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data-binary @- <<JSON
{
  "_source": {
    "excludes": ["image"]
  },
  "query": {
    "knn": {
      "field": "image", <1>
      "query_vector_builder": {
        "embedding": {
          "input": { <2>
            "type": "image",
            "format": "base64",
            "value": "data:image/jpeg;base64,$query_image"
          }
        }
      },
      "k": 3, <3>
      "num_candidates": 4 <4>
    }
  }
}
JSON
```

1. The query searches the embeddings indexed for the `image` field and uses the inference endpoint from that field's mapping.
2. The `embedding` query vector builder sends the raw image input to the endpoint to generate the query vector.
3. `k` returns the three nearest neighbors.
4. `num_candidates` compares the query vector against four approximate nearest-neighbor candidates per shard.

::::{dropdown} View response

```json
{
  "total": {
    "value": 3,
    "relation": "eq"
  },
  "max_score": 0.9901605,
  "hits": [
    {
      "_index": "nasa-mission-images",
      "_id": "curiosity",
      "_score": 0.9901605, <1>
      "_source": {
        "title": "Curiosity rover selfie at Buckskin",
        "mission": "Mars Science Laboratory",
        "year": 2015,
        "source_url": "https://www.jpl.nasa.gov/images/pia19808-looking-up-at-mars-rover-curiosity-in-buckskin-selfie/"
      }
    },
    {
      "_index": "nasa-mission-images",
      "_id": "apollo-11",
      "_score": 0.9066043,
      "_source": {
        "title": "Apollo 11 lunar EVA",
        "mission": "Apollo 11",
        "year": 1969,
        "source_url": "https://images.nasa.gov/details/as11-40-5874"
      }
    },
    {
      "_index": "nasa-mission-images",
      "_id": "cassini",
      "_score": 0.8949027,
      "_source": {
        "title": "Cassini approaches Saturn",
        "mission": "Cassini-Huygens",
        "year": 2004,
        "source_url": "https://science.nasa.gov/image-detail/pia05380-saturn-with-rings-16x9/"
      }
    }
  ]
}
```

1. The separately indexed Curiosity selfie ranks first for the query selfie. The query and indexed images are different files, so this result demonstrates similarity search rather than an exact-image match. Exact scores can vary.

::::

## 6. Search the images with a PDF [semantic-quickstart-pdf-query]

The `embedding` query vector builder also accepts PDF input. This example uses a one-page [Apollo 11 photography map](https://commons.wikimedia.org/wiki/File:Apollo_11_photo_map.pdf) as the query.

The following request downloads and encodes the PDF before using it as the query:

```sh
query_pdf=$(curl --fail-with-body --silent --show-error --location \
  "https://commons.wikimedia.org/wiki/Special:Redirect/file/Apollo_11_photo_map.pdf" \
  | base64 | tr -d '\n')

curl --fail-with-body --silent --show-error \
  --request POST \
  --url "$ELASTICSEARCH_URL/nasa-mission-images/_search" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data-binary @- <<JSON
{
  "_source": {
    "excludes": ["image"]
  },
  "query": {
    "knn": {
      "field": "image",
      "query_vector_builder": {
        "embedding": {
          "input": {
            "type": "pdf", <1>
            "format": "base64",
            "value": "data:application/pdf;base64,$query_pdf" <2>
          }
        }
      },
      "k": 4,
      "num_candidates": 4
    }
  }
}
JSON
```

1. The input type tells the multimodal inference endpoint to process the binary as a PDF.
2. The PDF is supplied as a Base64 data URL and embedded at query time.

::::{dropdown} View response

```json
{
  "total": {
    "value": 4,
    "relation": "eq"
  },
  "max_score": 0.80268264,
  "hits": [
    {
      "_index": "nasa-mission-images",
      "_id": "apollo-11",
      "_score": 0.80268264, <1>
      "_source": {
        "title": "Apollo 11 lunar EVA",
        "mission": "Apollo 11",
        "year": 1969,
        "source_url": "https://images.nasa.gov/details/as11-40-5874"
      }
    },
    {
      "_index": "nasa-mission-images",
      "_id": "curiosity",
      "_score": 0.7965702,
      "_source": {
        "title": "Curiosity rover selfie at Buckskin",
        "mission": "Mars Science Laboratory",
        "year": 2015,
        "source_url": "https://www.jpl.nasa.gov/images/pia19808-looking-up-at-mars-rover-curiosity-in-buckskin-selfie/"
      }
    },
    {
      "_index": "nasa-mission-images",
      "_id": "cassini",
      "_score": 0.78309894,
      "_source": {
        "title": "Cassini approaches Saturn",
        "mission": "Cassini-Huygens",
        "year": 2004,
        "source_url": "https://science.nasa.gov/image-detail/pia05380-saturn-with-rings-16x9/"
      }
    },
    {
      "_index": "nasa-mission-images",
      "_id": "atlantis",
      "_score": 0.77586085,
      "_source": {
        "title": "Space Shuttle Atlantis launches",
        "mission": "STS-125",
        "year": 2009,
        "source_url": "https://science.nasa.gov/image-detail/sts-125-launch-may-11-2009/"
      }
    }
  ]
}
```

1. The Apollo 11 image ranks first for a separate Apollo 11 PDF. The query targets only the `image` field, so the ordinary metadata fields do not affect the ranking. Exact scores can vary.

::::

This PDF, the text query, the query image, and the indexed images are all embedded into the same vector space by the field's {{infer}} endpoint. The input format changes, but the search target does not.

## 7. Clean up [semantic-quickstart-clean-up]

Delete the example index:

```sh
curl --fail-with-body --silent --show-error \
  --request DELETE \
  --url "$ELASTICSEARCH_URL/nasa-mission-images" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY"
```

The `.jina-embeddings-v5-omni-small` inference endpoint is preconfigured and is not deleted.

## Next steps [semantic-quickstart-next-steps]

You now have a `semantic` field that embeds images at index time and accepts text, image, or PDF input at search time. To adapt this tutorial to an application:

- Replace the sample images and metadata with your own visual archive.
- Add metadata filters that reflect your application's access controls, categories, or date ranges.
- Adjust `k` and `num_candidates` to balance result quality and query performance for your dataset.

## Related pages [semantic-quickstart-related-pages]

- [`semantic` field type](./semantic-field.md)
- [Jina multimodal embeddings](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-jina.md#jina-multimodal-embeddings)
- [{{infer-cap}} endpoints](docs-content://explore-analyze/elastic-inference/inference-api.md)
- [kNN search](docs-content://solutions/search/vector/knn.md)
