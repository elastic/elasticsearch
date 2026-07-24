---
navigation_title: "Quickstart: Image search"
applies_to:
  stack: preview 9.5
  serverless: preview
---

# Build image search with a `semantic` field in {{es}} [semantic-quickstart]

This quickstart maps a `semantic` field, indexes an image, and searches for the image using natural-language text. Elastic recommends [Jina multimodal embeddings](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-jina.md#jina-multimodal-embeddings) for multimodal search.

The quickstart uses the preconfigured `.jina-embeddings-v5-omni-small` endpoint, available through the [Elastic {{infer-cap}} Service (EIS)](docs-content://explore-analyze/elastic-inference/eis.md).

To run the curl examples, set your Elasticsearch URL and API key as environment variables:

```bash
export ELASTICSEARCH_URL="https://<DEPLOYMENT_URL>"
export ELASTICSEARCH_API_KEY="<ELASTICSEARCH_API_KEY>"
```

:::::::{stepper}

::::::{step} Create an image search index

Create an index with metadata fields and a `semantic` field named `image`:

:::::{tab-set}

::::{tab-item} Console

```console
PUT image-search
{
  "mappings": {
    "properties": {
      "title": {
        "type": "keyword"
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
}
```
% TEST[skip:Requires access to the preconfigured EIS endpoint]

::::

::::{tab-item} curl

```bash
curl --fail-with-body --silent --show-error \
  --request PUT \
  --url "$ELASTICSEARCH_URL/image-search" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data '
{
  "mappings": {
    "properties": {
      "title": {
        "type": "keyword"
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

::::

:::::

Unlike [`semantic_text`](./semantic-text.md), a `semantic` field has no default {{infer}} endpoint. You must use an endpoint that uses the `embedding` task type and specify its ID in the field mapping.

::::::

::::::{step} Index an image

This example uses [*Cat on windowsill*](https://commons.wikimedia.org/wiki/File:Cat_on_windowsill_-_geograph.org.uk_-_435478.jpg). Download the image using `curl` in your terminal:

```bash
curl --fail-with-body --silent --show-error --location \
  --output cat-on-windowsill.jpg \
  "https://commons.wikimedia.org/wiki/Special:Redirect/file/Cat_on_windowsill_-_geograph.org.uk_-_435478.jpg"
```

Encode the image as Base64 for the [data URL](https://developer.mozilla.org/en-US/docs/Web/URI/Reference/Schemes/data). The curl example uses the resulting shell variable. For Console, replace `<BASE64_ENCODED_IMAGE>` with its value:

```bash
image_data=$(base64 < cat-on-windowsill.jpg | tr -d '\n')
```

:::::{tab-set}

::::{tab-item} Console

```console
PUT image-search/_doc/cat-on-windowsill?refresh=wait_for
{
  "title": "Cat on windowsill",
  "source_url": "https://commons.wikimedia.org/wiki/File:Cat_on_windowsill_-_geograph.org.uk_-_435478.jpg",
  "image": {
    "type": "image",
    "value": "data:image/jpeg;base64,<BASE64_ENCODED_IMAGE>"
  }
}
```
% TEST[skip:Requires a Base64-encoded image and a multimodal embedding endpoint]

::::

::::{tab-item} curl

```bash
curl --fail-with-body --silent --show-error \
  --request PUT \
  --url "$ELASTICSEARCH_URL/image-search/_doc/cat-on-windowsill?refresh=wait_for" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data-binary @- <<JSON
{
  "title": "Cat on windowsill",
  "source_url": "https://commons.wikimedia.org/wiki/File:Cat_on_windowsill_-_geograph.org.uk_-_435478.jpg",
  "image": {
    "type": "image",
    "value": "data:image/jpeg;base64,$image_data"
  }
}
JSON
```

::::

:::::

Elasticsearch uses the field's {{infer}} endpoint to generate and index an embedding for the image.

::::::

::::::{step} Search for the image using text

Run a `match` query against the `image` field. The response excludes the image data URL from `_source`:

:::::{tab-set}

::::{tab-item} Console

```console
GET image-search/_search
{
  "_source": [
    "title",
    "source_url"
  ],
  "query": {
    "match": {
      "image": "a cat sitting on a windowsill"
    }
  }
}
```
% TEST[skip:Requires an indexed image and a multimodal embedding endpoint]

::::

::::{tab-item} curl

```bash
curl --fail-with-body --silent --show-error \
  --request POST \
  --url "$ELASTICSEARCH_URL/image-search/_search" \
  --header "Authorization: ApiKey $ELASTICSEARCH_API_KEY" \
  --header "Content-Type: application/json" \
  --data '
{
  "_source": [
    "title",
    "source_url"
  ],
  "query": {
    "match": {
      "image": "a cat sitting on a windowsill"
    }
  }
}'
```

::::

:::::

The endpoint embeds the text query in the same vector space as the indexed image and returns the most semantically similar results.

::::::

:::::::
