---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/8.18/indices-disk-usage.html
applies_to:
  serverless: unavailable
  stack: all
navigation_title: Analyze index disk usage
---

# Analyze index disk usage API example

The `_disk_usage` API analyzes how much disk space each field in an index or data stream consumes. It helps you understand storage distribution and identify fields that use the most space. This page shows an example request and response for the [Analyze index disk usage API]({{es-apis}}operation/operation-indices-disk-usage).

## Example request

The following request analyzes the disk usage of the index `my-index-000001`:

```console
POST /my-index-000001/_disk_usage?run_expensive_tasks=true
```
% TEST[setup:messages]

## Example response

The API returns:

```console-response
{
    "_shards": {
        "total": 1,
        "successful": 1,
        "failed": 0
    },
    "my-index-000001": {
        "store_size": "929mb", <1>
        "store_size_in_bytes": 974192723,
        "all_fields": {
            "total": "928.9mb", <2>
            "total_in_bytes": 973977084,
            "inverted_index": {
                "total": "107.8mb",
                "total_in_bytes": 113128526
            },
            "stored_fields": "623.5mb",
            "stored_fields_in_bytes": 653819143,
            "doc_values": "125.7mb",
            "doc_values_in_bytes": 131885142,
            "points": "59.9mb",
            "points_in_bytes": 62885773,
            "norms": "2.3kb",
            "norms_in_bytes": 2356,
            "term_vectors": "2.2kb",
            "term_vectors_in_bytes": 2310,
            "knn_vectors": "0b",
            "knn_vectors_in_bytes": 0
        },
        "fields": {
            "_id": {
                "total": "49.3mb",
                "total_in_bytes": 51709993,
                "inverted_index": {
                    "total": "29.7mb",
                    "total_in_bytes": 31172745
                },
                "stored_fields": "19.5mb", <3>
                "stored_fields_in_bytes": 20537248,
                "doc_values": "0b",
                "doc_values_in_bytes": 0,
                "points": "0b",
                "points_in_bytes": 0,
                "norms": "0b",
                "norms_in_bytes": 0,
                "term_vectors": "0b",
                "term_vectors_in_bytes": 0,
                "knn_vectors": "0b",
                "knn_vectors_in_bytes": 0
            },
            "_primary_term": {...},
            "_seq_no": {...},
            "_version": {...},
            "_source": {
                "total": "603.9mb",
                "total_in_bytes": 633281895,
                "inverted_index": {...},
                "stored_fields": "603.9mb", <4>
                "stored_fields_in_bytes": 633281895,
                "doc_values": "0b",
                "doc_values_in_bytes": 0,
                "points": "0b",
                "points_in_bytes": 0,
                "norms": "0b",
                "norms_in_bytes": 0,
                "term_vectors": "0b",
                "term_vectors_in_bytes": 0,
                "knn_vectors": "0b",
                "knn_vectors_in_bytes": 0
            },
            "context": {
                "total": "28.6mb",
                "total_in_bytes": 30060405,
                "inverted_index": {
                    "total": "22mb",
                    "total_in_bytes": 23090908
                },
                "stored_fields": "0b",
                "stored_fields_in_bytes": 0,
                "doc_values": "0b",
                "doc_values_in_bytes": 0,
                "points": "0b",
                "points_in_bytes": 0,
                "norms": "2.3kb",
                "norms_in_bytes": 2356,
                "term_vectors": "2.2kb",
                "term_vectors_in_bytes": 2310,
                "knn_vectors": "0b",
                "knn_vectors_in_bytes": 0
            },
            "context.keyword": {...},
            "message": {...},
            "message.keyword": {...}
        }
    }
}
```
% TESTRESPONSE[s/: \{\.\.\.\}/: $body.$_path/]
% TESTRESPONSE[s/: (\-)?[0-9]+/: $body.$_path/]
% TESTRESPONSE[s/: "[^"]*"/: $body.$_path/]

1. The total disk space used by the shards analyzed by the API. By default, only primary shards are analyzed.

2. The total disk space used by all fields in the analyzed shards. This total is usually smaller than the `store_size` annotated in <1>, because the API ignores some metadata files.

3. The disk space used by `_id` field values for direct document retrieval. This storage enables [getting a document by its ID]({{es-apis}}operation/operation-get), without needing to search the inverted index.

4. The disk space used by the `_source` field. As stored fields are stored
together in a compressed format, the sizes of stored fields are
estimates and can be inaccurate. The stored size of the `_id` field
is likely underestimated while the `_source` field is overestimated.