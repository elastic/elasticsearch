---
navigation_title: "Recover Failure Document"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/recover_failure_document-processor.html
---

# Recover Failure Document processor [recover_failure_document-processor]

Recovers documents that have been stored in a data stream's failure store by restoring them to their original format. This processor is designed to work with documents that failed during ingestion and were automatically stored in the failure store with additional error metadata and document structure wrapping. The relevant failure store metadata is stashed under _ingest.pre_recovery.

The Recover Failure Document processor performs the following operations:

* Checks the document is a valid failure store document.
* Stores the pre-recovery metadata of the document (all document fields except for `document.source`) under the ingest metadata _ingest.pre_recovery.
* Overwrites `_source` with the original document source from the `document.source` field
* Restores the original document id from `document._id` to the document metadata
* Restores the original index name from `document.index` to the document metadata
* Restores the original routing value from `document.routing` to the document metadata (if present)

$$$recover_failure_document-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

## Examples [recover_failure_document-processor-ex]

```console
POST _ingest/pipeline/_simulate
{
    "pipeline": {
        "processors": [
            {
                "recover_failure_document": {}
            }
        ]
    },
    "docs": [
        {
            "_index": ".fs-my-datastream-ingest-2025.05.09-000001",
            "_id": "HnTJs5YBwrYNjPmaFcri",
            "_score": 1,
            "_source": {
                "@timestamp": "2025-05-09T06:41:24.775Z",
                "document": {
                    "index": "my-datastream-ingest",
                    "source": {
                        "@timestamp": "2025-04-21T00:00:00Z",
                        "counter_name": "test"
                    }
                },
                "error": {
                    "type": "illegal_argument_exception",
                    "message": "field [counter] not present as part of path [counter]",
                    "stack_trace": "j.l.IllegalArgumentException: field [counter] not present as part of path [counter] at o.e.i.IngestDocument.getFieldValue(IngestDocument.java: 202 at o.e.i.c.SetProcessor.execute(SetProcessor.java: 86) 14 more",
                    "pipeline_trace": [
                        "complicated-processor"
                    ],
                    "pipeline": "complicated-processor",
                    "processor_type": "set",
                    "processor_tag": "copy to new counter again"
                }
            }
        }
    ]
}
```

Which produces the following response:

```console-result
{
    "docs": [
        {
            "doc": {
                "_index": "my-datastream-ingest",
                "_version": "-3",
                "_id": "HnTJs5YBwrYNjPmaFcri",
                "_source": {
                    "@timestamp": "2025-04-21T00:00:00Z",
                    "counter_name": "test"
                },
                "_ingest": {
                    "pre_recovery": {
                        "@timestamp": "2025-05-09T06:41:24.775Z",
                        "_index": ".fs-my-datastream-ingest-2025.05.09-000001",
                        "document": {
                            "index": "my-datastream-ingest"
                        },
                        "_id": "HnTJs5YBwrYNjPmaFcri",
                        "error": {
                            "pipeline": "complicated-processor",
                            "processor_type": "set",
                            "processor_tag": "copy to new counter again",
                            "pipeline_trace": [
                                "complicated-processor"
                            ],
                            "stack_trace": "j.l.IllegalArgumentException: field [counter] not present as part of path [counter] at o.e.i.IngestDocument.getFieldValue(IngestDocument.java: 202 at o.e.i.c.SetProcessor.execute(SetProcessor.java: 86) 14 more",
                            "type": "illegal_argument_exception",
                            "message": "field [counter] not present as part of path [counter]"
                        },
                        "_version": -3
                    },
                    "timestamp": "2025-09-04T22:32:12.800709Z"
                }
            }
        }
    ]
}
```
% TESTRESPONSE[s/"timestamp": "2025-10-22T00:08:54.934231755Z"/"timestamp": $body.docs.0.doc._ingest.timestamp/]

Documents which do not match the failure store document format result in errors:

```console
POST _ingest/pipeline/_simulate
{
    "pipeline": {
        "processors": [
            {
                "recover_failure_document": {}
            }
        ]
    },
    "docs": [
        {
            "_index": ".fs-my-datastream-ingest-2025.05.09-000001",
            "_id": "HnTJs5YBwrYNjPmaFcri",
            "_score": 1,
            "_source": {
                "@timestamp": "2025-05-09T06:41:24.775Z",
                "error": {
                    "type": "illegal_argument_exception",
                    "message": "field [counter] not present as part of path [counter]",
                    "stack_trace": "j.l.IllegalArgumentException: field [counter] not present as part of path [counter] at o.e.i.IngestDocument.getFieldValue(IngestDocument.java: 202 at o.e.i.c.SetProcessor.execute(SetProcessor.java: 86) 14 more",
                    "pipeline_trace": [
                        "complicated-processor"
                    ],
                    "pipeline": "complicated-processor",
                    "processor_type": "set",
                    "processor_tag": "copy to new counter again"
                }
            }
        }
    ]
}
```
Which produces the following response:

```console-result
{
    "docs": [
        {
            "error": {
                "root_cause": [
                    {
                        "type": "illegal_argument_exception",
                        "reason": "field [document] not present as part of path [document]"
                    }
                ],
                "type": "illegal_argument_exception",
                "reason": "field [document] not present as part of path [document]"
            }
        }
    ]
}
```
