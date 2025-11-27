---
navigation_title: "Semantic text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/semantic-text.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Semantic text field type [semantic-text]

The `semantic_text` field type automatically generates embeddings for text content using an [inference endpoint](./semantic-text-reference.md#configuring-inference-endpoints). 
Using `semantic_text`, you don't need to specify how to generate embeddings for your data, or how to index it. The inference endpoint automatically determines the embedding generation, indexing, and query to use. 

## Overview

The `semantic_text` field type documentation is organized into reference content and how-to guides. 

The [Reference](./semantic-text-reference.md) page provides parameter descriptions, explanations of concepts, and technical specifications:

- [Parameters](./semantic-text-reference.md#semantic-text-params): Parameter descriptions for `semantic_text` fields.

- [Inference endpoints](./semantic-text-reference.md#configuring-inference-endpoints): Overview of inference endpoints used with `semantic_text` fields.

- [Chunking](./semantic-text-reference.md#chunking-behavior): Overview of how `semantic_text` automatically processes long text passages by generating smaller chunks.

- [Updates and partial updates](./semantic-text-reference.md#updates-and-partial-updates): Behavior differences between full document updates, partial updates using the Bulk API, and partial updates using the Update API, including scripted update support.

- [Document count discrepancy](./semantic-text-reference.md#document-count-discrepancy): Explanation of why `_cat/indices` may show higher document counts due to nested documents used internally for storing embeddings.

- [Querying](./semantic-text-reference.md#querying-semantic-text-fields): Supported query types for `semantic_text` fields.

- [Limitations](./semantic-text-reference.md#limitations): Current restrictions and unsupported features for `semantic_text` fields.

The [How-to guides](./how-to-semantic-text.md) page contains procedure descriptions and examples for common tasks and configurations:

- [Configuring inference endpoints](./how-to-semantic-text.md#configure-inference-endpoints): Learn how to use [default and preconfigured endpoints](./how-to-semantic-text.md#default-and-preconfigured-endpoints), [ELSER on EIS](./how-to-semantic-text.md#using-elser-on-eis), and [custom inference endpoints](./how-to-semantic-text.md#using-custom-endpoint).

- [Setting up dedicated endpoints](./how-to-semantic-text.md#dedicated-endpoints-for-ingestion-and-search): Learn how to configure separate inference endpoints for ingestion and search operations.

- [Pre-chunking content](./how-to-semantic-text.md#pre-chunking): Learn how to provide pre-chunked text as arrays and [retrieve indexed chunks](./how-to-semantic-text.md#retrieving-indexed-chunks).

- [Returning embeddings](./how-to-semantic-text.md#returning-semantic-field-embeddings): Learn how to return `semantic_text` field embeddings in [`_source`](./how-to-semantic-text.md#returning-semantic-field-embeddings-in-_source) (for {{es}} 9.2+) or using the [`fields` parameter](./how-to-semantic-text.md#returning-semantic-field-embeddings-using-fields) (for earlier versions).

- [Highlighting fragments](./how-to-semantic-text.md#highlighting-fragments): Learn how to extract the most relevant text fragments from `semantic_text` fields.

- [Cross-cluster search](./how-to-semantic-text.md#cross-cluster-search): Learn how to perform cross-cluster searches on `semantic_text` fields.

- [Using `copy_to` and multi-fields](./how-to-semantic-text.md#use-copy-to-with-semantic-text): Learn how to combine multiple fields into a single `semantic_text` field using `copy_to` or multi-field mappings.











