---
navigation_title: "Costs"
description: "Understand the costs of querying external data with ES|QL Data Federation, from your storage provider and from Elastic."
applies_to:
  stack: preview 9.6+
  serverless: unavailable
products:
  - id: elasticsearch
---

# Costs of {{esql}} Data Federation

{{esql}} Data Federation queries data where it lives instead of copying it into {{es}}. Running these queries can incur costs from two places: your storage provider and Elastic.

## Storage provider costs [storage-provider-costs]

Your storage provider bills for the requests and data transfer that each query generates. These charges apply whether you run queries through the API or in {{kib}}, and on every deployment type.

To understand, estimate, and reduce these charges, refer to the page for your storage provider:

- [](esql-data-federation-costs-s3.md)

## Elastic costs [elastic-costs]

Elastic doesn't charge separately for Data Federation queries during the technical preview.

On {{ech}}, federated queries use your deployment's capacity and data transfer like any other query. For details, refer to [Cloud Hosted deployment billing dimensions](docs-content://deploy-manage/cloud-organization/billing/cloud-hosted-deployment-billing-dimensions.md).
