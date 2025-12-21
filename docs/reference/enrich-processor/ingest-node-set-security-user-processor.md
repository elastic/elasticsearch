---
navigation_title: "Set security user"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ingest-node-set-security-user-processor.html
---

# Set security user processor [ingest-node-set-security-user-processor]


Sets user-related details (such as `username`, `roles`, `email`, `full_name`, `metadata`, `api_key`, `realm` and `authentication_type`) from the current authenticated user. The `api_key` property exists only if the user authenticates with an API key. It is an object containing the `id`, `name` and `metadata` (if it exists and is non-empty) fields of the API key. The `realm` property is also an object with two fields, `name` and `type`. When using API key authentication, the `realm` property refers to the realm from which the API key is created. The `authentication_type` property is a string that can take value from `REALM`, `API_KEY`, `TOKEN` and `ANONYMOUS`.

::::{important}
Requires an authenticated user for the index request.
::::


$$$set-security-user-options$$$

| Name | Required | Default | Description |
| --- | --- | --- | --- |
| `field` | yes | - | The field to store the user information into. |
| `properties` | no | [`username`, `roles`, `email`, `full_name`, `metadata`, `api_key`, `realm`, `authentication_type`] | Controls what user related properties are added to the `field`. |
| `description` | no | - | Description of the processor. Useful for describing the purpose of the processor or its configuration. |
| `if` | no | - | Conditionally execute the processor. See [Conditionally run a processor](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#conditionally-run-processor). |
| `ignore_failure` | no | `false` | Ignore failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `on_failure` | no | - | Handle failures for the processor. See [Handling pipeline failures](docs-content://manage-data/ingest/transform-enrich/ingest-pipelines.md#handling-pipeline-failures). |
| `tag` | no | - | Identifier for the processor. Useful for debugging and metrics. |

The following example adds all user details for the current authenticated user to the `user` field for all documents that are processed by this pipeline:

```js
{
  "processors" : [
    {
      "set_security_user": {
        "field": "user"
      }
    }
  ]
}
```
% NOTCONSOLE

