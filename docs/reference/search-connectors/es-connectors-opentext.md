---
navigation_title: "OpenText Documentum"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-opentext.html
---

# Elastic OpenText Documentum connector reference [es-connectors-opentext]


::::{warning}
This connector is an **example connector** that serves as a building block for customizations and is subject to change. Its source code currently lives on a [feature branch](https://github.com/elastic/connectors/blob/opentext-connector-backup/connectors/sources/opentext_documentum.py) and is yet not part of the main Elastic Connectors codebase. The design and code is less mature than supported features and is being provided as-is with no warranties. This connector is not subject to the support SLA of supported features.

::::


The Elastic OpenText Documentum connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main?tab=readme-ov-file#connector-framework). View the [source code](https://github.com/elastic/connectors/blob/opentext-connector-backup/connectors/sources/opentext_documentum.py) for this example connector.


## Availability and prerequisites [es-connectors-opentext-documentum-connector-availability-and-prerequisites]

This **example connector** was introduced in Elastic **8.14.0**, available as a **self-managed** self-managed connector on a feature branch, for testing and development purposes only.

To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md). Importantly, you must deploy the connectors service on your own infrastructure. You have two deployment options:

* [Run connectors service from source](/reference/search-connectors/es-connectors-run-from-source.md). Use this option if you’re comfortable working with Python and want to iterate quickly locally.
* [Run connectors service in Docker](/reference/search-connectors/es-connectors-run-from-docker.md). Use this option if you want to deploy the connectors to a server, or use a container orchestration platform.


## Usage [es-connectors-opentext-documentum-connector-usage]

To set up this connector in the UI, select the **OpenText Documentum** tile when creating a new connector under **Search → Connectors**.

If you’re already familiar with how connectors work, you can also use the [Connector APIs](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector).

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


## Connecting to OpenText Documentum [es-connectors-opentext-documentum-connector-connecting-to-opentext-documentum]

Basic authentication is used to connect with OpenText Documentum.


## Configuration [es-connectors-opentext-documentum-connector-configuration]


### Configure OpenText Documentum connector [es-connectors-opentext-documentum-connector-configure-opentext-documentum-connector]

Note the following configuration fields:

`OpenText Documentum host url` (required)
:   The domain where OpenText Documentum is hosted. Example: `https://192.158.1.38:2099/`

`Username` (required)
:   The username of the account to connect to OpenText Documentum.

`Password` (required)
:   The password of the account to connect to OpenText Documentum.

`Repositories` (optional)
:   Comma-separated list of repositories to fetch data from OpenText Documentum. If the value is `*` the connector will fetch data from all repositories present in the configured user’s account.

    Default value is `*`.

    Examples:

    * `elastic`, `kibana`
    * `*`


`Enable SSL` (optional)
:   Enable SSL for the OpenText Documentum instance.

`SSL Certificate` (Required if SSL is enabled)
:   SSL certificate for the OpenText Documentum instance. Example:

    ```
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```



### Content Extraction [es-connectors-opentext-documentum-connector-content-extraction]

Refer to [content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


## Documents and syncs [es-connectors-opentext-documentum-connector-documents-and-syncs]

The connector syncs the following objects and entities:

* **Repositories**
* **Cabinets**
* **Files & Folders**

::::{note}
* Files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to the destination Elasticsearch index.

::::



### Sync types [es-connectors-opentext-documentum-connector-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

[Incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental) are not available for this connector in the present version.


## Sync rules [es-connectors-opentext-documentum-connector-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version.


## Connector Client operations [es-connectors-opentext-documentum-connector-connector-client-operations]


### End-to-end Testing [es-connectors-opentext-documentum-connector-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source, using Docker Compose. You don’t need a running Elasticsearch instance or OpenText Documentum source to run this test.

Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the OpenText Documentum connector, run the following command:

```shell
$ make ftest NAME=opentext_documentum
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=opentext_documentum DATA_SIZE=small
```

By default, `DATA_SIZE=MEDIUM`.


## Known issues [es-connectors-opentext-documentum-connector-known-issues]

* There are no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


## Troubleshooting [es-connectors-opentext-documentum-connector-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


## Security [es-connectors-opentext-documentum-connector-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

