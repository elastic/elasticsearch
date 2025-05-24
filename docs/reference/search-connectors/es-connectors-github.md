---
navigation_title: "GitHub"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-github.html
---

# Elastic GitHub connector reference [es-connectors-github]


The *Elastic GitHub connector* is a [connector](/reference/search-connectors/index.md) for [GitHub](https://www.github.com). This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/github.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::


## **Self-managed connector** [es-connectors-github-connector-client-reference]

### Availability and prerequisites [es-connectors-github-client-availability-prerequisites]

This connector is available as a self-managed connector.

This self-managed connector is compatible with Elastic versions **8.10.0+**.

To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a GitHub connector [es-connectors-github-create-connector-client]


#### Use the UI [es-connectors-github-client-create-use-the-ui]

To create a new GitHub connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **GitHub** self-managed connector.


#### Use the API [es-connectors-github-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed GitHub self-managed connector.

For example:

```console
PUT _connector/my-github-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from GitHub",
  "service_type": "github"
}
```
% TEST[skip:can’t test in isolation]

:::::{dropdown} You’ll also need to create an API key for the connector to use.
::::{note}
The user needs the cluster privileges `manage_api_key`, `manage_connector` and `write_connector_secrets` to generate API keys programmatically.

::::


To create an API key for the connector:

1. Run the following command, replacing values where indicated. Note the `encoded` return values from the response:

    ```console
    POST /_security/api_key
    {
      "name": "connector_name-connector-api-key",
      "role_descriptors": {
        "connector_name-connector-role": {
          "cluster": [
            "monitor",
            "manage_connector"
          ],
          "indices": [
            {
              "names": [
                "index_name",
                ".search-acl-filter-index_name",
                ".elastic-connectors*"
              ],
              "privileges": [
                "all"
              ],
              "allow_restricted_indices": false
            }
          ]
        }
      }
    }
    ```

2. Update your `config.yml` file with the API key `encoded` value.

:::::


Refer to the [{{es}} API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) for details of all available Connector APIs.


### Usage [es-connectors-github-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


#### GitHub personal access token [es-connectors-github-client-personal-access-token]

Configure a GitHub personal access token to fetch data from GitHub.

Follow these steps to generate a GitHub  access token:

* Go to **GitHub Settings → Developer settings → Personal access tokens → Tokens(classic)**.
* Select `Generate new token`.
* Add a note and select the following scopes:

    * `repo`
    * `user`
    * `read:org`

* Select `Generate token` and copy the token.


#### GitHub App [es-connectors-github-client-github-app]

Configure a GitHub App to fetch data from GitHub.

Follow these steps to create a GitHub App:

* Go to **GitHub Settings → Developer settings → GitHub Apps**.
* Select `New GitHub App`.
* Add a name and Homepage URL, deselect `Active` under `Webhook`.
* Under `Permissions`, select `Read-only` for `Commit statuses`, `Contents`, `Issues`, `Metadata` and `Pull requests` under `Repository permissions`, select `Read-only` for `Members` under `Organization permissions`.
* Select `Any account` for `Where can this GitHub App be installed?`.
* Click `Create GitHub App`.
* Scroll down to the section `Private keys`, and click `Generate a private key`.
* Click `Install App` in the upper-left corner, select the organizations/personal accounts you want to install the GitHub App on, click `Install`.
* You can choose to install it on all repositories or selected repositories, and click `Install`.


### Compatibility [es-connectors-github-client-compatability]

Both GitHub and GitHub Enterprise are supported.


### Configuration [es-connectors-github-client-configuration]

The following configuration fields are required:

`data_source`
:   GitHub Cloud or GitHub Server.

`host`
:   URL of the GitHub Server instance. (GitHub Server only)

`auth_method`
:   The method to authenticate the GitHub instance. Toggle between `Personal access token` and `GitHub App`.

`token`
:   GitHub personal access token to authenticate the GitHub instance.  This field is only available for `Personal access token` authentication method.

`repo_type`
:   Toggle between `Organization` and `Other`. Note that document level security (DLS) is only available for `Organization` repositories.

`org_name`
:   Name of the organization to fetch data from. This field is only available when `Authentication method` is set to `Personal access token` and `Repository Type` is set to `Organization`.

`app_id`
:   App ID of the GitHub App. This field is only available when `Authentication method` is set to `GitHub App`.

`private_key`
:   Private key generated for the GitHub App. This field is only available when `Authentication method` is set to `GitHub App`.

`repositories`
:   Comma-separated list of repositories to fetch data from GitHub instance. If the value is `*` the connector will fetch data from all repositories present in the configured user’s account.

    Default value is `*`.

    Examples:

    * `elasticsearch`,`elastic/kibana`
    * `*`


::::{tip}
**Repository ownership**

If the "OWNER/" portion of the "OWNER/REPO" repository argument is omitted, it defaults to the name of the authenticating user.

In the examples provided here:

* the `elasticsearch` repo synced will be the `<OWNER>/elasticsearch`
* the `kibana` repo synced will be the Elastic owned repo

The "OWNER/" portion of the "OWNER/REPO" repository argument must be provided when `GitHub App` is selected as the `Authentication method`.

::::


::::{note}
This field can be bypassed by advanced sync rules.

::::


`ssl_enabled`
:   Whether SSL verification will be enabled. Default value is `False`.

`ssl_ca`
:   Content of SSL certificate. Note: If `ssl_enabled` is `False`, the value in this field is ignored. Example certificate:

    ```txt
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```


`use_document_level_security`
:   Toggle to enable [document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled, full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. DLS is only available when `Repository Type` is set to `Organization`.

`retry_count`
:   The number of retry attempts after failed request to GitHub. Default value is `3`.

`use_text_extraction_service`
:   Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that pipeline settings disable text extraction. Default value is `False`.


### Deployment using Docker [es-connectors-github-client-docker]

You can deploy the GitHub connector as a self-managed connector using Docker. Follow these instructions.

::::{dropdown} Step 1: Download sample configuration file
Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example --output ~/connectors-config/config.yml
```
% NOTCONSOLE

Remember to update the `--output` argument value if your directory name is different, or you want to use a different config file name.

::::


::::{dropdown} Step 2: Update the configuration file for your self-managed connector
Update the configuration file with the following settings to match your environment:

* `elasticsearch.host`
* `elasticsearch.api_key`
* `connectors`

If you’re running the connector service against a Dockerized version of Elasticsearch and Kibana, your config file will look like this:

```yaml
# When connecting to your cloud deployment you should edit the host value
elasticsearch.host: http://host.docker.internal:9200
elasticsearch.api_key: <ELASTICSEARCH_API_KEY>

connectors:
  -
    connector_id: <CONNECTOR_ID_FROM_KIBANA>
    service_type: github
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

Using the `elasticsearch.api_key` is the recommended authentication method. However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

Note: You can change other default configurations by simply uncommenting specific settings in the configuration file and modifying their values.

::::


::::{dropdown} Step 3: Run the Docker image
Run the Docker image with the Connector Service using the following command:

```sh
docker run \
-v ~/connectors-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors:9.0.0 \
/app/bin/elastic-ingest \
-c /config/config.yml
```

::::


Refer to [`DOCKER.md`](https://github.com/elastic/connectors/tree/main/docs/DOCKER.md) in the `elastic/connectors` repo for more details.

Find all available Docker images in the [official registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).

::::{tip}
We also have a quickstart self-managed option using Docker Compose, so you can spin up all required services at once: Elasticsearch, Kibana, and the connectors service. Refer to this [README](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.

::::



### Documents and syncs [es-connectors-github-client-documents-syncs]

The connector syncs the following objects and entities:

* **Repositories**
* **Pull Requests**
* **Issues**
* **Files & Folder**

Only the following file extensions are ingested:

* `.markdown`
* `.md`
* `.rst`

::::{note}
* Content of files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elasticsearch Index.

::::



#### Sync types [es-connectors-github-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-github-client-sync-rules]

*Basic* sync rules are identical for all connectors and are available by default. For more information read [Types of sync rule](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


#### Advanced sync rules [es-connectors-github-client-sync-rules-advanced]

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


The following section describes **advanced sync rules** for this connector. Advanced sync rules are defined through a source-specific DSL JSON snippet.

The following sections provide examples of advanced sync rules for this connector.

$$$es-connectors-github-client-sync-rules-advanced-branch$$$
**Indexing document and files based on branch name configured via branch key**

```js
[
  {
    "repository": "repo_name",
    "filter": {
      "branch": "sync-rules-feature"
    }
  }
]
```
% NOTCONSOLE

$$$es-connectors-github-client-sync-rules-advanced-issue-key$$$
**Indexing document based on issue query related to bugs via issue key**

```js
[
  {
    "repository": "repo_name",
    "filter": {
      "issue": "is:bug"
    }
  }
]
```
% NOTCONSOLE

$$$es-connectors-github-client-sync-rules-advanced-pr-key$$$
**Indexing document based on PR query related to open PR’s via PR key**

```js
[
  {
    "repository": "repo_name",
    "filter": {
      "pr": "is:open"
    }
  }
]
```
% NOTCONSOLE

$$$es-connectors-github-client-sync-rules-advanced-issue-query-branch-name$$$
**Indexing document and files based on queries and branch name**

```js
[
  {
    "repository": "repo_name",
    "filter": {
      "issue": "is:bug",
      "pr": "is:open",
      "branch": "sync-rules-feature"
    }
  }
]
```
% NOTCONSOLE

::::{note}
All documents pulled by a given rule are indexed regardless of whether the document has already been indexed by a previous rule. This can lead to document duplication, but the indexed documents count will differ in the logs. Check the Elasticsearch index for the actual document count.

::::


$$$es-connectors-github-client-sync-rules-advanced-overlapping$$$
**Advanced rules for overlapping**

```js
[
  {
    "filter": {
      "pr": "is:pr is:merged label:auto-backport merged:>=2023-07-20"
    },
    "repository": "repo_name"
  },
  {
    "filter": {
      "pr": "is:pr is:merged label:auto-backport merged:>=2023-07-15"
    },
    "repository": "repo_name"
  }
]
```
% NOTCONSOLE

::::{note}
If `GitHub App` is selected as the authentication method, the  "OWNER/" portion of the "OWNER/REPO" repository argument must be provided.

::::



### Content Extraction [es-connectors-github-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Self-managed connector operations [es-connectors-github-client-connector-client-operations]


### End-to-end testing [es-connectors-github-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the GitHub connector, run the following command:

```shell
$ make ftest NAME=github
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=github DATA_SIZE=small
```


### Known issues [es-connectors-github-client-known-issues]

There are currently no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-github-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-github-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).