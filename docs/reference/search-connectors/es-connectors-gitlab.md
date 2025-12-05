---
applies_to:
  stack: preview 9.3
  serverless: preview
navigation_title: "GitLab"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-gitlab.html
---

# Elastic GitLab connector reference [es-connectors-gitlab]


The *Elastic GitLab connector* is a [connector](/reference/search-connectors/index.md) for [GitLab](https://www.gitlab.com). This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/gitlab) (branch *main*, compatible with Elastic *9.3*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::


## **Self-managed connector** [es-connectors-gitlab-connector-client-reference]

### Availability and prerequisites [es-connectors-gitlab-client-availability-prerequisites]

This connector is available as a self-managed connector.

This self-managed connector is compatible with Elastic versions **9.3.0+**.

To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a GitLab connector [es-connectors-gitlab-create-connector-client]


#### Use the UI [es-connectors-gitlab-client-create-use-the-ui]

To create a new GitLab connector:

1. In the Kibana UI, search for "connectors" using the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects) and choose the "Elasticsearch" connectors.
2. Follow the instructions to create a new  **GitLab** self-managed connector.


#### Use the API [es-connectors-gitlab-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed GitLab self-managed connector.

For example:

```console
PUT _connector/my-gitlab-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from GitLab",
  "service_type": "gitlab"
}
```
% TEST[skip:can't test in isolation]

:::::{dropdown} You'll also need to create an API key for the connector to use.
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


### Usage [es-connectors-gitlab-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


#### GitLab personal access token [es-connectors-gitlab-client-personal-access-token]

Configure a GitLab personal access token to fetch data from GitLab.

Follow these steps to generate a GitLab personal access token:

* Go to **User Settings → Access Tokens** (or for project tokens: **Settings → Access Tokens → Project Access Tokens**).
* Click `Add new token`.
* Enter a token name and optional expiration date.
* Select the following scopes:
    * `api` - Required for GraphQL API access (note: the connector only performs read-only operations)
    * `read_api` - Grants read access to REST API endpoints
    * `read_repository` - Grants read access to repository files

* Click `Create personal access token` and copy the token.


### Compatibility [es-connectors-gitlab-client-compatibility]

This connector supports GitLab Cloud (gitlab.com) only. GitLab Self-Managed instances are not currently supported.


### Configuration [es-connectors-gitlab-client-configuration]

The following configuration fields are required:

`token`
:   GitLab personal access token to authenticate with the GitLab instance. The token must have `api`, `read_api`, and `read_repository` scopes.

`projects`
:   List of project paths to sync (e.g., `group/project`, `username/project`). Use `*` or leave empty (`[]`) to sync all projects where the token's user is a member.

    Default value is `[]` (empty list, which syncs all projects).

    Examples:

    * `elastic/elasticsearch`, `elastic/kibana`
    * `*`
    * `[]` (syncs all projects)


::::{tip}
**Project path format**

Projects should be specified using their full path including the namespace (group or username).

In the examples provided here:

* `elastic/elasticsearch` syncs the Elasticsearch project from the elastic group
* `elastic/kibana` syncs the Kibana project from the elastic group


::::


### Deployment using Docker [es-connectors-gitlab-client-docker]

You can deploy the GitLab connector as a self-managed connector using Docker. Follow these instructions.

::::{dropdown} Step 1: Download sample configuration file
Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/app/connectors_service/config.yml.example --output ~/connectors-config/config.yml
```
% NOTCONSOLE

Remember to update the `--output` argument value if your directory name is different, or you want to use a different config file name.

::::


::::{dropdown} Step 2: Update the configuration file for your self-managed connector
Update the configuration file with the following settings to match your environment:

* `elasticsearch.host`
* `elasticsearch.api_key`
* `connectors`

If you're running the connector service against a Dockerized version of Elasticsearch and Kibana, your config file will look like this:

```yaml
# When connecting to your cloud deployment you should edit the host value
elasticsearch.host: http://host.docker.internal:9200
elasticsearch.api_key: <ELASTICSEARCH_API_KEY>

connectors:
  -
    connector_id: <CONNECTOR_ID_FROM_KIBANA>
    service_type: gitlab
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

Using the `elasticsearch.api_key` is the recommended authentication method. However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

Note: You can change other default configurations by simply uncommenting specific settings in the configuration file and modifying their values.

::::


::::{dropdown} Step 3: Run the Docker image
Run the Docker image with the Connector Service using the following command:

```sh subs=true
docker run \
-v ~/connectors-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors:{{version.stack}} \
/app/bin/elastic-ingest \
-c /config/config.yml
```

::::


Refer to [`DOCKER.md`](https://github.com/elastic/connectors/tree/main/docs/DOCKER.md) in the `elastic/connectors` repo for more details.

Find all available Docker images in the [official registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).

::::{tip}
We also have a quickstart self-managed option using Docker Compose, so you can spin up all required services at once: Elasticsearch, Kibana, and the connectors service. Refer to this [README](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.

::::



### Documents and syncs [es-connectors-gitlab-client-documents-syncs]

The connector syncs the following objects and entities:

* **Projects**
* **Issues** (using Work Items API)
* **Merge Requests**
* **Epics** (using Work Items API, group-level, requires Premium/Ultimate tier)
* **Releases** (project-level version releases with changelogs)
* **README Files** (.md, .rst, .txt)

Only the following file extensions are ingested for README files:

* `.md`
* `.rst`
* `.txt`

::::{note}
* Content of files bigger than 10 MB won't be extracted.
* Epics are only available for Premium/Ultimate GitLab tiers and are synced at the group level.
* **Epic syncing behavior**: Epics are fetched only for groups that contain synced projects.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elasticsearch Index.

::::


#### Sync types [es-connectors-gitlab-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector does not currently support [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-gitlab-client-sync-rules]

*Basic* sync rules are identical for all connectors and are available by default. For more information read [Types of sync rule](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


#### Advanced sync rules [es-connectors-gitlab-client-sync-rules-advanced]

Advanced sync rules are not currently supported for this connector. This feature may be added in future releases.


### Content Extraction [es-connectors-gitlab-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Self-managed connector operations [es-connectors-gitlab-client-connector-client-operations]


### End-to-end testing [es-connectors-gitlab-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the GitLab connector, run the following command:

```shell
$ make ftest NAME=gitlab
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=gitlab DATA_SIZE=small
```


### Known issues [es-connectors-gitlab-client-known-issues]

There are currently no known issues for this connector. Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-gitlab-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-gitlab-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).
