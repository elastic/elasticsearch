---
navigation_title: "Google Drive"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-google-drive.html
---

# Elastic Google Drive connector reference [es-connectors-google-drive]


The *Elastic Google Drive connector* is a [connector](/reference/search-connectors/index.md) for [Google Drive](https://www.google.com/drive). This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/google_drive.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-google-drive-connector-client-reference]

### Availability and prerequisites [es-connectors-google-drive-client-availability-and-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Usage [es-connectors-google-drive-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional usage operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Connector authentication prerequisites [es-connectors-google-drive-client-connector-authentication-prerequisites]

Before syncing any data from Google Drive, you need to create a [service account](https://cloud.google.com/iam/docs/service-account-overview) with appropriate access to Google Drive API.

To get started, log into [Google Cloud Platform](https://cloud.google.com) and go to the `Console`.

1. **Create a Google Cloud Project.** Give your project a name, change the project ID and click the Create button.
2. **Enable Google APIs.** Choose APIs & Services from the left menu and click on `Enable APIs and Services`. You need to enable the **Drive API**.
3. **Create a Service Account.** In the `APIs & Services` section, click on `Credentials` and click on `Create credentials` to create a service account. Give your service account a name and a service account ID. This is like an email address and will be used to identify your service account in the future. Click `Done` to finish creating the service account. Your service account needs to have access to at least the following scope:

    * `https://www.googleapis.com/auth/drive.readonly`

4. **Create a Key File**.

    * In the Cloud Console, go to `IAM and Admin` > `Service accounts` page.
    * Click the email address of the service account that you want to create a key for.
    * Click the `Keys` tab. Click the `Add key` drop-down menu, then select `Create new key`.
    * Select JSON as the Key type and then click `Create`. This will download a JSON file that will contain the service account credentials.

5. **[Optional] Share Google Drive Folders.** If you use domain-wide delegation for syncing data you can skip this step. Go to your Google Drive. Right-click the folder or shared drive, choose `Share` and add the email address of the service account you created in step 3. as a viewer to this folder.

::::{note}
When you grant a service account access to a specific folder or shared drive in Google Drive, it’s important to note that the permissions extend to all the children within that folder or drive. This means that any folders or files contained within the granted folder or drive inherit the same access privileges as the parent.

::::



#### Additional authentication prerequisites for domain-wide delegation [es-connectors-google-drive-client-additional-prerequisites-for-domain-wide-delegation]

This step is **required** when **Use domain-wide delegation for data sync** or **Enable document level security** configuration option is enabled.

1. **Enable Google APIs**.

    Choose APIs & Services from the left menu and click on `Enable APIs and Services`. You need to enable the **Admin SDK API** and **Drive API**.

2. **Google Workspace domain-wide delegation of authority**.

    To access drive and user data in a Google Workspace domain, the service account that you created needs to be granted access by a super administrator for the domain. You can follow [the official documentation](https://developers.google.com/cloud-search/docs/guides/delegation) to perform Google Workspace domain-wide delegation of authority.

    You need to grant the following **OAuth Scopes** to your service account:

    * `https://www.googleapis.com/auth/admin.directory.group.readonly`
    * `https://www.googleapis.com/auth/admin.directory.user.readonly`
    * `https://www.googleapis.com/auth/drive.readonly`
    * `https://www.googleapis.com/auth/drive.metadata.readonly`

    This step allows the connector to:

    * access user data and their group memberships in a Google Workspace organization
    * access Google Drive data in drives associated to Google Workspace members



### Configuration [es-connectors-google-drive-client-configuration]




The following configuration fields are required:

`service_account_credentials`
:   The service account credentials generated from Google Cloud Platform (JSON string). Refer to the [Google Cloud documentation](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) for more information.

`use_domain_wide_delegation_for_sync`
:   Use [domain-wide delegation](https://developers.google.com/cloud-search/docs/guides/delegation) to automatically sync content from all shared and personal drives in the Google workspace. This eliminates the need to manually share Google Drive data with your service account, though it may increase the sync time. If disabled, only items and folders manually shared with the service account will be synced.

`google_workspace_admin_email_for_data_sync`
:   Required when domain-wide delegation for data sync is enabled. This email is used for discovery and syncing of shared drives. Only the shared drives this user has access to are synced.

`google_workspace_email_for_shared_drives_sync`
:   Required when domain-wide delegation for data sync is enabled. Provide the Google Workspace user email for discovery and syncing of shared drives. Only the shared drives this user has access to will be synced.

`use_document_level_security`
:   Toggle to enable [document level security (DLS](/reference/search-connectors/document-level-security.md). DLS is supported for the Google Drive connector. When enabled:

    * Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
    * Access control syncs will fetch users' access control lists and store them in a separate index.


`google_workspace_admin_email`
:   Google Workspace admin email. Required to enable document level security (DLS) or domain-wide delegation for data sync. A service account with delegated authority can impersonate an admin user with permissions to access Google Workspace user data and their group memberships. Refer to the [Google Cloud documentation](https://support.google.com/a/answer/162106?hl=en) for more information.

`max_concurrency`
:   The maximum number of concurrent HTTP requests to the Google Drive API. Increasing this value can improve data retrieval speed, but it may also place higher demands on system resources and network bandwidth.

`use_text_extraction_service`
:   Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that pipeline settings disable text extraction. Default value is `False`.


### Deployment using Docker [es-connectors-google-drive-client-deployment-using-docker]

You can deploy the Google Drive connector as a self-managed connector using Docker. Follow these instructions.

::::{dropdown} Step 1: Download sample configuration file
Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example --output ~/connectors-config/config.yml
```

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
    service_type: google_drive
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



### Documents and syncs [es-connectors-google-drive-client-documents-and-syncs]

The connector will fetch all files and folders the service account has access to.

It will attempt to extract the content from Google Suite documents (Google Docs, Google Sheets and Google Slides) and regular files.

::::{note}
* Content from files bigger than 10 MB won’t be extracted
* Permissions are not synced by default. You must first enable [DLS](#es-connectors-google-drive-client-document-level-security). Otherwise, **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-google-drive-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Sync rules [es-connectors-google-drive-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently filtering is controlled via ingest pipelines.


### Document level security [es-connectors-google-drive-client-document-level-security]

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. Refer to [configuration](#es-connectors-google-drive-client-configuration) on this page for how to enable DLS for this connector.

::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Content extraction [es-connectors-google-drive-client-content-extraction]

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md) for more information.


### End-to-end testing [es-connectors-google-drive-client-end-to-end-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the Google Drive connector, run the following command:

```shell
make ftest NAME=google_drive
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=google_drive DATA_SIZE=small
```


### Known issues [es-connectors-google-drive-client-known-issues]

There are currently no known issues for this connector.


### Troubleshooting [es-connectors-google-drive-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-google-drive-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).

