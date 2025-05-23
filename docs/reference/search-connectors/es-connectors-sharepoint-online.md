---
navigation_title: "SharePoint Online"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-sharepoint-online.html
---

# Elastic SharePoint Online connector reference [es-connectors-sharepoint-online]


::::{tip}
Looking for the SharePoint **Server** connector? See [SharePoint Server reference](/reference/search-connectors/es-connectors-sharepoint.md).

::::


The *Elastic SharePoint Online connector* is a [connector](/reference/search-connectors/index.md) for [Microsoft SharePoint Online](https://www.microsoft.com/en-ww/microsoft-365/sharepoint/).

This connector is written in Python using the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [**source code** for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/sharepoint_online.py) (branch *main*, compatible with Elastic *9.0*).

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector** [es-connectors-sharepoint-online-connector-client-reference]

### Availability and prerequisites [es-connectors-sharepoint-online-client-availability-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).

::::{note}
This connector requires a subscription. View the requirements for this feature under the **Elastic Search** section of the [Elastic Stack subscriptions](https://www.elastic.co/subscriptions) page.
::::

### Usage [es-connectors-sharepoint-online-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md) For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### SharePoint prerequisites [es-connectors-sharepoint-online-client-sharepoint-prerequisites]


#### Create SharePoint OAuth app [es-connectors-sharepoint-online-client-oauth-app-create]

Before you can configure the connector, you must create an **OAuth App** in the SharePoint Online platform. Your connector will authenticate to SharePoint as the registered OAuth application/client. You’ll collect values (`client ID`, `tenant ID`, and `client secret`) during this process that you’ll need for the [configuration step](#es-connectors-sharepoint-online-client-configuration) in Kibana.

To get started, first log in to SharePoint Online and access your administrative dashboard. Ensure you are logged in as the Azure Portal **service account**.

Follow these steps:

* Sign in to [https://portal.azure.com/](https://portal.azure.com/) and click on **Microsoft Entra ID** (formerly Azure Active Directory).
* Locate **App Registrations** and Click **New Registration**.
* Give your app a name - like "Search".
* Leave the **Redirect URIs** blank for now.
* **Register** the application.
* Find and keep the **Application (client) ID** and **Directory (tenant) ID** handy.
* Create a certificate and private key. This can, for example, be done by running `openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout azure_app.key -out azure_app.crt` command. Store both in a safe and secure place
* Locate the **Certificates** by navigating to **Client credentials: Certificates & Secrets**.
* Select **Upload certificate**
* Upload the certificate created in one of previous steps: `azure_app.crt`
* Set up the permissions the OAuth App will request from the Azure Portal service account.

    * Navigate to **API Permissions** and click **Add Permission**.
    * Add **application permissions** until the list looks like the following:

        ```
        Graph API
        - Sites.Selected
        - Files.Read.All
        - Group.Read.All
        - User.Read.All

        Sharepoint
        - Sites.Selected
        ```

        ::::{note}
        If the `Comma-separated list of sites` configuration is set to `*` or if a user enables the toggle button `Enumerate all sites`, the connector requires `Sites.Read.All` permission.
        ::::

* **Grant admin consent**, using the `Grant Admin Consent` link from the permissions screen.
* Save the tenant name (i.e. Domain name) of Azure platform.

::::{warning}
The connector requires application permissions.  It does not support delegated permissions (scopes).

::::


::::{note}
The connector uses the [Graph API](https://learn.microsoft.com/en-us/sharepoint/dev/apis/sharepoint-rest-graph) (stable [v1.0 API](https://learn.microsoft.com/en-us/graph/api/overview?view=graph-rest-1.0#other-api-versions)) where possible to fetch data from Sharepoint Online. When entities are not available via the Graph API the connector falls back to using the Sharepoint [REST API](https://learn.microsoft.com/en-us/sharepoint/dev/sp-add-ins/get-to-know-the-sharepoint-rest-service).

::::



#### SharePoint permissions [es-connectors-sharepoint-online-client-oauth-app-permissions]

Microsoft is [retiring Azure Access Control Service (ACS)](https://learn.microsoft.com/en-us/sharepoint/dev/sp-add-ins/retirement-announcement-for-azure-acs). This affects permission configuration: * **Tenants created after November 1st, 2024**: Certificate authentication is required * **Tenants created before November 1st, 2024**: Secret-based authentication must be migrated to certificate authentication by April 2nd, 2026


### Certificate Authentication [es-connectors-sharepoint-online-client-oauth-app-certificate-auth]

This authentication method does not require additional setup other than creating and uploading certificates to the OAuth App.


### Secret Authentication [es-connectors-sharepoint-online-client-oauth-app-secret-auth]

::::{important}
This method is only applicable to tenants created before November 1st, 2024. This method will be fully retired as of April 2nd, 2026.

::::


Refer to the following documentation for setting [SharePoint permissions](https://learn.microsoft.com/en-us/sharepoint/dev/solution-guidance/security-apponly-azureacs).

* To set `DisableCustomAppAuthentication` to false, connect to SharePoint using PowerShell and run `set-spotenant -DisableCustomAppAuthentication $false`
* To assign full permissions to the tenant in SharePoint Online, go to the tenant URL in your browser. The URL follows this pattern: `https://<office_365_admin_tenant_URL>/_layouts/15/appinv.aspx`. This loads the SharePoint admin center page.

    * In the **App ID** box, enter the application ID that you recorded earlier, and then click **Lookup**. The application name will appear in the Title box.
    * In the **App Domain** box, type <tenant_name>.onmicrosoft.com
    * In the **App’s Permission Request XML** box, type the following XML string:

        ```xml
        <AppPermissionRequests AllowAppOnlyPolicy="true">
        <AppPermissionRequest Scope="http://sharepoint/content/tenant" Right="FullControl" />
        <AppPermissionRequest Scope="http://sharepoint/social/tenant" Right="Read" />
        </AppPermissionRequests>
        ```



#### Granting `Sites.Selected` permissions [es-connectors-sharepoint-online-sites-selected-permissions-self-managed]

To configure `Sites.Selected` permissions, follow these steps in the Microsoft Entra ID portal. These permissions enable precise access control to specific SharePoint sites.

1. Sign in to the [Microsoft Entra ID portal](https://portal.azure.com/).
2. Navigate to **App registrations** and locate the application created for the connector.
3. Under **API permissions**, click **Add permission**.
4. Select **Microsoft Graph** > **Application permissions**, then add `Sites.Selected`.
5. Click **Grant admin consent** to approve the permission.

::::{tip}
Refer to the official [Microsoft documentation](https://learn.microsoft.com/en-us/graph/permissions-reference) for managing permissions in Azure AD.

::::


To assign access to specific SharePoint sites using `Sites.Selected`:

1. Use Microsoft Graph Explorer or PowerShell to grant access.
2. To fetch the site ID, run the following Graph API query:

    ```http
    GET https://graph.microsoft.com/v1.0/sites?select=webUrl,Title,Id&$search="<Name of the site>*"
    ```

    This will return the `id` of the site.

3. Use the `id` to assign read or write access:

    ```http
    POST https://graph.microsoft.com/v1.0/sites/<siteId>/permissions
    {
        "roles": ["read"], // or "write"
        "grantedToIdentities": [
            {
                "application": {
                    "id": "<App_Client_ID>",
                    "displayName": "<App_Display_Name>"
                }
            }
        ]
    }
    ```


::::{note}
When using the `Comma-separated list of sites` configuration field, ensure the sites specified match those granted `Sites.Selected` permission in SharePoint. If the `Comma-separated list of sites` field is set to `*` or the `Enumerate all sites` toggle is enabled, the connector will attempt to access all sites. This requires broader permissions, which are not supported with `Sites.Selected`.

::::


::::{admonition} Graph API permissions
Microsoft recommends using Graph API for all operations with Sharepoint Online. Graph API is well-documented and more efficient at fetching data, which helps avoid throttling. Refer to [Microsoft’s throttling policies](https://learn.microsoft.com/en-us/sharepoint/dev/general-development/how-to-avoid-getting-throttled-or-blocked-in-sharepoint-online) for more information.

Here’s a summary of why we use these Graph API permissions:

* **Sites.Selected** is used to fetch the sites and their metadata
* **Files.Read.All** is used to fetch Site Drives and files in these drives
* **Groups.Read.All** is used to fetch groups for document-level permissions
* **User.Read.All** is used to fetch user information for document-level permissions

Due to the way the Graph API is designed, these permissions are "all or nothing" - it’s currently impossible to limit access to these resources.

::::



### Compatibility [es-connectors-sharepoint-online-client-compatability]

This connector is compatible with SharePoint Online.


### Configuration [es-connectors-sharepoint-online-client-configuration]

Use the following configuration fields to set up the connector:

`tenant_id`
:   The tenant id for the Azure account hosting the Sharepoint Online instance.

`tenant_name`
:   The tenant name for the Azure account hosting the Sharepoint Online instance.

`client_id`
:   The client id to authenticate with SharePoint Online.

`auth_method`
:   Authentication method to use to connector to Sharepoint Online and Rest APIs. `secret` is deprecated and `certificate` is recommended.

`secret_value`
:   The secret value to authenticate with SharePoint Online, if auth_method: `secret` is chosen.

`certificate`
:   Content of certificate file if auth_method: `certificate` is chosen.

`private_key`
:   Content of private key file if auth_method: `certificate` is chosen.

`site_collections`
:   List of site collection names or paths to fetch from SharePoint. When enumerating all sites, these values should be the *names* of the sites. Use `*` to include all available sites. Examples:

    * `collection1`
    * `collection1,sub-collection`
    * `*`

        When **not** enumerating all sites, these values should be the *paths* (URL after `/sites/`) of the sites. Examples:

    * `collection1`
    * `collection1,collection1/sub-collection`


`enumerate_all_sites`
:   If enabled, the full list of all sites will be fetched from the API, in bulk, and will be filtered down to match the configured list of site names. If disabled, each path in the configured list of site paths will be fetched individually from the API. Enabling this configuration is most useful when syncing large numbers (more than total/200) of sites. This is because, at high volumes, it is more efficient to fetch sites in bulk. When syncing fewer sites, disabling this configuration can result in improved performance. This is because, at low volumes, it is more efficient to only fetch the sites that you need.

    ::::{note}
    When disabled, `*` is not a valid configuration for `Comma-separated list of sites`.

    ::::


`fetch_subsites`
:   Whether sub-sites of the configured site(s) should be automatically fetched. This option is only available when not enumerating all sites (see above).

`use_text_extraction_service`
:   Toggle to enable local text extraction service for documents. Requires a separate deployment of the [Elastic Text Extraction Service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local). Requires that ingest pipeline settings disable text extraction. Default value is `False`.

`use_document_level_security`
:   Toggle to enable [document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled, full and incremental syncs will fetch access control lists for each document and store them in the `_allow_access_control` field. Access control syncs will fetch users' access control lists and store them in a separate index.

    Once enabled, the following granular permissions toggles will be available:

    * **Fetch drive item permissions**: Enable this option to fetch **drive item** specific permissions.
    * **Fetch unique page permissions**: Enable this option to fetch unique **page** permissions. If this setting is disabled a page will inherit permissions from its parent site.
    * **Fetch unique list permissions**: Enable this option to fetch unique **list** permissions. If this setting is disabled a list will inherit permissions from its parent site.
    * **Fetch unique list item permissions**: Enable this option to fetch unique **list item** permissions. If this setting is disabled a list item will inherit permissions from its parent site.

        ::::{note}
        If left empty the default value `true` will be used for these granular permissions toggles. Note that these settings may increase sync times.

        ::::



### Deployment using Docker [es-connectors-sharepoint-online-client-docker]

You can deploy the SharePoint Online connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: sharepoint_online
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



### Documents and syncs [es-connectors-sharepoint-online-client-documents-syncs]

The connector syncs the following SharePoint object types:

* **Sites** (and subsites)
* **Lists**
* **List items** and **attachment content**
* **Document libraries** and **attachment content** (including web pages)

::::{tip}
**Making Sharepoint Site Pages Web Part content searchable**

If you’re using Web Parts on Sharepoint Site Pages and want to make this content searchable, you’ll need to consult the [official documentation](https://learn.microsoft.com/en-us/sharepoint/dev/spfx/web-parts/guidance/integrate-web-part-properties-with-sharepoint#specify-web-part-property-value-type/).

We recommend setting `isHtmlString` to **True** for all Web Parts that need to be searchable.

::::


::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. Use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced by default. Enable [document-level security (DLS)](/reference/search-connectors/document-level-security.md) to sync permissions.

::::



#### Limitations [es-connectors-sharepoint-online-client-documents-syncs-limitations]

* The connector does not currently sync content from Teams-connected sites.


### Sync rules [es-connectors-sharepoint-online-client-sync-rules]

*Basic* sync rules are identical for all connectors and are available by default. For more information read [Types of sync rule](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


#### Advanced sync rules [es-connectors-sharepoint-online-client-sync-rules-advanced]

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


The following section describes **advanced sync rules** for this connector. Advanced sync rules are defined through a source-specific DSL JSON snippet.

[Advanced rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) for the Sharepoint Online connector enable you to avoid extracting and syncing older data that might no longer be relevant for search.

Example:

```js
{
	"skipExtractingDriveItemsOlderThan": 60
}
```

This rule will not extract content of any drive items (files in document libraries) that haven’t been modified for 60 days or more.

$$$es-connectors-sharepoint-online-client-sync-rules-limitations$$$
**Limitations of sync rules with incremental syncs**

Changing sync rules after Sharepoint Online content has already been indexed can bring unexpected results, when using [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).

Incremental syncs ensure *updates* from 3rd-party system, but do not modify existing documents in the index.

**To avoid these issues, run a full sync after changing sync rules (basic or advanced).**

Let’s take a look at several examples where incremental syncs might lead to inconsistent data on your index.

$$$es-connectors-sharepoint-online-client-sync-rules-limitations-restrictive-added$$$
**Example: Restrictive basic sync rule added after a full sync**

Imagine your Sharepoint Online drive contains the following drive items:

```txt
/Documents/Report.doc
/Documents/Spreadsheet.xls
/Presentations/Q4-2020-Report.pdf
/Presentations/Q4-2020-Report-Data.xls
/Personal/Documents/Sales.xls
```

After a sync, all these drive items will be stored on your Elasticsearch index. Let’s add a basic sync rule, filtering files by their path:

```txt
Exclude WHERE path CONTAINS "Documents"
```

These filtering rules will exclude all files with "Documents" in their path, leaving only files in `/Presentations` directory:

```txt
/Presentations/Q4-2020-Report.pdf
/Presentations/Q4-2020-Report-Data.xls
```

If no files were changed, incremental sync will not receive information about changes from Sharepoint Online and won’t be able to delete any files, leaving the index in the same state it was before the sync.

After a **full sync**, the index will be updated and files that are excluded by sync rules will be removed.

$$$es-connectors-sharepoint-online-client-sync-rules-limitations-restrictive-removed$$$
**Example: Restrictive basic sync rules removed after a full sync**

Imagine that Sharepoint Online drive has the following drive items:

```txt
/Documents/Report.doc
/Documents/Spreadsheet.xls
/Presentations/Q4-2020-Report.pdf
/Presentations/Q4-2020-Report-Data.xls
/Personal/Documents/Sales.xls
```

Before doing a sync, we add a restrictive basic filtering rule:

```txt
Exclude WHERE path CONTAINS "Documents"
```

After a full sync, the index will contain only files in the `/Presentations` directory:

```txt
/Presentations/Q4-2020-Report.pdf
/Presentations/Q4-2020-Report-Data.xls
```

Afterwards, we can remove the filtering rule and run an incremental sync. If no changes happened to the files, incremental sync will not mirror these changes in the Elasticsearch index, because Sharepoint Online will not report any changes to the items. Only a **full sync** will include the items previously ignored by the sync rule.

$$$es-connectors-sharepoint-online-client-sync-rules-limitations-restrictive-changed$$$
**Example: Advanced sync rules edge case**

Advanced sync rules can be applied to limit which documents will have content extracted. For example, it’s possible to set a rule so that documents older than 180 days won’t have content extracted.

However, there is an edge case. Imagine a document that is 179 days old and its content is extracted and indexed into Elasticsearch. After 2 days, this document will be 181 days old. Since this document was already ingested it will not be modified. Therefore, the content will not be removed from the index, following an incremental sync.

In this situation, if you want older documents to be removed, you will need to clean the index up manually. For example, you can manually run an Elasticsearch query that removes drive item content older than 180 days:

```console
POST INDEX_NAME/_update_by_query?conflicts=proceed
{
  "query": {
    "bool": {
      "filter": [
        {
          "match": {
            "object_type": "drive_item"
          }
        },
        {
          "exists": {
            "field": "file"
          }
        },
        {
          "range": {
            "lastModifiedDateTime": {
              "lte": "now-180d"
            }
          }
        }
      ]
    }
  },
  "script": {
    "source": "ctx._source.body = ''",
    "lang": "painless"
  }
}
```


### Document-level security [es-connectors-sharepoint-online-client-dls]

Document-level security (DLS) enables you to restrict access to documents based on a user’s permissions. This feature is available by default for this connector.

Refer to [configuration](#es-connectors-sharepoint-online-client-configuration) on this page for how to enable DLS for this connector.

::::{tip}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from SharePoint Online with DLS enabled, when building a search application.

::::



### Content extraction [es-connectors-sharepoint-online-client-content-extraction]


#### Default content extraction [es-connectors-sharepoint-online-client-content-extraction-pipeline]

The default content extraction service is powered by the default ingest pipeline. (See [Ingest pipelines for Search indices](docs-content://solutions/search/ingest-for-search.md).)

See [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


#### Local content extraction (for large files) [es-connectors-sharepoint-online-client-content-extraction-local]

The SharePoint Online self-managed connector supports large file content extraction (> **100MB**). This requires:

* A self-managed deployment of the Elastic Text Extraction Service.
* Text extraction to be *disabled* in the default ingest pipeline settings.

Refer to [local content extraction](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) for more information.


### End-to-end testing [es-connectors-sharepoint-online-client-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To perform E2E testing for the SharePoint Online connector, run the following command:

```shell
$ make ftest NAME=sharepoint_online
```

For faster tests, add the `DATA_SIZE=small` flag:

```shell
make ftest NAME=sharepoint_online DATA_SIZE=small
```


### Known issues [es-connectors-sharepoint-online-client-known-issues]

* **Documents failing to sync due to SharePoint file and folder limits**

    SharePoint has limits on the number of files and folders that can be synced. You might encounter an error like the following written to the body of documents that failed to sync: `The file size exceeds the allowed limit. CorrelationId: fdb36977-7cb8-4739-992f-49878ada6686, UTC DateTime: 4/21/2022 11:24:22 PM`

    Refer to [SharePoint documentation](https://support.microsoft.com/en-us/office/download-files-and-folders-from-onedrive-or-sharepoint-5c7397b7-19c7-4893-84fe-d02e8fa5df05#:~:text=Downloads%20are%20subject%20to%20the,zip%20file%20and%2020GB%20overall) for more information about these limits.

    * **Syncing a large number of files**

        The connector will fail to download files from folders that contain more than 5000 files. The List View Threshold (default 5000) is a limit that prevents operations with a high performance impact on the SharePoint Online environment.

        **Workaround:** Reduce batch size to avoid this issue.

    * **Syncing large files**

        SharePoint has file size limits, but these are configurable.

        **Workaround:** Increase the file size limit. Refer to [SharePoint documentation](https://learn.microsoft.com/en-us/sharepoint/manage-site-collection-storage-limits#set-automatic-or-manual-site-storage-limits) for more information.

    * **Deleted documents counter is not updated during incremental syncs**

        If the configuration `Enumerate All Sites?` is enabled, incremental syncs may not behave as expected. Drive Item documents that were deleted between incremental syncs may not be detected as deleted.

        **Workaround**: Disable `Enumerate All Sites?`, and configure full site paths for all desired sites.


Refer to [Known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.


### Troubleshooting [es-connectors-sharepoint-online-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-sharepoint-online-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).
