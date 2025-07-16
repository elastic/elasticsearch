---
navigation_title: "Salesforce"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-salesforce.html
---

# Elastic Salesforce connector reference [es-connectors-salesforce]


The *Elastic Salesforce connector* is a [connector](/reference/search-connectors/index.md) for [Salesforce](https://www.salesforce.com/) data sources.

::::{important}
As of Elastic 9.0, managed connectors on Elastic Cloud Hosted are no longer available. All connectors must be [self-managed](/reference/search-connectors/self-managed-connectors.md).
::::

## **Self-managed connector reference** [es-connectors-salesforce-connector-client-reference]

### Availability and prerequisites [es-connectors-salesforce-client-availability-prerequisites]

This connector is available as a self-managed connector. This self-managed connector is compatible with Elastic versions **8.10.0+**. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Compatibility [es-connectors-salesforce-client-compatability]

This connector is compatible with the following:

* Salesforce
* Salesforce Sandbox


### Create a Salesforce connector [es-connectors-salesforce-create-connector-client]


#### Use the UI [es-connectors-salesforce-client-create-use-the-ui]

To create a new Salesforce connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Salesforce** self-managed connector.


#### Use the API [es-connectors-salesforce-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Salesforce self-managed connector.

For example:

```console
PUT _connector/my-salesforce-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Salesforce",
  "service_type": "salesforce"
}
```

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


### Usage [es-connectors-salesforce-client-usage]

To use this connector as a **self-managed connector**, use the **Connector** workflow in the Kibana UI.

For additional operations, see [connectors usage](/reference/search-connectors/connectors-ui-in-kibana.md).

::::{note}
You need to create an Salesforce connected app with OAuth2.0 enabled to authenticate with Salesforce.

::::



#### Create a Salesforce connected app [es-connectors-salesforce-client-connected-app]

The Salesforce connector authenticates with Salesforce through a **connected app**. Follow the official Salesforce documentation for [Configuring a Connected App for the OAuth 2.0 Client Credentials Flow](https://help.salesforce.com/s/articleView?id=sf.connected_app_client_credentials_setup.htm).

When creating the connected app, in the section titled **API (Enable OAuth Settings)** ensure the following settings are *enabled*:

* **Enable OAuth Settings**
* **Enable for Device Flow**

    * **Callback URL** should be the Salesforce dummy callback URL, `https://test.salesforce.com/services/oauth2/success`

* **Require Secret for Web Server Flow**
* **Require Secret for Refresh Token Flow**
* **Enable Client Credentials Flow**

All other options should be disabled. Finally, in the section **Selected OAuth Scopes**, include the following OAuth scopes:

* **Manage user data via APIs (api)**
* **Perform requests at any time (refresh_token, offline_access)**


### Salesforce admin requirements [es-connectors-client-salesforce-admin-prerequisites]

By default, the Salesforce connector requires global administrator permissions to access Salesforce data. Expand the section below to learn how to create a custom Salesforce user with minimal permissions.

::::{dropdown} Create a custom Salesforce user with minimal permissions
By creating a custom profile with sufficient permissions from the Setup menu, you can remove the system administrator role requirement for fetching data from Salesforce.

To create a new profile:

1. From the Salesforce Setup menu, go to **Administration ⇒ Users ⇒ Profiles**.
2. Create a new profile.
3. Choose `Read Only` or `Standard User` from the **Existing Profile** dropdown. Name the profile and save it.

    ::::{tip}
    By default, `Read Only` or `Standard User` users have read permission to access all standard objects.

    ::::

4. Edit the newly created profile. Under **Object Permissions**, assign at least `Read` access to the standard objects and custom objects you want to ingest into Elasticsearch.
5. Make sure the newly created profile has at least `Read` access for the following standard objects:

    * Account
    * Campaign
    * Case
    * Contact
    * EmailMessage
    * Lead
    * Opportunity
    * User

        ::::{tip}
        If using [advanced sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-advanced) you’ll need to assign `Read` access for that specific object in the profile.

        ::::

6. Go to **Users ⇒ Profiles** and assign the newly created profile to the user.
7. Go to **Connected apps**, select your app and then select **Edit policies**. Assign the client credentials flow to the user with the custom profile in Salesforce.

    Now, the connector can be configured for this user profile to fetch all object records, without needing the system administration role.


::::



### Deployment using Docker [es-connectors-salesforce-client-docker]

Self-managed connectors are run on your own infrastructure.

You can deploy the Salesforce connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: salesforce
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



### Configuration [es-connectors-salesforce-client-configuration]

The following settings are required to set up this connector:

`domain`(required)
:   The domain for your Salesforce account. This is the subdomain that appears in your Salesforce URL. For example, if your Salesforce URL is `foo.my.salesforce.com`, then your domain would be `foo`. If you are using Salesforce Sandbox, your URL will contain an extra subdomain and will look similar to `foo.sandbox.my.salesforce.com`. In this case, your domain would be `foo.sandbox`.

`client_id`(required)
:   The Client ID generated by your connected app. The Salesforce documentation will sometimes also call this a **Consumer Key**

`client_secret`(required)
:   The Client Secret generated by your connected app. The Salesforce documentation will sometimes also call this a **Consumer Secret**.

`use_document_level_security`
:   Toggle to enable document level security (DLS). Optional, disabled by default. Refer to the [DLS section](#es-connectors-salesforce-client-dls) for more information, including how to set various Salesforce permission types.

    When enabled:

    * Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
    * Access control syncs will fetch users' access control lists and store them in a separate index.



#### Finding the Client ID and Client Secret [es-connectors-salesforce-client-configuration-credentials]

The Client ID and Client Secret are not automatically shown to you after you create a connected app. You can find them by taking the following steps:

* Navigate to **Setup**
* Go to **Platform Tools > Apps > App Manager**
* Click on the triangle next to your app and select **View**
* After the page loads, click on **Manage Consumer Details**

Your Client ID and Client Secret should now be visible at the top of the page.


### Document level security (DLS) [es-connectors-salesforce-client-dls]

[Document level security (DLS)](/reference/search-connectors/document-level-security.md) enables you to restrict access to documents based on a user'­s permissions. This feature is available by default for the Salesforce connector and supports both **standard and custom objects**.

Salesforce allows users to set permissions in the following ways:

* **Profiles**
* **Permission sets**
* **Permission set Groups**

For guidance, refer to these [video tutorials](https://howtovideos.hubs.vidyard.com/watch/B1bQnMFg2VyZq7V6zXQjPg#:~:text=This%20is%20a%20must%20watch,records%20in%20your%20Salesforce%20organization) about setting Salesforce permissions.

To ingest any standard or custom objects, users must ensure that at least `Read` permission is granted to that object. This can be granted using any of the following methods for setting permissions.


#### Set Permissions using Profiles [es-connectors-salesforce-client-dls-profiles]

Refer to the [Salesforce documentation](https://help.salesforce.com/s/articleView?id=sf.admin_userprofiles.htm&type=5) for setting permissions via Profiles.


#### Set Permissions using Permissions Set [es-connectors-salesforce-client-dls-permission-sets]

Refer to the [Salesforce documentation](https://help.salesforce.com/s/articleView?id=sf.perm_sets_overview.htm&language=en_US&type=5) for setting permissions via Permissions Sets.


#### Set Permissions using Permissions Set group [es-connectors-salesforce-client-dls-permission-set-groups]

Refer to the [Salesforce documentation](https://help.salesforce.com/s/articleView?id=sf.perm_set_groups.htm&type=5) for setting permissions via Permissions Set Groups.


#### Assign Profiles, Permission Set and Permission Set Groups to the User [es-connectors-salesforce-client-dls-assign-permissions]

Once the permissions are set, assign the Profiles, Permission Set or Permission Set Groups to the user. Follow these steps in Salesforce:

1. Navigate to `Administration` under the `Users` section.
2. Select `Users` and choose the user to set the permissions to.
3. Set the `Profile`, `Permission Set` or `Permission Set Groups` created in the earlier steps.


### Sync rules [es-connectors-salesforce-client-sync-rules]

*Basic* sync rules are identical for all connectors and are available by default.

For more information read [sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-types).


#### Advanced sync rules [es-connectors-salesforce-client-sync-rules-advanced]

::::{note}
A [full sync](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


The following section describes **advanced sync rules** for this connector. Advanced sync rules enable filtering of data in Salesforce *before* indexing into Elasticsearch.

They take the following parameters:

1. `query` : Salesforce query to filter the documents.
2. `language` : Salesforce query language. Allowed values are **SOQL** and **SOSL**.

$$$es-connectors-salesforce-client-sync-rules-advanced-fetch-query-language$$$
**Fetch documents based on the query and language specified**

**Example**: Fetch documents using SOQL query

```js
[
  {
    "query": "SELECT Id, Name FROM Account",
    "language": "SOQL"
  }
]
```

**Example**: Fetch documents using SOSL query.

```js
[
  {
    "query": "FIND {Salesforce} IN ALL FIELDS",
    "language": "SOSL"
  }
]
```

$$$es-connectors-salesforce-client-sync-rules-advanced-fetch-objects$$$
**Fetch standard and custom objects using SOQL and SOSL queries**

**Example**: Fetch documents for standard objects via SOQL and SOSL query.

```js
[
  {
    "query": "SELECT Account_Id, Address, Contact_Number FROM Account",
    "language": "SOQL"
  },
  {
    "query": "FIND {Alex Wilber} IN ALL FIELDS RETURNING Contact(LastModifiedDate, Name, Address)",
    "language": "SOSL"
  }
]
```

**Example**: Fetch documents for custom objects via SOQL and SOSL query.

```js
[
  {
    "query": "SELECT Connector_Name, Version FROM Connector__c",
    "language": "SOQL"
  },
  {
    "query": "FIND {Salesforce} IN ALL FIELDS RETURNING Connectors__c(Id, Connector_Name, Connector_Version)",
    "language": "SOSL"
  }
]
```

$$$es-connectors-salesforce-client-sync-rules-advanced-fetch-standard-custom-fields$$$
**Fetch documents with standard and custom fields**

**Example**: Fetch documents with all standard and custom fields for Account object.

```js
[
  {
    "query": "SELECT FIELDS(ALL) FROM Account",
    "language": "SOQL"
  }
]
```

**Example**: Fetch documents with all custom fields for Connector object.

```js
[
  {
    "query": "SELECT FIELDS(CUSTOM) FROM Connector__c",
    "language": "SOQL"
  }
]
```

**Example**: Fetch documents with all standard fields for Account object.

```js
[
  {
    "query": "SELECT FIELDS(STANDARD) FROM Account",
    "language": "SOQL"
  }
]
```


### Documents and syncs [es-connectors-salesforce-client-documents-syncs]

The connector syncs the following Salesforce objects:

* **Accounts**
* **Campaigns**
* **Cases**
* **Contacts**
* **Content Documents** (files uploaded to Salesforce)
* **Leads**
* **Opportunities**

The connector will not ingest any objects that it does not have permissions to query.

::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. Use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced by default. You must enable [document level security](/reference/search-connectors/document-level-security.md). Otherwise, **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



#### Sync types [es-connectors-salesforce-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Content Extraction [es-connectors-salesforce-client-content-extraction]

The connector will retrieve Content Documents from your Salesforce source if they meet the following criteria:

* Are attached to one or more objects that are synced
* Are of a file type that can be extracted

This means that the connector will not ingest any Content Documents you have that are *not* attached to a supported Salesforce object. See [documents and syncs](#es-connectors-salesforce-client-documents-syncs) for a list of supported object types.

If a single Content Document is attached to multiple supported objects, only one Elastic document will be created for it. This document will retain links to every object that it was connected to in the `related_ids` field.

See [content extraction](/reference/search-connectors/es-connectors-content-extraction.md) for more specifics on content extraction.


### Known issues [es-connectors-salesforce-client-known-issues]

* **DLS feature is "type-level" not "document-level"**

    Salesforce DLS, added in 8.13.0, does not accomodate specific access controls to specific Salesforce Objects. Instead, if a given user/group can have access to *any* Objects of a given type (`Case`, `Lead`, `Opportunity`, etc), that user/group will appear in the `\_allow_access_control` list for *all* of the Objects of that type. See [https://github.com/elastic/connectors/issues/3028](https://github.com/elastic/connectors/issues/3028) for more details.

* **Only first 500 nested entities are ingested**
    
    Some of the entities that Salesforce connector fetches are nested - they are ingested along the parent objects using a `JOIN` query. Examples of such entities are `EmailMessages`, `CaseComments` and `FeedComments`. When Salesforce connector fetches these entities it sets a limit to fetch only first 500 entities per parent object. The only possible workaround for it now is to fork the Connectors repository and modify the code in Salesforce connector to increase these limits.

Refer to [connector known issues](/release-notes/known-issues.md) for a list of known issues for all connectors.

### Security [es-connectors-salesforce-client-security]

See [connectors security](/reference/search-connectors/es-connectors-security.md).


### Framework and source [es-connectors-salesforce-client-source]

This connector is built with the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/salesforce.py) (branch *main*, compatible with Elastic *9.0*).
