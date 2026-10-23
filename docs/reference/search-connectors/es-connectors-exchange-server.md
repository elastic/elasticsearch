---
applies_to:
  stack: ga 9.6
navigation_title: "Exchange Server"
---

# Elastic Exchange Server connector reference [es-connectors-exchange-server]

The Elastic Exchange Server connector is built with the Elastic connector framework and is available as a [self-managed connector](/reference/search-connectors/self-managed-connectors.md). It syncs mail, contacts, calendar events, tasks, and attachments from on-premises Microsoft Exchange Server.

::::{note}
This connector is the on-premises (server) mode of the [Outlook connector](/reference/search-connectors/es-connectors-outlook.md), available as its own connector. The `outlook_server` data source of the Outlook connector is deprecated. Use the Exchange Server connector for on-premises Exchange.
::::

## **Self-managed connector reference** [es-connectors-exchange-server-connector-client-reference]

### Availability and prerequisites [es-connectors-exchange-server-client-availability-and-prerequisites]

This connector is available as a self-managed connector. To use this connector, satisfy all [self-managed connector prerequisites](/reference/search-connectors/self-managed-connectors.md).


### Create an Exchange Server connector [es-connectors-exchange-server-create-connector-client]


#### Use the UI [es-connectors-exchange-server-client-create-use-the-ui]

To create a new Exchange Server connector:

1. In the Kibana UI, search for "connectors" using the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects) and choose the "Elasticsearch" connectors.
2. Follow the instructions to create a new  **Exchange Server** self-managed connector.


#### Use the API [es-connectors-exchange-server-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Exchange Server connector.

For example:

```console
PUT _connector/my-exchange-server-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Exchange Server",
  "service_type": "exchange_server"
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


### Usage [es-connectors-exchange-server-client-usage]

To use this connector as a **self-managed connector**, use the **Exchange Server** tile from the connectors list OR **Customized connector** workflow.

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Connecting to Exchange Server [es-connectors-exchange-server-client-connecting-to-exchange-server]

To connect to Exchange Server, the connector fetches Active Directory users with the help of the `ldap3` Python library, then reads each mailbox through Exchange Web Services (EWS).

The connector supports two authentication methods: NTLM and Kerberos.


#### NTLM [es-connectors-exchange-server-client-ntlm]

With `auth_method: ntlm` (default), the service account you configure in `username` and `password` needs:

* Permission to read user objects from Active Directory.
* The Exchange `ApplicationImpersonation` role for the mailboxes you want to sync. The connector opens each mailbox with impersonation, so valid credentials alone are not enough.


#### Kerberos [es-connectors-exchange-server-client-kerberos]

With `auth_method: kerberos`, the connector uses GSSAPI for LDAP and EWS. {applies_to}`stack: ga 9.7` Username and password are not used. The service account still needs the same Active Directory and Exchange permissions.

Kerberos is available for self-managed connectors only. The host that runs the connector service must provide:

* The optional Kerberos dependencies: `pip install 'elasticsearch-connectors[kerberos]'`. Building `gssapi` might require krb5 development packages, for example `libkrb5-dev` on Debian or `krb5-dev` on Alpine.
* A working `krb5.conf` and a reachable Kerberos key distribution center (KDC).
* A valid Kerberos ticket for the service account, for example by setting `KRB5CCNAME`. The connector does not obtain or renew tickets.
* Host names (FQDNs) in `exchange_server` and `active_directory_server`, not IP addresses, so that service principal names can be resolved.

The standard connector Docker image does not include the Kerberos packages. To run Kerberos in a container, use the FIPS image from the [connectors repository](https://github.com/elastic/connectors/blob/main/Dockerfile.fips) or build a custom image that installs the `[kerberos]` extra, and make the ticket cache and `krb5.conf` available to the container.


### FIPS mode [es-connectors-exchange-server-client-fips]

When FIPS mode is enabled in the [connector service](https://github.com/elastic/connectors/blob/main/docs/FIPS.md), `auth_method: ntlm` is rejected because NTLM relies on non-FIPS algorithms. Configure `auth_method: kerberos` and ensure a valid Kerberos ticket is available for the connector process.

The deprecated `outlook_server` mode of the [Outlook connector](/reference/search-connectors/es-connectors-outlook.md) supports the same `auth_method` setting.


### Configuration [es-connectors-exchange-server-client-configuration]

`exchange_server`
:   (required) IP address to connect with Exchange Server. Example: `127.0.0.1`

`active_directory_server`
:   (required) IP address of the Active Directory server used to fetch users. Example: `127.0.0.1`

`auth_method` {applies_to}`stack: ga 9.7`
:   Authentication method for LDAP user lookup and EWS. Supported values: `ntlm` (default) or `kerberos`. Use `kerberos` when running the connector service in FIPS mode. Kerberos is available for self-managed connectors only. Refer to [Kerberos](#es-connectors-exchange-server-client-kerberos) for requirements.

`username`
:   (required when `auth_method` is `ntlm`) Username to authenticate with Exchange Server.

`password`
:   (required when `auth_method` is `ntlm`) Password to authenticate with Exchange Server.

`domain`
:   (required) Active Directory domain name, used to look up users. Example: `corp.example.com`

`ssl_enabled`
:   Whether SSL verification will be enabled. Default value is `False`.

`ssl_ca`
:   (required if SSL is enabled) Content of SSL certificate. Example certificate:

    ```txt
    -----BEGIN CERTIFICATE-----
    MIID+jCCAuKgAwIBAgIGAJJMzlxLMA0GCSqGSIb3DQEBCwUAMHoxCzAJBgNVBAYT
    ...
    7RhLQyWn2u00L7/9Omw=
    -----END CERTIFICATE-----
    ```


`sync_all_mail_folders`
:   Indexes mail from the user mail folders in each mailbox, not only Inbox, Sent, Junk, and Archive. Other folders are stored as `Mail` with a `folder_name` field. The four default folders keep their existing types. System folders such as Deleted Items, Drafts, Outbox, and search folders are never indexed. Enabling this setting increases sync time, load on Exchange, and index size. Default value is `False`.

`use_text_extraction_service`
:   Use [self-hosted content extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-data-extraction-service). Default value is `False`.

`Index full raw email (including headers)`
:   Toggle to index the full raw MIME message. Default value is `False`. When off, only the email body (preferring `text/plain` over `text/html`) and a minimal set of headers (`Subject`, `From`, `Reply-To`, `To`, `Cc`, `Bcc`, `Date`, `Message-ID`) are indexed. Routing/authentication headers and binary attachments are dropped. Enable this toggle to restore the passthrough behavior when body extraction misses content.

`use_document_level_security`
:   Toggle to enable [Document level security (DLS)](/reference/search-connectors/document-level-security.md). When enabled:

    * Full syncs will fetch access control lists for each document and store them in the `_allow_access_control` field.
    * Access control syncs fetch users' access control lists and store them in a separate index.


### Deployment using Docker [es-connectors-exchange-server-client-client-docker]

You can deploy the Exchange Server connector as a self-managed connector using Docker. Follow these instructions.

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

If you’re running the connector service against a Dockerized version of Elasticsearch and Kibana, your config file will look like this:

```yaml
# When connecting to your cloud deployment you should edit the host value
elasticsearch.host: http://host.docker.internal:9200
elasticsearch.api_key: <ELASTICSEARCH_API_KEY>

connectors:
  -
    connector_id: <CONNECTOR_ID_FROM_KIBANA>
    service_type: exchange_server
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

Using the `elasticsearch.api_key` is the recommended authentication method. However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

Note: You can change other default configurations by uncommenting specific settings in the configuration file and modifying their values.

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



### Content Extraction [es-connectors-exchange-server-client-content-extraction]

Refer to [Content extraction](/reference/search-connectors/es-connectors-content-extraction.md).


### Documents and syncs [es-connectors-exchange-server-client-documents-and-syncs]

The connector syncs the following objects and entities:

* **Mails**

    * **Inbox Mails**
    * **Sent Mails**
    * **Archive Mails**
    * **Junk Mails**
    * **Mail** — other user mail folders, when `sync_all_mail_folders` is enabled. Includes a `folder_name` field.

* **Contacts**
* **Calendar Events**
* **Tasks**
* **Attachments**

    * **Mail Attachments**
    * **Task Attachments**
    * **Calendar Attachments**


::::{note}
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](/reference/search-connectors/es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced unless document level security is enabled. When it is not enabled, **all documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.
* Mailbox discovery uses **Active Directory** (LDAP), not Microsoft Graph. The connector considers AD `user` objects that match its LDAP search filters and have a valid **`mail`** attribute, then opens each mailbox through **Exchange Web Services (EWS)**. AD accounts without `mail`, and SMTP addresses with **no mailbox** on Exchange (including many **service accounts**), are **skipped** during sync. **Shared mailboxes** are synced only when their AD user is discovered and Exchange exposes a mailbox for that address.

::::



#### Sync types [es-connectors-exchange-server-client-sync-types]

[Full syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-full) are supported by default for all connectors.

This connector also supports [incremental syncs](/reference/search-connectors/content-syncs.md#es-connectors-sync-types-incremental).


### Document level security [es-connectors-exchange-server-client-document-level-security]

Document level security (DLS) enables you to restrict access to documents based on a user’s permissions. Refer to [configuration](#es-connectors-exchange-server-client-configuration) on this page for how to enable DLS for this connector.

::::{note}
Refer to [DLS in Search Applications](/reference/search-connectors/es-dls-e2e-guide.md) to learn how to ingest data from a connector with DLS enabled, when building a search application. The example uses SharePoint Online as the data source, but the same steps apply to every connector.

::::



### Sync rules [es-connectors-exchange-server-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


### Advanced Sync Rules [es-connectors-exchange-server-client-advanced-sync-rules]

Advanced sync rules are not available for this connector in the present version.


### Connector Client operations [es-connectors-exchange-server-client-connector-client-operations]


#### End-to-end Testing [es-connectors-exchange-server-client-end-to-end-testing]

**Note:** End-to-end testing is not available in the current version of the connector.


#### Known issues [es-connectors-exchange-server-client-known-issues]

Refer to [Known issues](/reference/search-connectors/es-connectors-known-issues.md) for a list of known issues for all connectors. Known issues listed for the Outlook connector that relate to Exchange Server (on-premises) can also affect this connector.


### Troubleshooting [es-connectors-exchange-server-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-exchange-server-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Framework and source [es-connectors-exchange-server-client-source]

This connector is included in the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/app/connectors_service/connectors/sources/exchange_server) (branch *main*, compatible with Elastic *9.6*).

