---
navigation_title: "Oracle"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/es-connectors-oracle.html
---

# Elastic Oracle connector reference [es-connectors-oracle]

## **Self-managed connector reference** [es-connectors-oracle-connector-client-reference]

### Availability and prerequisites [es-connectors-oracle-client-availability-prerequisites]

This connector is available as a self-managed connector. This self-managed connector is compatible with Elastic versions **8.6.0+**. To use this connector, satisfy all [self-managed connector requirements](/reference/search-connectors/self-managed-connectors.md).


### Create a Oracle connector [es-connectors-oracle-create-connector-client]


#### Use the UI [es-connectors-oracle-client-create-use-the-ui]

To create a new Oracle connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](docs-content://explore-analyze/query-filter/filtering.md#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Oracle** self-managed connector.


#### Use the API [es-connectors-oracle-client-create-use-the-api]

You can use the {{es}} [Create connector API](https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-connector) to create a new self-managed Oracle self-managed connector.

For example:

```console
PUT _connector/my-oracle-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Oracle",
  "service_type": "oracle"
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


### Usage [es-connectors-oracle-client-usage]

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](/reference/search-connectors/self-managed-connectors.md).

The database user requires `CONNECT` and `DBA` privileges and must be the owner of the tables to be indexed.


#### Secure connection [es-connectors-oracle-client-usage-secure-connection]

To set up a secure connection the Oracle service must be installed on the system where the connector is running.

Follow these steps:

1. Set the `oracle_home` parameter to your Oracle home directory. If configuration files are not at the default location, set the `wallet_configuration_path` parameter.
2. Create a directory to store the wallet.

    ```shell
    $ mkdir $ORACLE_HOME/ssl_wallet
    ```

3. Create file named `sqlnet.ora` at `$ORACLE_HOME/network/admin` and add the following content:

    ```shell
    WALLET_LOCATION = (SOURCE = (METHOD = FILE) (METHOD_DATA = (DIRECTORY = $ORACLE_HOME/ssl_wallet)))
    SSL_CLIENT_AUTHENTICATION = FALSE
    SSL_VERSION = 1.0
    SSL_CIPHER_SUITES = (SSL_RSA_WITH_AES_256_CBC_SHA)
    SSL_SERVER_DN_MATCH = ON
    ```

4. Run the following commands to create a wallet and attach an SSL certificate. Replace the file name with your file name.

    ```shell
    $ orapki wallet create -wallet path-to-oracle-home/ssl_wallet -auto_login_only
    $ orapki wallet add -wallet path-to-oracle-home/ssl_wallet -trusted_cert -cert path-to-oracle-home/ssl_wallet/root_ca.pem -auto_login_only
    ```


For more information, refer to this [Amazon RDS documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.Oracle.Options.SSL.html) about Oracle SSL. Oracle docs: [https://docs.oracle.com/database/121/DBSEG/asossl.htm#DBSEG070](https://docs.oracle.com/database/121/DBSEG/asossl.htm#DBSEG070).

For additional operations, see [*Connectors UI in {{kib}}*](/reference/search-connectors/connectors-ui-in-kibana.md).


### Compatibility [es-connectors-oracle-client-compatability]

Oracle Database versions **18c**, **19c** and **21c** are compatible with Elastic connector frameworks.


### Configuration [es-connectors-oracle-client-configuration]




Use the following configuration fields to set up the connector:

`connection_source`
:   Determines the Oracle source: Service Name or SID. Default value is SID. Select *Service Name* if connecting to a pluggable database.

`sid`
:   SID of the database.

`service_name`
:   Service name for the database.

`host`
:   The IP address or hostname of the Oracle database server. Default value is `127.0.0.1`.

`port`
:   Port number of the Oracle database server.

`username`
:   Username to use to connect to the Oracle database server.

`password`
:   Password to use to connect to the Oracle database server.

`tables`
:   Comma-separated list of tables to monitor for changes. Default value is `*`. Examples:

    * `TABLE_1, TABLE_2`
    * `*`


`oracle_protocol`
:   Protocol which the connector uses to establish a connection. Default value is `TCP`. For secure connections, use `TCPS`.

`oracle_home`
:   Path to Oracle home directory to run connector in thick mode for secured connection. For unsecured connections, keep this field empty.

`wallet_configuration_path`
:   Path to SSL Wallet configuration files.

`fetch_size`
:   Number of rows to fetch per request. Default value is `50`.

`retry_count`
:   Number of retry attempts after failed request to Oracle Database. Default value is `3`.


### Deployment using Docker [es-connectors-oracle-client-docker]

You can deploy the Oracle connector as a self-managed connector using Docker. Follow these instructions.

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
    service_type: oracle
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



### Documents and syncs [es-connectors-oracle-client-documents-syncs]

* Tables with no primary key defined are skipped.
* If the table’s system change number (SCN) value is not between the `min(SCN)` and `max(SCN)` values of the `SMON_SCN_TIME` table, the connector will not be able to retrieve the most recently updated time. Data will therefore index in every sync. For more details refer to the following [discussion thread](https://community.oracle.com/tech/apps-infra/discussion/4076446/show-error-about-ora-08181-specified-number-is-not-a-valid-system-change-number-when-using-scn-t).
* The `sys` user is not supported, as it contains 1000+ system tables. If you need to work with the `sys` user, use either `sysdba` or `sysoper` and configure this as the username.

::::{note}
* Files bigger than 10 MB won’t be extracted.
* Permissions are not synced. **All documents** indexed to an Elastic deployment will be visible to **all users with access** to that Elastic Deployment.

::::



### Sync rules [es-connectors-oracle-client-sync-rules]

[Basic sync rules](/reference/search-connectors/es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.

Advanced sync rules are not available for this connector in the present version. Currently, filtering is controlled by ingest pipelines.


### Self-managed connector operations [es-connectors-oracle-client-operations]


#### End-to-end testing [es-connectors-oracle-client-operations-testing]

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](/reference/search-connectors/self-managed-connectors.md#es-build-connector-testing) for more details.

To execute a functional test for the Oracle connector, run the following command:

```shell
make ftest NAME=oracle
```

By default, this will use a medium-sized dataset. To make the test faster add the `DATA_SIZE=small` argument:

```shell
make ftest NAME=oracle DATA_SIZE=small
```


### Known issues [es-connectors-oracle-client-known-issues]

There are no known issues for this connector.

See [Known issues](/release-notes/known-issues.md) for any issues affecting all connectors.


### Troubleshooting [es-connectors-oracle-client-troubleshooting]

See [Troubleshooting](/reference/search-connectors/es-connectors-troubleshooting.md).


### Security [es-connectors-oracle-client-security]

See [Security](/reference/search-connectors/es-connectors-security.md).


### Framework and source [es-connectors-oracle-client-source]

This connector is built with the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

This connector uses the [generic database connector source code](https://github.com/elastic/connectors/blob/master/connectors/sources/generic_database.py) (branch *main*, compatible with Elastic *9.0*).

View [additional code specific to this data source](https://github.com/elastic/connectors/tree/main/connectors/sources/oracle.py) (branch *main*, compatible with Elastic *9.0*).