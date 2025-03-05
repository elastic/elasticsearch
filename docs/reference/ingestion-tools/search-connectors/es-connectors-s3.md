---
navigation_title: "S3"
---

# Elastic S3 connector reference [es-connectors-s3]


%  Attributes used in this file:

The *Elastic S3 connector* is a [connector](es-connectors.md) for [Amazon S3](https://aws.amazon.com/s3/) data sources.

%  //////// //// //// //// //// //// //// ////////

%  //////// NATIVE CONNECTOR REFERENCE (MANAGED SERVICE) ///////

%  //////// //// //// //// //// //// //// ////////


## **Elastic managed connector reference** [es-connectors-s3-native-connector-reference] 

::::::{dropdown} View **Elastic managed connector** reference

### Availability and prerequisites [es-connectors-s3-prerequisites] 

This connector is available natively in Elastic Cloud as of version **8.12.0**. To use this connector, satisfy all [managed connector requirements](es-native-connectors.md).


### Create a Amazon S3 connector [es-connectors-s3-create-native-connector] 


## Use the UI [es-connectors-s3-create-use-the-ui] 

To create a new Amazon S3 connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](https://www.elastic.co/guide/en/kibana/current/kibana-concepts-analysts.html#_finding_your_apps_and_objects).
2. Follow the instructions to create a new native  **Amazon S3** connector.

For additional operations, see [*Connectors UI in {{kib}}*](es-connectors-usage.md).


## Use the API [es-connectors-s3-create-use-the-api] 

You can use the {{es}} [Create connector API](https://www.elastic.co/guide/en/elasticsearch/reference/current/connector-apis.html) to create a new native Amazon S3 connector.

For example:

```console
PUT _connector/my-s3-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Amazon S3",
  "service_type": "s3",
  "is_native": true
}
```

%  TEST[skip:can’t test in isolation]

:::::{dropdown} You’ll also need to **create an API key** for the connector to use.
::::{note} 
The user needs the cluster privileges `manage_api_key`, `manage_connector` and `write_connector_secrets` to generate API keys programmatically.

::::


To create an API key for the connector:

1. Run the following command, replacing values where indicated. Note the `id` and `encoded` return values from the response:

    ```console
    POST /_security/api_key
    {
      "name": "my-connector-api-key",
      "role_descriptors": {
        "my-connector-connector-role": {
          "cluster": [
            "monitor",
            "manage_connector"
          ],
          "indices": [
            {
              "names": [
                "my-index_name",
                ".search-acl-filter-my-index_name",
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

2. Use the `encoded` value to store a connector secret, and note the `id` return value from this response:

    ```console
    POST _connector/_secret
    {
      "value": "encoded_api_key"
    }
    ```


%  TEST[skip:need to retrieve ids from the response]

+ . Use the API key `id` and the connector secret `id` to update the connector:

+

```console
PUT /_connector/my_connector_id>/_api_key_id
{
  "api_key_id": "API key_id",
  "api_key_secret_id": "secret_id"
}
```

%  TEST[skip:need to retrieve ids from the response]

:::::


Refer to the [{{es}} API documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/connector-apis.html) for details of all available Connector APIs.


### Usage [es-connectors-s3-usage] 

To use this managed connector, see [*Elastic managed connectors*](es-native-connectors.md).

For additional operations, see [*Connectors UI in {{kib}}*](es-connectors-usage.md).

S3 users will also need to [Create an IAM identity](es-connectors-s3.md#es-connectors-s3-usage-create-iam)


#### Create an IAM identity [es-connectors-s3-usage-create-iam] 

Users need to create an IAM identity to use this connector as a **self-managed connector**. Refer to [the AWS documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-set-up.md).

The [policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.md) associated with the IAM identity must have the following **AWS permissions**:

* `ListAllMyBuckets`
* `ListBucket`
* `GetBucketLocation`
* `GetObject`


### Compatibility [es-connectors-s3-compatibility] 

Currently the connector does not support S3-compatible vendors.


### Configuration [es-connectors-s3-configuration] 

The following configuration fields are required to **set up** the connector:

AWS Buckets
:   List of S3 bucket names. `*` will fetch data from all buckets. Examples:

    * `testbucket, prodbucket`
    * `testbucket`
    * `*`


::::{note} 
This field is ignored when using advanced sync rules.

::::


AWS Access Key ID
:   Access Key ID for the AWS identity that will be used for bucket access.

AWS Secret Key
:   Secret Access Key for the AWS identity that will be used for bucket access.


### Documents and syncs [es-connectors-s3-documents-syncs] 

::::{note} 
* Content from files bigger than 10 MB won’t be extracted. (Self-managed connectors can use the [self-managed local extraction service](es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.)
* Permissions are not synced. ***All documents*** indexed to an Elastic deployment will be visible to ***all users with access*** to that Elastic Deployment.

::::



### Sync rules [es-connectors-s3-sync-rules] 

[Basic sync rules](es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


#### Advanced sync rules [es-connectors-s3-sync-rules-advanced] 

::::{note} 
A [full sync](es-connectors-sync-types.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


Advanced sync rules are defined through a source-specific DSL JSON snippet.

Use advanced sync rules to filter data to be fetched from Amazon S3 buckets. They take the following parameters:

1. `bucket`: S3 bucket the rule applies to.
2. `extension` (optional): Lists which file types to sync. Defaults to syncing all types.
3. `prefix` (optional): String of prefix characters. The connector will fetch file and folder data that matches the string. Defaults to `""` (syncs all bucket objects).

$$$es-connectors-s3-sync-rules-advanced-examples$$$
**Advanced sync rules examples**

**Fetching files and folders recursively by prefix**

**Example**: Fetch files/folders in `folder1/docs`.

```js
[
  {
    "bucket": "bucket1",
    "prefix": "folder1/docs"
  }

]
```

%  NOTCONSOLE

**Example**: Fetch files/folder starting with `folder1`.

```js
[
  {
    "bucket": "bucket2",
    "prefix": "folder1"
  }
]
```

%  NOTCONSOLE

**Fetching files and folders by specifying extensions**

**Example**: Fetch all objects which start with `abc` and then filter using file extensions.

```js
[
  {
    "bucket": "bucket2",
    "prefix": "abc",
    "extension": [".txt", ".png"]
  }
]
```

%  NOTCONSOLE


### Content extraction [es-connectors-s3-content-extraction] 

See [Content extraction](es-connectors-content-extraction.md).


### Known issues [es-connectors-s3-known-issues] 

There are no known issues for this connector.

See [Known issues](es-connectors-known-issues.md) for any issues affecting all connectors.


### Troubleshooting [es-connectors-s3-troubleshooting] 

See [Troubleshooting](es-connectors-troubleshooting.md).


### Security [es-connectors-s3-security] 

See [Security](es-connectors-security.md).


### Framework and source [es-connectors-s3-source] 

This connector is built with the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/s3.py) (branch *main*, compatible with Elastic *9.0*).

%  Closing the collapsible section

::::::


%  //////// //// //// //// //// //// //// ////////

%  //////// CONNECTOR CLIENT REFERENCE (SELF-MANAGED) ///////

%  //////// //// //// //// //// //// //// ////////


## **Self-managed connector reference** [es-connectors-s3-connector-client-reference] 

::::::{dropdown} View **self-managed connector** reference

### Availability and prerequisites [es-connectors-s3-client-prerequisites] 

This connector is available as a self-managed **self-managed connector**. This self-managed connector is compatible with Elastic versions **8.6.0+**. To use this connector, satisfy all [self-managed connector requirements](es-build-connector.md).


### Create a Amazon S3 connector [es-connectors-s3-create-connector-client] 


## Use the UI [es-connectors-s3-client-create-use-the-ui] 

To create a new Amazon S3 connector:

1. In the Kibana UI, navigate to the **Search → Content → Connectors** page from the main menu, or use the [global search field](https://www.elastic.co/guide/en/kibana/current/kibana-concepts-analysts.html#_finding_your_apps_and_objects).
2. Follow the instructions to create a new  **Amazon S3** self-managed connector.


## Use the API [es-connectors-s3-client-create-use-the-api] 

You can use the {{es}} [Create connector API](https://www.elastic.co/guide/en/elasticsearch/reference/current/connector-apis.html) to create a new self-managed Amazon S3 self-managed connector.

For example:

```console
PUT _connector/my-s3-connector
{
  "index_name": "my-elasticsearch-index",
  "name": "Content synced from Amazon S3",
  "service_type": "s3"
}
```

%  TEST[skip:can’t test in isolation]

:::::{dropdown} You’ll also need to **create an API key** for the connector to use.
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


Refer to the [{{es}} API documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/connector-apis.html) for details of all available Connector APIs.


### Usage [es-connectors-s3-client-usage] 

To use this connector as a **self-managed connector**, see [*Self-managed connectors*](es-build-connector.md).

For additional operations, see [*Connectors UI in {{kib}}*](es-connectors-usage.md).

S3 users will also need to [Create an IAM identity](es-connectors-s3.md#es-connectors-s3-client-usage-create-iam)


#### Create an IAM identity [es-connectors-s3-client-usage-create-iam] 

Users need to create an IAM identity to use this connector as a **self-managed connector**. Refer to [the AWS documentation](https://docs.aws.amazon.com/IAM/latest/UserGuide/getting-set-up.md).

The [policy](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.md) associated with the IAM identity must have the following **AWS permissions**:

* `ListAllMyBuckets`
* `ListBucket`
* `GetBucketLocation`
* `GetObject`


### Compatibility [es-connectors-s3-client-compatibility] 

Currently the connector does not support S3-compatible vendors.


### Configuration [es-connectors-s3-client-configuration] 

::::{tip} 
When using the [self-managed connector](es-build-connector.md) workflow, these fields will use the default configuration set in the [connector source code](https://github.com/elastic/connectors/blob/a5976d20cd8277ae46511f7176662afc889e56ec/connectors/sources/s3.py#L231-L258). These configurable fields will be rendered with their respective **labels** in the Kibana UI. Once connected, you’ll be able to update these values in Kibana.

::::


The following configuration fields are required to **set up** the connector:

`buckets`
:   List of S3 bucket names. `*` will fetch data from all buckets. Examples:

    * `testbucket, prodbucket`
    * `testbucket`
    * `*`


::::{note} 
This field is ignored when using advanced sync rules.

::::


`aws_access_key_id`
:   Access Key ID for the AWS identity that will be used for bucket access.

`aws_secret_access_key`
:   Secret Access Key for the AWS identity that will be used for bucket access.

`read_timeout`
:   The `read_timeout` for Amazon S3. Default value is `90`.

`connect_timeout`
:   Connection timeout for crawling S3. Default value is `90`.

`max_attempts`
:   Maximum retry attempts. Default value is `5`.

`page_size`
:   Page size for iterating bucket objects in Amazon S3. Default value is `100`.


### Deployment using Docker [es-connectors-s3-client-docker] 

You can deploy the Amazon S3 connector as a self-managed connector using Docker. Follow these instructions.

::::{dropdown} **Step 1: Download sample configuration file**
Download the sample configuration file. You can either download it manually or run the following command:

```sh
curl https://raw.githubusercontent.com/elastic/connectors/main/config.yml.example --output ~/connectors-config/config.yml
```

%  NOTCONSOLE

Remember to update the `--output` argument value if your directory name is different, or you want to use a different config file name.

::::


::::{dropdown} **Step 2: Update the configuration file for your self-managed connector**
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
    service_type: s3
    api_key: <CONNECTOR_API_KEY_FROM_KIBANA> # Optional. If not provided, the connector will use the elasticsearch.api_key instead
```

Using the `elasticsearch.api_key` is the recommended authentication method. However, you can also use `elasticsearch.username` and `elasticsearch.password` to authenticate with your Elasticsearch instance.

Note: You can change other default configurations by simply uncommenting specific settings in the configuration file and modifying their values.

::::


::::{dropdown} **Step 3: Run the Docker image**
Run the Docker image with the Connector Service using the following command:

```sh
docker run \
-v ~/connectors-config:/config \
--network "elastic" \
--tty \
--rm \
docker.elastic.co/integrations/elastic-connectors:9.0.0-beta1.0 \
/app/bin/elastic-ingest \
-c /config/config.yml
```

::::


Refer to [`DOCKER.md`](https://github.com/elastic/connectors/tree/main/docs/DOCKER.md) in the `elastic/connectors` repo for more details.

Find all available Docker images in the [official registry](https://www.docker.elastic.co/r/integrations/elastic-connectors).

::::{tip} 
We also have a quickstart self-managed option using Docker Compose, so you can spin up all required services at once: Elasticsearch, Kibana, and the connectors service. Refer to this [README](https://github.com/elastic/connectors/tree/main/scripts/stack#readme) in the `elastic/connectors` repo for more information.

::::



### Documents and syncs [es-connectors-s3-client-documents-syncs] 

::::{note} 
* Content from files bigger than 10 MB won’t be extracted by default. You can use the [self-managed local extraction service](es-connectors-content-extraction.md#es-connectors-content-extraction-local) to handle larger binary files.
* Permissions are not synced. ***All documents*** indexed to an Elastic deployment will be visible to ***all users with access*** to that Elastic Deployment.

::::



### Sync rules [es-connectors-s3-client-sync-rules] 

[Basic sync rules](es-sync-rules.md#es-sync-rules-basic) are identical for all connectors and are available by default.


#### Advanced sync rules [es-connectors-s3-client-sync-rules-advanced] 

::::{note} 
A [full sync](es-connectors-sync-types.md#es-connectors-sync-types-full) is required for advanced sync rules to take effect.

::::


Advanced sync rules are defined through a source-specific DSL JSON snippet.

Use advanced sync rules to filter data to be fetched from Amazon S3 buckets. They take the following parameters:

1. `bucket`: S3 bucket the rule applies to.
2. `extension` (optional): Lists which file types to sync. Defaults to syncing all types.
3. `prefix` (optional): String of prefix characters. The connector will fetch file and folder data that matches the string. Defaults to `""` (syncs all bucket objects).

$$$es-connectors-s3-client-sync-rules-advanced-examples$$$
**Advanced sync rules examples**

**Fetching files and folders recursively by prefix**

**Example**: Fetch files/folders in `folder1/docs`.

```js
[
  {
    "bucket": "bucket1",
    "prefix": "folder1/docs"
  }

]
```

%  NOTCONSOLE

**Example**: Fetch files/folder starting with `folder1`.

```js
[
  {
    "bucket": "bucket2",
    "prefix": "folder1"
  }
]
```

%  NOTCONSOLE

**Fetching files and folders by specifying extensions**

**Example**: Fetch all objects which start with `abc` and then filter using file extensions.

```js
[
  {
    "bucket": "bucket2",
    "prefix": "abc",
    "extension": [".txt", ".png"]
  }
]
```

%  NOTCONSOLE


### Content extraction [es-connectors-s3-client-content-extraction] 

See [Content extraction](es-connectors-content-extraction.md).


### End-to-end testing [es-connectors-s3-client-testing] 

The connector framework enables operators to run functional tests against a real data source. Refer to [Connector testing](es-build-connector.md#es-build-connector-testing) for more details.

To execute a functional test for the Amazon S3 **self-managed connector**, run the following command:

```shell
make ftest NAME=s3
```

By default, this will use a medium-sized dataset. To make the test faster add the `DATA_SIZE=small` argument:

```shell
make ftest NAME=s3 DATA_SIZE=small
```


### Known issues [es-connectors-s3-client-known-issues] 

There are no known issues for this connector.

See [Known issues](es-connectors-known-issues.md) for any issues affecting all connectors.


### Troubleshooting [es-connectors-s3-client-troubleshooting] 

See [Troubleshooting](es-connectors-troubleshooting.md).


### Security [es-connectors-s3-client-security] 

See [Security](es-connectors-security.md).


### Framework and source [es-connectors-s3-client-source] 

This connector is built with the [Elastic connector framework](https://github.com/elastic/connectors/tree/main).

View the [source code for this connector](https://github.com/elastic/connectors/tree/main/connectors/sources/s3.py) (branch *main*, compatible with Elastic *9.0*).

%  Closing the collapsible section

::::::


