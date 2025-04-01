---
mapped_pages:
  - https://www.elastic.co/guide/en/cloud/current/ec-custom-bundles.html
applies_to:
  deployment:
    ess: ga
---

# Upload custom plugins and bundles [ec-custom-bundles]

There are several cases where you might need your own files to be made available to your {{es}} cluster’s nodes:

* Your own custom plugins, or third-party plugins that are not amongst the [officially available plugins](/reference/elasticsearch-plugins/plugin-management.md).
* Custom dictionaries, such as synonyms, stop words, compound words, and so on.
* Cluster configuration files, such as an Identity Provider metadata file used when you [secure your clusters with SAML](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/saml.md).

To facilitate this, we make it possible to upload a ZIP file that contains the files you want to make available. Uploaded files are stored using Amazon’s highly-available S3 service. This is necessary so we do not have to rely on the availability of third-party services, such as the official plugin repository, when provisioning nodes.

Custom plugins and bundles are collectively referred to as extensions.

## Before you begin [ec_before_you_begin_7]

The selected plugins/bundles are downloaded and provided when a node starts. Changing a plugin does not change it for nodes already running it. Refer to [Updating Plugins and Bundles](#ec-update-bundles-and-plugins).

With great power comes great responsibility: your plugins can extend your deployment with new functionality, but also break it. Be careful. We obviously cannot guarantee that your custom code works.

::::{important}
You cannot edit or delete a custom extension after it has been used in a deployment. To remove it from your deployment, you can disable the extension and update your deployment configuration.
::::


Uploaded files cannot be bigger than 20MB for most subscription levels, for Platinum and Enterprise the limit is 8GB.

It is important that plugins and dictionaries that you reference in mappings and configurations are available at all times. For example, if you try to upgrade {{es}} and de-select a dictionary that is referenced in your mapping, the new nodes will be unable to recover the cluster state and function. This is true even if the dictionary is referenced by an empty index you do not actually use.


## Prepare your files for upload [ec-prepare-custom-bundles]

Plugins are uploaded as ZIP files. You need to choose whether your uploaded file should be treated as a *plugin* or as a *bundle*. Bundles are not installed as plugins. If you need to upload both a custom plugin and custom dictionaries, upload them separately.

To prepare your files, create one of the following:

Plugins
:   A plugin is a ZIP file that contains a plugin descriptor file and binaries.

    The plugin descriptor file is called either `stable-plugin-descriptor.properties` for plugins built against the stable plugin API, or `plugin-descriptor.properties` for plugins built against the classic plugin API. A plugin ZIP file should only contain one plugin descriptor file.

    {{es}} assumes that the uploaded ZIP file contains binaries. If it finds any source code, it fails with an error message, causing provisioning to fail. Make sure you upload binaries, and not source code.

    ::::{note}
    Plugins larger than 5GB should have the plugin descriptor file at the top of the archive. This order can be achieved by specifying at time of creating the ZIP file:

    ```sh
    zip -r name-of-plugin.zip name-of-descriptor-file.properties *
    ```

    ::::


Bundles
:   The entire content of a bundle is made available to the node by extracting to the {{es}} container’s `/app/config` directory. This is useful to make custom dictionaries available. Dictionaries should be placed in a `/dictionaries` folder in the root path of your ZIP file.

    Here are some examples of bundles:

    **Script**

    ```text
    $ tree .
    .
    └── scripts
        └── test.js
    ```

    The script `test.js` can be referred in queries as `"script": "test"`.

    **Dictionary of synonyms**

    ```text
    $ tree .
    .
    └── dictionaries
        └── synonyms.txt
    ```

    The dictionary `synonyms.txt` can be used as `synonyms.txt` or using the full path `/app/config/synonyms.txt` in the `synonyms_path` of the `synonym-filter`.

    To learn more about analyzing with synonyms, check [Synonym token filter](/reference/text-analysis/analysis-synonym-tokenfilter.md) and [Formatting Synonyms](https://www.elastic.co/guide/en/elasticsearch/guide/2.x/synonym-formats.html).

    **GeoIP database bundle**

    ```text
    $ tree .
    .
    └── ingest-geoip
        └── MyGeoLite2-City.mmdb
    ```

    Note that the extension must be `-(City|Country|ASN).mmdb`, and it must be a different name than the original file name `GeoLite2-City.mmdb` which already exists in Elasticsearch Service. To use this bundle, you can refer it in the GeoIP ingest pipeline as `MyGeoLite2-City.mmdb` under `database_file`.



## Add your extension [ec-add-your-plugin]

You must upload your files before you can apply them to your cluster configuration:

1. Log in to the [Elasticsearch Service Console](https://cloud.elastic.co?page=docs&placement=docs-body).
2. Find your deployment on the home page in the Elasticsearch Service card and select **Manage** to access it directly. Or, select **Hosted deployments** to go to the deployments page to view all of your deployments.
3. Under **Features**, select **Extensions**.
4. Select **Upload extension**.
5. Complete the extension fields, including the {{es}} version.

    * Plugins must use full version notation down to the patch level, such as `7.10.1`. You cannot use wildcards. This version notation should match the version in your plugin’s plugin descriptor file. For classic plugins, it should also match the target deployment version.
    * Bundles should specify major or minor versions with wildcards, such as `7.*` or `*`. Wildcards are recommended to ensure the bundle is compatible across all versions of these releases.

6. Select the extension **Type**.
7. Under **Plugin file**, choose the file to upload.
8. Select **Create extension**.

After creating your extension, you can [enable them for existing {{es}} deployments](#ec-update-bundles) or enable them when creating new deployments.

::::{note}
Creating extensions larger than 200MB should be done through the extensions API.

Refer to [Managing plugins and extensions through the API](/reference/elasticsearch-plugins/cloud/ec-plugins-guide.md) for more details.

::::



## Update your deployment configuration [ec-update-bundles]

After uploading your files, you can select to enable them when creating a new {{es}} deployment. For existing deployments, you must update your deployment configuration to use the new files:

1. Log in to the [Elasticsearch Service Console](https://cloud.elastic.co?page=docs&placement=docs-body).
2. Find your deployment on the home page in the Elasticsearch Service card and select **Manage** to access it directly. Or, select **Hosted deployments** to go to the deployments page to view all of your deployments.

    On the deployments page you can narrow your deployments by name, ID, or choose from several other filters. To customize your view, use a combination of filters, or change the format from a grid to a list.

3. From the **Actions** dropdown, select **Edit deployment**.
4. Select **Manage user settings and extensions**.
5. Select the **Extensions** tab.
6. Select the custom extension.
7. Select **Back**.
8. Select **Save**. The {{es}} cluster is then updated with new nodes that have the plugin installed.


## Update your extension [ec-update-bundles-and-plugins]

While you can update the ZIP file for any plugin or bundle, these are downloaded and made available only when a node is started.

You should be careful when updating an extension. If you update an existing extension with a new file, and if the file is broken for some reason, all the nodes could be in trouble, as a restart or move node could make even HA clusters non-available.

If the extension is not in use by any deployments, then you are free to update the files or extension details as much as you like. However, if the extension is in use, and if you need to update it with a new file, it is recommended to [create a new extension](#ec-add-your-plugin) rather than updating the existing one that is in use.

By following this method, only the one node would be down even if the extension file is faulty. This would ensure that HA clusters remain available.

This method also supports having a test/staging deployment to test out the extension changes before applying them on a production deployment.

You may delete the old extension after updating the deployment successfully.

To update an extension with a new file version,

1. Prepare a new plugin or bundle.
2. On the **Extensions** page, [upload a new extension](#ec-add-your-plugin).
3. Make your new files available by uploading them.
4. Find your deployment on the home page in the Elasticsearch Service card and select **Manage** to access it directly. Or, select **Hosted deployments** to go to the deployments page to view all of your deployments.

    On the deployments page you can narrow your deployments by name, ID, or choose from several other filters. To customize your view, use a combination of filters, or change the format from a grid to a list.

5. From the **Actions** dropdown, select **Edit deployment**.
6. Select **Manage user settings and extensions**.
7. Select the **Extensions** tab.
8. Select the new extension and de-select the old one.
9. Select **Back**.
10. Select **Save**.


## How to use the extensions API [ec-extension-api-usage-guide]

::::{note}
For a full set of examples, check [Managing plugins and extensions through the API](/reference/elasticsearch-plugins/cloud/ec-plugins-guide.md).
::::


If you don’t already have one, create an [API key](docs-content://deploy-manage/api-keys/elastic-cloud-api-keys.md)

There are ways that you can use the extensions API to upload a file.

### Method 1: Use HTTP `POST` to create metadata and then upload the file using HTTP `PUT` [ec_method_1_use_http_post_to_create_metadata_and_then_upload_the_file_using_http_put]

Step 1: Create metadata

```text
curl -XPOST \
-H "Authorization: ApiKey $EC_API_KEY" \
-H 'content-type:application/json' \
https://api.elastic-cloud.com/api/v1/deployments/extensions \
-d'{
  "name" : "synonyms-v1",
  "description" : "The best synonyms ever",
  "extension_type" : "bundle",
  "version" : "7.*"
}'
```

Step 2: Upload the file

```text
curl -XPUT \
-H "Authorization: ApiKey $EC_API_KEY" \
"https://api.elastic-cloud.com/api/v1/deployments/extensions/$extension_id" \
-T /tmp/synonyms.zip
```

If you are using a client that does not have native `application/zip` handling like `curl`, be sure to use the equivalent of the following with `content-type: multipart/form-data`:

```text
curl -XPUT \
-H 'Expect:' \
-H 'content-type: multipart/form-data' \
-H "Authorization: ApiKey $EC_API_KEY" \
"https://api.elastic-cloud.com/api/v1/deployments/extensions/$extension_id" -F "file=@/tmp/synonyms.zip"
```

For example, using the Python `requests` module, the `PUT` request would be as follows:

```text
import requests
files = {'file': open('/tmp/synonyms.zip','rb')}
r = requests.put('https://api.elastic-cloud.com/api/v1/deployments/extensions/{}'.format(extension_id), files=files, headers= {'Authorization': 'ApiKey {}'.format(EC_API_KEY)})
```


### Method 2: Single step. Use a `download_url` so that the API server downloads the object at the specified URL [ec_method_2_single_step_use_a_download_url_so_that_the_api_server_downloads_the_object_at_the_specified_url]

```text
curl -XPOST \
-H "Authorization: ApiKey $EC_API_KEY" \
-H 'content-type:application/json' \
https://api.elastic-cloud.com/api/v1/deployments/extensions \
-d'{
  "name" : "anylysis_icu",
  "description" : "Helpful description",
  "extension_type" : "plugin",
  "version" : "7.13.2",
  "download_url": "https://artifacts.elastic.co/downloads/elasticsearch-plugins/analysis-icu/analysis-icu-7.13.2.zip"
}'
```

Please refer to the [Extensions API reference](https://www.elastic.co/docs/api/doc/cloud/group/endpoint-extensions) for the complete set of HTTP methods and payloads.



