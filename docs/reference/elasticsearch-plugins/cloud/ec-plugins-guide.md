---
mapped_pages:
  - https://www.elastic.co/guide/en/cloud/current/ec-plugins-guide.html
applies_to:
  deployment:
    ess: ga
---

# Managing plugins and extensions through the API [ec-plugins-guide]

This guide provides a full list of tasks for managing [plugins and extensions](../plugin-management.md) in Elasticsearch Service, using the API.

* [Create an extension](ec-plugins-guide.md#ec-extension-guide-create)
* [Add an extension to a deployment plan](ec-plugins-guide.md#ec-extension-guide-add-plan)
* [Get an extension](ec-plugins-guide.md#ec-extension-guide-get-extension)
* [Update the name of an existing extension](ec-plugins-guide.md#ec-extension-guide-update-name)
* [Update the type of an existing extension](ec-plugins-guide.md#ec-extension-guide-update-type)
* [Update the version of an existing bundle](ec-plugins-guide.md#ec-extension-guide-update-version-bundle)
* [Update the version of an existing plugin](ec-plugins-guide.md#ec-extension-guide-update-version-plugin)
* [Update the file associated to an existing extension](ec-plugins-guide.md#ec-extension-guide-update-file)
* [Upgrade Elasticsearch](ec-plugins-guide.md#ec-extension-guide-upgrade-elasticsearch)
* [Delete an extension](ec-plugins-guide.md#ec-extension-guide-delete)


## Create an extension [ec-extension-guide-create]

There are two methods to create an extension. You can:

1. Stream the file from a publicly-accessible download URL.
2. Upload the file from a local file path.

::::{note}
For plugins larger than 200MB the download URL option **must** be used. Plugins larger than 8GB cannot be uploaded with either method.
::::


These two examples are for the `plugin` extension type. For bundles, change `extension_type` to `bundle`.

For plugins, `version` must match (exactly) the `elasticsearch.version` field defined in the plugin’s `plugin-descriptor.properties` file. Check [Help for plugin authors](/extend/index.md) for details. For plugins larger than 5GB, the `plugin-descriptor.properties` file needs to be at the top of the archive. This ensures that the our verification process is able to detect that it is an Elasticsearch plugin; otherwise the plugin will be rejected by the API. This order can be achieved by specifying at time of creating the ZIP file: `zip -r name-of-plugin.zip plugin-descriptor.properties *`.

For bundles, we recommend setting `version` using wildcard notation that matches the major version of the Elasticsearch deployment. For example, if Elasticsearch is on version 8.4.3, simply set `8.*` as the version. The value `8.*` means that the bundle is compatible with all 8.x versions of Elasticsearch.

$$$ec-extension-guide-create-option1$$$
**Option 1: Stream the file from a publicly-accessible download URL**

```sh
curl -X POST \
  https://api.elastic-cloud.com/api/v1/deployments/extensions \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
   "download_url" : "https://my_site/custom-plugin-8.4.3.zip",
   "extension_type" : "plugin",
   "name" : "custom-plugin",
   "version" : "8.4.3"
}'
```

The single POST request creates an extension with the metadata, validates, and streams the file from the `download_url` specified. The accepted protocols for `download_url` are `http` and `https`.

::::{note}
The `download_url` must be directly and publicly accessible. There is currently no support for redirection or authentication unless it contains security credentials/tokens expected by your HTTP service as part of the URL. Otherwise, use the following Option 2 to upload the file from a local path.
::::


::::{note}
When the file is larger than 5GB, the request may timeout after 2-5 minutes, but streaming will continue on the server. Check the Extensions page in the Cloud UI after 5-10 minutes to make sure that the plugin has been created. A successfully created plugin will contain correct name, type, version, size, and last modified information.
::::


$$$ec-extension-guide-create-option2$$$
**Option 2: Upload the file from a local file path**

This option requires a two step process. First, create the metadata for the extension:

```sh
curl -X POST \
  https://api.elastic-cloud.com/api/v1/deployments/extensions \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "extension_type": "plugin",
    "name": "custom-plugin",
    "version" : "8.4.3"
}'
```

```sh
{
    "url": "repo://4226448541",
    "version": "8.4.3",
    "extension_type": "plugin",
    "id": "4226448541",
    "name": "custom-plugin"
}
```

The response returns a `url` you can reference later in the plan (the numeric value in the `url` is the `EXTENSION_ID`). Use this `EXTENSION_ID` in the following PUT call:

```sh
curl -v -X PUT "https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID" \
-H 'Content-type:application/zip' \
-H "Authorization: ApiKey $CLOUD_API_KEY" \
-H 'Expect:' \
-T "/path_to/custom-plugin-8.4.3.zip"
```

::::{note}
When using curl, always use the `-T` option.  DO NOT use `-F` (we have seen inconsistency in curl behavior across systems; using `-F` can result in partially uploaded or truncated files).
::::


The above PUT request uploads the file from the local path specified. This request is synchronous. An HTTP 200 response indicates that the file has been successfully uploaded and is ready for use.

```sh
{
    "url": "repo://2286113333",
    "version": "8.4.3",
    "extension_type": "plugin",
    "id": "2286113333",
    "name": "custom-plugin"
}
```


## Add an extension to a deployment plan [ec-extension-guide-add-plan]

Once the extension is created and uploaded, you can add the extension using its `EXTENSION_ID` in an [update deployment API call](https://www.elastic.co/docs/api/doc/cloud/operation/operation-update-deployment).

The following are examples of a GCP plan. Your specific deployment plan will be different. The important parts related to extensions are in the `user_plugins` object.

```sh
{
    "name": "Extensions",
    "prune_orphans": false,
    "resources": {
        "elasticsearch": [
            {
                "region": "gcp-us-central1",
                "ref_id": "main-elasticsearch",
                "plan": {
                    "cluster_topology": [

                      ...

                    ],
                    "elasticsearch": {
                        "version": "8.4.3",
                        "enabled_built_in_plugins": [ ],
                      "user_bundles": [
                        {
                          "name": "custom-plugin",
                          "url": "repo://2286113333",
                          "elasticsearch_version": "8.4.3"
                        }
                      ]
                    },
                    "deployment_template": {
                        "id": "gcp-storage-optimized-v3"
                    }
                }
            }
        ]
    }
}
```

You can use the [cat plugins API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-plugins) to confirm that the plugin has been deployed successfully to Elasticsearch.

The previous examples are for plugins. For bundles, use the `user_bundles` construct instead.

```sh
      "user_bundles": [
              {
                    "elasticsearch_version": "8.*",
                    "name": "custom-bundle",
                    "url": "repo://5886113212"
              }
       ]
```


## Get an extension [ec-extension-guide-get-extension]

You can use the GET call to retrieve information about an extension.

To list all extensions for the account:

```sh
curl -X GET \
  https://api.elastic-cloud.com/api/v1/deployments/extensions \
  -H 'Content-Type: application/json' \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
```

To get a specific extension:

```sh
curl -X GET \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID \
  -H 'Content-Type: application/json' \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
```

The previous GET calls support an optional `include_deployments` parameter. When set to `true`, the call also returns the deployments that currently have the extension in-use:

```sh
curl -X GET \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID?include_deployments=true \
  -H 'Content-Type: application/json' \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
```

For example, the previous call returns:

```sh
{
    "name": "custom-plugin",
    "url": "repo://2286113333",
    "extension_type": "plugin",
    "deployments": [
        "f91f3a9360a74e9d8c068cd2698c92ea"
    ],
    "version": "8.4.3",
    "id": "2286113333"
}
```


## Update the name of an existing extension [ec-extension-guide-update-name]

To update the name of an existing extension, simply update the name field without uploading a new file. You do not have to specify the `download_url` when only making metadata changes to an extension.

Example using the [Option 1](ec-plugins-guide.md#ec-extension-guide-create-option1) create an extension method:

```sh
curl -X POST \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
   "extension_type" : "plugin",
    "name": "custom-plugin-07012020",
   "version" : "8.4.3"
}'
```

Example using the [Option 2](ec-plugins-guide.md#ec-extension-guide-create-option2) create an extension method:

```sh
curl -X POST \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
   "extension_type" : "plugin",
    "name": "custom-plugin-07012020",
   "version" : "8.4.3"
}'
```

Updating the name of an existing extension does not change its `EXTENSION_ID`.


## Update the type of an existing extension [ec-extension-guide-update-type]

Updating `extension_type` has no effect. You cannot change the extension’s type (`plugin` versus `bundle`) after the initial creation of a plugin.


## Update the version of an existing bundle [ec-extension-guide-update-version-bundle]

For bundles, we recommend setting `version` using wildcard notation that matches the major version of the Elasticsearch deployment. For example, if Elasticsearch is on version 8.4.3, simply set `8.*` as the version. The value `8.*` means that the bundle is compatible with all 7.x versions of Elasticsearch.

For example, if the bundle was previously uploaded with the version `8.4.2`, simply update the version field. You no longer have to specify the `download_url` when only making metadata changes to a bundle.

Example using the [Option 1](ec-plugins-guide.md#ec-extension-guide-create-option1) create an extension method:

```sh
curl -X POST \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
   "extension_type" : "bundle",
    "name": "custom-bundle",
   "version" : "8.*"
}'
```

Example using the [Option 2](ec-plugins-guide.md#ec-extension-guide-create-option2) create an extension method:

```sh
curl -X POST \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
    "extension_type" : "bundle",
    "name": "custom-bundle",
    "version" : "8.*"
}'
```

Updating the name of an existing extension does not change its `EXTENSION_ID`.


## Update the version of an existing plugin [ec-extension-guide-update-version-plugin]

For plugins, `version` must match (exactly) the `elasticsearch.version` field defined in the plugin’s `plugin-descriptor.properties` file.  Check [Help for plugin authors](/extend/index.md) for details. If you change the version, the associated plugin file *must* also be updated accordingly.


## Update the file associated to an existing extension [ec-extension-guide-update-file]

You may want to update an uploaded file for an existing extension without performing an Elasticsearch upgrade. If you are updating the extension to prepare for an Elasticsearch upgrade, check the [Upgrade Elasticsearch](ec-plugins-guide.md#ec-extension-guide-upgrade-elasticsearch) scenario later on this page.

This example is for the `plugin` extension type. For bundles, change `extension_type` to `bundle`.

If you used [Option 1](ec-plugins-guide.md#ec-extension-guide-create-option1) to create the extension, simply re-run the POST request with the `download_url` pointing to the location of your updated extension file.

```sh
curl -X POST \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{
   "download_url" : "https://my_site/custom-plugin-8.4.3-10212022.zip",
   "extension_type" : "plugin",
    "name": "custom-plugin-10212022",
   "version" : "8.4.3"
}'
```

If you used [Option 2](ec-plugins-guide.md#ec-extension-guide-create-option2) to create the extension, simply re-run the PUT request with the `file` parameter pointing to the location of your updated extension file.

```sh
curl -v -X PUT "https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID" \
-H 'Content-type:application/zip' \
-H "Authorization: ApiKey $CLOUD_API_KEY" \
-H 'Expect:' \
-T "/path_to/custom-plugin-8.4.3-10212022.zip"
```

::::{important}
If you are not making any other plan changes and simply updating an extension file, you need to issue a no-op plan so that Elasticsearch will make use of this new file. A *no-op* (no operation) plan triggers a rolling restart on the deployment, applying the same (unchanged) plan as the current plan.
::::


Updating the file of an existing extension or bundle does not change its `EXTENSION_ID`.


## Upgrade Elasticsearch [ec-extension-guide-upgrade-elasticsearch]

When you upgrade Elasticsearch in a deployment, you must ensure that:

* Bundles are on versions that are compatible with the Elasticsearch version that you are upgrading to.
* Plugins match (exactly) the Elasticsearch upgrade version.

**To prepare existing bundle and update the plan:**

1. **Update the bundle version to be compatible with the Elasticsearch upgrade version.**

    Bundles using wildcard notation for versions (for example, `7.*`, `8.*`) in their extension metadata are compatible with all minor versions of the same Elasticsearch major version. In other words, if you are performing a patch (for example, from `8.4.2` to `8.4.3`) or a minor (for example `8.3.0` to `8.4.3`) version upgrade of Elasticsearch and you are already using `8.*` as the `version` for the extension, you are ready for the Elasticsearch upgrade and can proceed to Step 2.

    However, if you are using a specific `version` for bundles, or upgrading to a major version, you must update the metadata of the extension to specify the matching Elasticsearch `version` that you are upgrading to, or use the wildcard syntax described in the previous paragraph. For example, if you are upgrading from version 7.x to 8.x, set `version` to `8.*` before the upgrade. Refer to [Update the version of an existing bundle](ec-plugins-guide.md#ec-extension-guide-update-version-bundle).

2. **Update the bundle reference as part of an upgrade plan.**

    Submit a plan change that performs the following operations in a *single* [update deployment API](https://www.elastic.co/docs/api/doc/cloud/operation/operation-update-deployment) call:

    * Upgrade the version of Elasticsearch to the upgrade version (for example, `8.4.3`).
    * Update reference to the existing bundle to be compatible with Elasticsearch upgrade version (for example, `8.*`).

    This triggers a rolling upgrade plan change to the later Elasticsearch version and updates the reference to the bundle at the same time.

    The following example shows the upgrade of an Elasticsearch deployment and its bundle. You can also upgrade other deployment resources within the same plan change.

    Update `resources.elasticsearch.plan.elasticsearch.version` and `resources.elasticsearch.plan.cluster_topology.elasticsearch.user_bundles.elasticsearch_version` accordingly.

    ```sh
    {
        "name": "Extensions",
        "prune_orphans": false,
        "resources": {
            "elasticsearch": [
                {
                    "region": "gcp-us-central1",
                    "ref_id": "main-elasticsearch",
                    "plan": {
                        "cluster_topology": [
                          ...
                        ],
                        "elasticsearch": {
                            "version": "8.4.3",
                            "enabled_built_in_plugins": [],
                            "user_bundles": [
                                {
                                      "elasticsearch_version": "7.*",
                                      "name": "custom-bundle",
                                      "url": "repo://5886113212"
                                }
                            ]

                        },
                        "deployment_template": {
                            "id": "gcp-storage-optimized-v3"
                        }
                    }
                }
            ]
        }
    }
    ```


**To create a new plugin and update the plan:**

Unlike bundles, plugins *must* match the Elasticsearch version down to the patch level (for example, `8.4.3`). When upgrading Elasticsearch to a new patch, minor, or major version, update the version in the extension metadata and update the extension file. The following example updates an existing plugin and upgrades the Elasticsearch deployment from version 8.3.0 to 8.4.3.

1. **Create a new plugin that matches the Elasticsearch upgrade version.**

    Follow the steps in [Get an extension](ec-plugins-guide.md#ec-extension-guide-get-extension) to create a new extension with a `version` metadata field and the plugin’s `elasticsearch.version` field in `plugin-descriptor.properties` that matches the Elasticsearch upgrade version (for example, `8.4.3`).

2. **Remove the old plugin and add the new plugin to the upgrade plan.**

    Submit a plan change that performs the following operations in a *single* [update deployment API](https://www.elastic.co/docs/api/doc/cloud/operation/operation-update-deployment) call:

    * Upgrade the version of Elasticsearch to the upgrade version (for example, `8.4.3`).
    * Remove reference to the the plugin on the older version (for example, `8.3.0`) from the plan.
    * Add reference to the new plugin on the upgrade version (for example, `8.4.3`) to the plan.

    This triggers a rolling upgrade plan change to the later Elasticsearch version, removes reference to the older plugin, and deploys your updated plugin at the same time.

    The following example shows the upgrade of an Elasticsearch deployment and its plugin. You can also upgrade other deployment resources within the same plan change.

    Update deployment plans, update `resources.elasticsearch.plan.elasticsearch.version` and `resources.elasticsearch.plan.cluster_topology.elasticsearch.user_plugins.elasticsearch_version` accordingly.

    ```sh
    {
        "name": "Extensions",
        "prune_orphans": false,
        "resources": {
            "elasticsearch": [
                {
                    "region": "gcp-us-central1",
                    "ref_id": "main-elasticsearch",
                    "plan": {
                        "cluster_topology": [
                          ...
                        ],
                        "elasticsearch": {
                            "version": "8.4.3",
                            "enabled_built_in_plugins": [],
                            "user_plugins": [
                               {
                                    "elasticsearch_version": "8.4.3",
                                    "name": "custom-plugin",
                                    "url": "repo://4226448541"
                                }
                            ]

                        },
                        "deployment_template": {
                            "id": "gcp-storage-optimized-v3"
                        }
                    }
                }
            ]
        }
    }
    ```

    You can use the [cat plugins API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cat-plugins) to confirm that the plugin has been upgraded successfully to Elasticsearch.



## Delete an extension [ec-extension-guide-delete]

You can delete an extension simply by calling a DELETE against the EXTENSION_ID of interest:

```sh
curl -X DELETE \
  https://api.elastic-cloud.com/api/v1/deployments/extensions/EXTENSION_ID \
  -H "Authorization: ApiKey $CLOUD_API_KEY" \
  -H 'Content-Type: application/json'
```

Only extensions not currently referenced in a deployment plan can be deleted. If you attempt to delete an extension that is in use, you will receive an HTTP 400 Bad Request error like the following, indicating the deployments that are currently using the extension.

```sh
{
    "errors": [
        {
            "message": "Cannot delete extension [EXTENSION_ID]. It is used by deployments [DEPLOYMENT_NAME].",
            "code": "extensions.extension_in_use"
        }
    ]
}
```

To remove an extension reference from a deployment plan, simply update the deployment with the extension reference deleted from the `user_plugins` or `user_bundles` arrays. Check [Add an extension to a deployment plan](ec-plugins-guide.md#ec-extension-guide-add-plan) for where these are specified in the plan.

