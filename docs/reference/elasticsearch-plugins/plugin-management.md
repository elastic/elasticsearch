---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management.html
  - https://www.elastic.co/guide/en/cloud/current/ec-adding-plugins.html
  - https://www.elastic.co/guide/en/cloud-enterprise/current/ece-add-plugins.html
---

# Plugin management

## Managing plugins for ECE
[Add plugins and extensions ECE](./cloud-enterprise/ece-add-plugins.md) ?what product?

Plugins extend the core functionality of Elasticsearch. Elastic Cloud Enterprise makes it easy to add plugins to your deployment by providing a number of plugins that work with your version of Elasticsearch. One advantage of these plugins is that you generally don’t have to worry about upgrading plugins when upgrading to a new Elasticsearch version, unless there are breaking changes. The plugins simply are upgraded along with the rest of your deployment.

Adding plugins to a deployment is as simple as selecting it from the list of available plugins, but different versions of Elasticsearch support different plugins. Plugins are available for different purposes, such as:

* National language support, phonetic analysis, and extended unicode support
* Ingesting attachments in common formats and ingesting information about the geographic location of IP addresses
* Adding new field datatypes to Elasticsearch

Additional plugins might be available. If a plugin is listed for your version of Elasticsearch, it can be used.

To add plugins when creating a new deployment:

1. [Log into the Cloud UI](docs-content://deploy-manage/deploy/cloud-enterprise/log-into-cloud-ui.md) and select **Create deployment**.
2. Make your initial deployment selections, then select **Customize Deployment**.
3. Beneath the Elasticsearch master node, expand the **Manage plugins and settings** caret.
4. Select the plugins you want.
5. Select **Create deployment**.

The deployment spins up with the plugins installed.

To add plugins to an existing deployment:

1. [Log into the Cloud UI](docs-content://deploy-manage/deploy/cloud-enterprise/log-into-cloud-ui.md).
2. On the **Deployments** page, select your deployment.

    Narrow the list by name, ID, or choose from several other filters. To further define the list, use a combination of filters.

3. From your deployment menu, go to the **Edit** page.
4. Beneath the Elasticsearch master node, expand the **Manage plugins and settings** caret.
5. Select the plugins that you want.
6. Select **Save changes**.

There is no downtime when adding plugins to highly available deployments. The deployment is updated with new nodes that have the plugins installed.

## Managing plugins for ECH
[Add plugins and extensions ECH](./cloud/ec-adding-plugins.md) ?what product?

Plugins extend the core functionality of {{es}}. There are many suitable plugins, including:

* Discovery plugins, such as the cloud AWS plugin that allows discovering nodes on EC2 instances.
* Analysis plugins, to provide analyzers targeted at languages other than English.
* Scripting plugins, to provide additional scripting languages.

There are two ways to add plugins to a deployment in Elasticsearch Service:

* [Enable one of the official plugins already available in Elasticsearch Service](/reference/elasticsearch-plugins/cloud/ec-adding-elastic-plugins.md\).
* [Upload a custom plugin and then enable it per deployment](/reference/elasticsearch-plugins/cloud/ec-custom-bundles.md\).

Custom plugins can include the official {{es}} plugins not provided with Elasticsearch Service, any of the community-sourced plugins, or [plugins that you write yourself](/extend/index.md). Uploading custom plugins is available only to Gold, Platinum, and Enterprise subscriptions. For more information, check [Upload custom plugins and bundles](/reference/elasticsearch-plugins/cloud/ec-custom-bundles.md\).

To learn more about the official and community-sourced plugins, refer to [{{es}} Plugins and Integrations](/reference/elasticsearch-plugins/index.md).

For a detailed guide with examples of using the Elasticsearch Service API to create, get information about, update, and delete extensions and plugins, check [Managing plugins and extensions through the API](/reference/elasticsearch-plugins/cloud/ec-plugins-guide.md\).

Plugins are not supported for {{kib}}. To learn more, check [Restrictions for {{es}} and {{kib}} plugins](cloud://release-notes/cloud-hosted/known-issues.md#ec-restrictions-plugins).

### Add plugins provided with Elasticsearch Service [ec-adding-elastic-plugins]

You can use a variety of official plugins that are compatible with your version of {{es}}. When you upgrade to a new {{es}} version, these plugins are simply upgraded with the rest of your deployment.

#### Before you begin [ec_before_you_begin_6]

Some restrictions apply when adding plugins. To learn more, check [Restrictions for {{es}} and {{kib}} plugins](cloud://release-notes/cloud-hosted/known-issues.md#ec-restrictions-plugins).

Only Gold, Platinum, Enterprise and Private subscriptions, running version 2.4.6 or later, have access to uploading custom plugins. All subscription levels, including Standard, can upload scripts and dictionaries.

To enable a plugin for a deployment:

1. Log in to the [Elasticsearch Service Console](https://cloud.elastic.co?page=docs&placement=docs-body).
2. Find your deployment on the home page in the Elasticsearch Service card and select **Manage** to access it directly. Or, select **Hosted deployments** to go to the deployments page to view all of your deployments.

    On the deployments page you can narrow your deployments by name, ID, or choose from several other filters. To customize your view, use a combination of filters, or change the format from a grid to a list.

3. From the **Actions** dropdown, select **Edit deployment**.
4. Select **Manage user settings and extensions**.
5. Select the **Extensions** tab.
6. Select the plugins that you want to enable.
7. Select **Back**.
8. Select **Save**. The {{es}} cluster is then updated with new nodes that have the plugin installed.

## Managing plugins for self-managed deployments
Use the `elasticsearch-plugin` command line tool to install, list, and remove plugins. It is located in the `$ES_HOME/bin` directory by default but it may be in a different location depending on which Elasticsearch package you installed. For more information, see [Plugins directory](_plugins_directory.md)

Run the following command to get usage instructions:

``` 
sudo bin/elasticsearch-plugin -h
```

:::{important} Running as root
If Elasticsearch was installed using the deb or rpm package then run `/usr/share/elasticsearch/bin/elasticsearch-plugin` as `root` so it can write to the appropriate files on disk. Otherwise run `bin/elasticsearch-plugin` as the user that owns all of the Elasticsearch files.
:::

* [Intalling Plugings](./installation.md) ?what product?
* [Custom URL or file system](./plugin-management-custom-url.md) ?what product?
* [Installing multiple plugins](./installing-multiple-plugins.md) ?what product?
* [Mandatory plugins](./mandatory-plugins.md) ?what product?
* [Listing, removing and updating installed plugins](./listing-removing-updating.md) ?what product?
* [Other command line parameters](./_other_command_line_parameters.md) ?what product?
* [Manage plugins using a configuration file](./manage-plugins-using-configuration-file.md) ?what product?

## Managing plugins for docker deployments
If you run Elasticsearch using Docker, you can manage plugins using a [configuration file](manage-plugins-using-configuration-file.md).





