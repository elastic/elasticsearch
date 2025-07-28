---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management.html
  - https://www.elastic.co/guide/en/cloud/current/ec-adding-plugins.html
  - https://www.elastic.co/guide/en/cloud/current/ec-adding-elastic-plugins.html
  - https://www.elastic.co/guide/en/cloud-enterprise/current/ece-add-plugins.html
applies_to:
  deployment:
    ess: ga
    ece: ga
    self: ga
  serverless: unavailable
---

# Plugin management

Plugins extend Elasticsearch’s core functionality and can be managed differently depending on the deployment type. While {{ece}} (ECE) and {{ech}} (ECH) provide built-in plugin management, self-managed deployments require manual installation.

{{ece}} and {{ech}} deployments simplify plugin management by offering compatible plugins for your Elasticsearch version. These plugins are automatically upgraded with your deployment, except in cases of breaking changes.

In ECE and ECH deployments, you can add plugins by selecting them from the available list. However, plugin availability depends on the Elasticsearch version.

Plugins serve various purposes, including:

* National language support, phonetic analysis, and extended unicode support
* Ingesting attachments in common formats and ingesting information about the geographic location of IP addresses
* Adding new field datatypes to Elasticsearch
* Discovery plugins, such as the cloud AWS plugin that allows discovering nodes on EC2 instances.
* Analysis plugins, to provide analyzers targeted at languages other than English.
* Scripting plugins, to provide additional scripting languages.

## Managing plugins for ECE
```{applies_to}
    ece: ga
```

### Add plugins when creating a new ECE deployment

1. [Log into the Cloud UI](docs-content://deploy-manage/deploy/cloud-enterprise/log-into-cloud-ui.md) and select **Create deployment**.
2. Make your initial deployment selections, then select **Customize Deployment**.
3. Beneath the Elasticsearch master node, expand the **Manage plugins and settings** caret.
4. Select the plugins you want.
5. Select **Create deployment**.

The deployment spins up with the plugins installed.

### Add plugins to an existing ECE deployment

1. [Log into the Cloud UI](docs-content://deploy-manage/deploy/cloud-enterprise/log-into-cloud-ui.md).
2. On the **Deployments** page, select your deployment.

    Narrow the list by name, ID, or choose from several other filters. To further define the list, use a combination of filters.

3. From your deployment menu, go to the **Edit** page.
4. Beneath the Elasticsearch master node, expand the **Manage plugins and settings** caret.
5. Select the plugins that you want.
6. Select **Save changes**.

There is no downtime when adding plugins to highly available deployments. The deployment is updated with new nodes that have the plugins installed.

## Managing plugins for ECH
```{applies_to}
    ess: ga
```

There are two ways to add plugins to {{ech}} deployments:

* Enable one of the official plugins already available in ECH
* [Upload a custom plugin and then enable it per deployment](./cloud/ec-custom-bundles.md).

Custom plugins can include the official {{es}} plugins not provided with ECH, any of the community-sourced plugins, or [plugins that you write yourself](/extend/index.md). Uploading custom plugins is available only to Gold, Platinum, and Enterprise subscriptions. For more information, check [Upload custom plugins and bundles](./cloud/ec-custom-bundles.md).

To learn more about the official and community-sourced plugins, refer to [{{es}} Plugins and Integrations](index.md).

For a detailed guide with examples of using the Elasticsearch Service API to create, get information about, update, and delete extensions and plugins, check [Managing plugins and extensions through the API](./cloud/ec-plugins-guide.md).

### Add plugins provided with ECH [ec-adding-elastic-plugins]

You can use a variety of official plugins that are compatible with your version of {{es}}. When you upgrade to a new {{es}} version, these plugins are simply upgraded with the rest of your deployment.

#### Before you begin [ec_before_you_begin_6]

Some restrictions apply when adding plugins. To learn more, check [Restrictions for {{es}} and {{kib}} plugins](cloud://release-notes/cloud-hosted/known-issues.md#ec-restrictions-plugins).

Only Gold, Platinum, Enterprise and Private subscriptions have access to uploading custom plugins. All subscription levels, including Standard, can upload scripts and dictionaries.

### Enabling plugins for a deployment

1. Log in to the [{{ecloud}} Console](https://cloud.elastic.co?page=docs&placement=docs-body).
2. Find your deployment On the home page and select **Manage** next to it, or go to the **Deployments** page to view all deployments.

    On the **Deployments** page you can narrow your deployments by name, ID, or choose from several other filters. To customize your view, use a combination of filters, or change the format from a grid to a list.

3. From the **Actions** dropdown, select **Edit deployment**.
4. Select **Manage user settings and extensions**.
5. Select the **Extensions** tab.
6. Select the plugins that you want to enable.
7. Select **Back**.
8. Select **Save**. The {{es}} cluster is then updated with new nodes that have the plugin installed.

## Managing plugins for self-managed deployments
```{applies_to}
    self: ga
```
Use the `elasticsearch-plugin` command line tool to install, list, and remove plugins. It is located in the `$ES_HOME/bin` directory by default but it may be in a different location depending on which Elasticsearch package you installed. For more information, see [Plugins directory](_plugins_directory.md)

Run the following command to get usage instructions:

``` 
sudo bin/elasticsearch-plugin -h
```

:::{important} Running as root
If Elasticsearch was installed using the deb or rpm package then run `/usr/share/elasticsearch/bin/elasticsearch-plugin` as `root` so it can write to the appropriate files on disk. Otherwise run `bin/elasticsearch-plugin` as the user that owns all of the Elasticsearch files.
:::

For detailed instructions on installing, managing, and configuring plugins, see the following:

* [Intalling Plugings](./installation.md)
* [Custom URL or file system](./plugin-management-custom-url.md)
* [Installing multiple plugins](./installing-multiple-plugins.md)
* [Mandatory plugins](./mandatory-plugins.md)
* [Listing, removing and updating installed plugins](./listing-removing-updating.md)
* [Other command line parameters](./_other_command_line_parameters.md)
* [Manage plugins using a configuration file](./manage-plugins-using-configuration-file.md)

## Managing plugins for docker deployments
```{applies_to}
    self: ga
```
If you run Elasticsearch using Docker, you can manage plugins using a [configuration file](manage-plugins-using-configuration-file.md).





