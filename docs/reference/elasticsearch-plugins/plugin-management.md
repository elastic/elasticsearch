---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management.html
applies_to:
  deployment:
    ess: ga
    ece: ga
    self: ga
  serverless: unavailable
---

# Plugin management

Plugins extend {{es}}'s core functionality and can be managed differently depending on the deployment type. While {{ece}} (ECE) and {{ech}} 
(ECH) provide built-in plugin management, self-managed deployments require manual installation.

Plugins serve various purposes, including:

* National language support, phonetic analysis, and extended unicode support
* Ingesting attachments in common formats and ingesting information about the geographic location of IP addresses
* Adding new field datatypes to {{es}}
* Discovery plugins, such as the cloud AWS plugin that allows discovering nodes on EC2 instances
* Analysis plugins, to provide analyzers targeted at languages other than English
* Scripting plugins, to provide additional scripting languages

## Managing plugins for {{ech}}
```{applies_to}
    ess: ga
```

{{ech}} simplifies plugin management by offering compatible plugins for your {{es}} version. These plugins are automatically upgraded with your deployment, except when there are breaking changes.

To add plugins to a hosted deployment, refer to:

* [Add plugins and extensions in {{ech}}](docs-content://deploy-manage/deploy/elastic-cloud/add-plugins-extensions.md)
* [Upload custom plugins and bundles](docs-content://deploy-manage/deploy/elastic-cloud/upload-custom-plugins-bundles.md)
* [Manage plugins and extensions through the API](docs-content://deploy-manage/deploy/elastic-cloud/manage-plugins-extensions-through-api.md)

## Managing plugins for {{ece}}
```{applies_to}
    ece: ga
```

{{ece}} provides built-in plugins that work with your version of {{es}} and are upgraded along with your deployment, unless there are breaking changes.

To add plugins to an {{ece}} deployment, refer to:

* [Add plugins and extensions in {{ece}}](docs-content://deploy-manage/deploy/cloud-enterprise/add-plugins.md)
* [Add custom bundles and plugins](docs-content://deploy-manage/deploy/cloud-enterprise/add-custom-bundles-plugins.md)

## Managing plugins for self-managed deployments
```{applies_to}
    self: ga
```

Use the `elasticsearch-plugin` command line tool to install, list, and remove plugins. It is located in the `$ES_HOME/bin` directory by default but it may be in a different location depending on which {{es}} package you installed. For more information, see [Plugins directory](_plugins_directory.md)

Run the following command to get usage instructions:

```
sudo bin/elasticsearch-plugin -h
```

:::{important} Running as root
If {{es}} was installed using the deb or rpm package then run `/usr/share/elasticsearch/bin/elasticsearch-plugin` as `root` so it can write to the appropriate files on disk. Otherwise run `bin/elasticsearch-plugin` as the user that owns all of the {{es}} files.
:::

For detailed instructions on installing, managing, and configuring plugins, see the following:

* [Installing Plugins](./installation.md)
* [Custom URL or file system](./plugin-management-custom-url.md)
* [Installing multiple plugins](./installing-multiple-plugins.md)
* [Mandatory plugins](./mandatory-plugins.md)
* [Listing, removing and updating installed plugins](./listing-removing-updating.md)
* [Other command line parameters](./_other_command_line_parameters.md)
* [Manage plugins using a configuration file](./manage-plugins-using-configuration-file.md)

### Managing plugins for Docker deployments

If you run {{es}} using the [official {{es}} Docker image](https://www.docker.elastic.co/), you can manage plugins using a declarative [configuration file](manage-plugins-using-configuration-file.md). This approach applies to self-managed Docker deployments only.


:::{admonition} Running {{es}} with plugins on {{eck}}
:applies_to: eck: ga
On {{eck}}, install plugins by building a custom container image or using init containers. For details, see [Custom configuration files and plugins](docs-content://deploy-manage/deploy/cloud-on-k8s/custom-configuration-files-plugins.md).
:::