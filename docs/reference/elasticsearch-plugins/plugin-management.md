---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management.html
applies_to:
  stack: ga
  serverless: unavailable
---

# Plugin management

Plugins extend {{es}}'s core functionality and can serve various purposes, including:

* National language support, phonetic analysis, and extended unicode support
* Ingesting attachments in common formats and ingesting information about the geographic location of IP addresses
* Adding new field datatypes to {{es}}
* Discovery plugins, such as the cloud AWS plugin that allows discovering nodes on EC2 instances
* Analysis plugins, to provide analyzers targeted at languages other than English
* Scripting plugins, to provide additional scripting languages


How you add and manage plugins depends on where {{es}} runs:
* Hosted Cloud deployments such as [{{ech}}](#managing-plugins-for-ech) and [{{ece}}](#managing-plugins-for-ece) expose plugin and extension management in the Cloud console and API.
* [Self-managed deployments](#managing-plugins-for-self-managed) — use a [configuration file](manage-plugins-using-configuration-file.md) with the official Docker image, or the `elasticsearch-plugin` CLI for package and archive installs.
* On [{{eck}}](#managing-plugins-for-eck) deployments you install plugins by building a custom container image or using init containers.

::::{note} {{serverless-full}} projects
{{serverless-full}} projects do not support installing plugins or uploading custom plugins and bundles. {{serverless-short}} includes [core analysis plugins](./analysis-plugins.md#_core_analysis_plugins) by default. To manage synonyms, use the [synonyms API]({{es-serverless-apis}}group/endpoint-synonyms). For other differences between {{ech}} and {{serverless-short}} on plugins, bundles, and custom dictionaries, see [Compare {{ech}} and Serverless](docs-content://deploy-manage/deploy/elastic-cloud/differences-from-other-elasticsearch-offerings.md#elasticsearch-differences-custom-plugins-and-bundles).
::::

## Managing plugins for {{ech}} [managing-plugins-for-ech]
```{applies_to}
    ess: ga
```

{{ech}} simplifies plugin management by offering compatible plugins for your {{es}} version. These plugins are automatically upgraded with your deployment, except when there are breaking changes.

To add plugins to a hosted deployment, refer to:

* [Add plugins and extensions in {{ech}}](docs-content://deploy-manage/deploy/elastic-cloud/add-plugins-extensions.md)
% * [Add plugins provided with {{ech}}](docs-content://deploy-manage/deploy/elastic-cloud/add-plugins-provided-with-ech.md)
* [Upload custom plugins and bundles](docs-content://deploy-manage/deploy/elastic-cloud/upload-custom-plugins-bundles.md)
* [Manage plugins and extensions through the API](docs-content://deploy-manage/deploy/elastic-cloud/manage-plugins-extensions-through-api.md)

## Managing plugins for {{ece}} [managing-plugins-for-ece]
```{applies_to}
    ece: ga
```

{{ece}} provides built-in plugins that work with your version of {{es}} and are upgraded along with your deployment, unless there are breaking changes.

To add plugins to an {{ece}} deployment, refer to:

* [Add plugins and extensions in {{ece}}](docs-content://deploy-manage/deploy/cloud-enterprise/add-plugins.md)
* [Add custom bundles and plugins](docs-content://deploy-manage/deploy/cloud-enterprise/add-custom-bundles-plugins.md)


## Managing plugins for self-managed deployments [managing-plugins-for-self-managed]
```{applies_to}
    self: ga
```

How you manage plugins depends on how you install {{es}}:

* If you run {{es}} using the [official {{es}} Docker image](https://www.docker.elastic.co/), manage plugins with a declarative [configuration file](manage-plugins-using-configuration-file.md).
* For all [other installation methods](#manage-plugins-with-package-and-archive-installs), use the `elasticsearch-plugin` command-line tool to install, list, and remove plugins.

### Manage plugins with the Docker image

List the plugins you want in `elasticsearch-plugins.yml` in your config directory. Each time the container starts, {{es}} syncs installed plugins to match that list: installing missing plugins, removing ones you deleted from the file, and upgrading official plugins when you upgrade {{es}}.

To add or remove a plugin, edit the file and restart the container. Do not use `elasticsearch-plugin install` or `remove` as those commands are disabled when `elasticsearch-plugins.yml` is present.

Refer to the following pages:

* [Manage plugins using a configuration file](./manage-plugins-using-configuration-file.md)
* [Mandatory plugins](./mandatory-plugins.md)

### Manage plugins with package and archive installs [manage-plugins-with-package-and-archive-installs]

Use the `elasticsearch-plugin` command-line tool. It is located in the `$ES_HOME/bin` directory by default, but it might be in a different location depending on which {{es}} package you installed. For more information, see [Plugins directory](_plugins_directory.md).

Run the following command to get usage instructions:

```
sudo bin/elasticsearch-plugin -h
```

:::{important} - Running as root
If {{es}} was installed using the deb or rpm package, then run `/usr/share/elasticsearch/bin/elasticsearch-plugin` as `root` so it can write to the appropriate files on disk. Otherwise, run `bin/elasticsearch-plugin` as the user that owns all of the {{es}} files.
:::

Refer to the following pages:

* [Installing plugins](./installation.md)
* [Custom URL or file system](./plugin-management-custom-url.md)
* [Installing multiple plugins](./installing-multiple-plugins.md)
* [Mandatory plugins](./mandatory-plugins.md)
* [Listing, removing and updating installed plugins](./listing-removing-updating.md)
* [Other command line parameters](./_other_command_line_parameters.md)
## Managing plugins for {{eck}} [managing-plugins-for-eck]
```{applies_to}
    eck: ga
```

On {{eck}}, {{es}} runs in Kubernetes pods. Plugins must be on disk before the main {{es}} container starts. The two supported approaches are:

* [Using a custom container image](docs-content://deploy-manage/deploy/cloud-on-k8s/custom-configuration-files-plugins.md): You build a custom image from the official Elastic image with the required plugins pre-installed. This option is reproducible, works without internet access at runtime, and starts quickly, but requires a container registry and a new image for each {{es}} version upgrade.
* [Using init containers](docs-content://deploy-manage/deploy/cloud-on-k8s/init-containers-for-plugin-downloads.md): You use an init container to run `elasticsearch-plugin install` before the main {{es}} container starts. This option is easier to get started with, but requires pod internet access and repeats the download on each new node.

:::{note}
You can inject configuration files, such as synonym dictionaries, SAML metadata, or TLS certificates by [mounting them with ConfigMaps or Secrets](docs-content://deploy-manage/deploy/cloud-on-k8s/custom-configuration-files-plugins.md#use-a-volume-and-volume-mount-together-with-a-configmap-or-secret). However, mounting plugin files into a pod does not run `elasticsearch-plugin install`, so {{es}} will not load them at startup. Instead, to install plugins, use a custom container image or init container. 
:::
