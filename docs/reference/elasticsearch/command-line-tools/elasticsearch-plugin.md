---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/installation.html
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/installing-multiple-plugins.html
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/listing-removing-updating.html
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management-custom-url.html
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/_other_command_line_parameters.html
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/_plugins_directory.html
applies_to:
  deployment:
    self: ga
products:
  - id: elasticsearch
---

# elasticsearch-plugin [elasticsearch-plugin]

The `elasticsearch-plugin` command installs, lists, and removes [{{es}} plugins](/reference/elasticsearch-plugins/index.md).

## Synopsis [elasticsearch-plugin-synopsis]

```shell
bin/elasticsearch-plugin
( [list]
| [install [-b, --batch] (<plugin_id>)+]
| [remove [-p, --purge] (<plugin_id>)+]
) [-h, --help] ([-s, --silent] | [-v, --verbose])
```

## Description [elasticsearch-plugin-description]

A plugin is only available to the node it was installed on, so run this command on every node in the cluster, then restart each node to load the plugin.

The command is in the `$ES_HOME/bin` directory by default, but it might be elsewhere depending on which {{es}} package you installed. Plugins are installed into the node's `plugins` directory, whose location also depends on the package:

* [Directory layout of `.tar.gz` archives](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-from-archive-on-linux-macos.md#targz-layout)
* [Directory layout of Windows `.zip` archives](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-zip-on-windows.md#windows-layout)
* [Directory layout of Debian package](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-debian-package.md#deb-layout)
* [Directory layout of RPM](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-rpm.md#rpm-layout)

Every plugin records the {{es}} version it was built against, and {{es}} checks that version both when you install the plugin and when the node starts. A node refuses to start if the check fails. Most plugins must match the running version exactly, so you have to install a new build for every upgrade, including patch releases. Text analysis plugins built against the [stable plugin API](/extend/creating-stable-plugins.md) only need to match the major version, so one artifact stays valid across upgrades within that major version, provided you do not downgrade a node below the version the plugin was built against.

To prevent a node from starting when a plugin you depend on is missing, use the [`plugin.mandatory`](/reference/elasticsearch/configuration-reference/node-settings.md#mandatory-plugins) setting.

::::{important} Running as root
If {{es}} was installed using the deb or rpm package, then run `/usr/share/elasticsearch/bin/elasticsearch-plugin` as `root` so it can write to the appropriate files on disk. Otherwise, run `bin/elasticsearch-plugin` as the user that owns all of the {{es}} files.
::::

::::{note} Docker installations
If you run {{es}} using the [official {{es}} Docker images](https://www.docker.elastic.co/), manage plugins with the declarative [`elasticsearch-plugins.yml` configuration file](/reference/elasticsearch-plugins/manage-plugins-using-configuration-file.md) instead. The `install` and `remove` commands are disabled when that file is present.
::::

## Parameters [elasticsearch-plugin-parameters]

`-b, --batch`
:   Used with the `install` command. Skips the confirmation prompt shown for plugins that request privileges beyond those granted by default, automatically granting all requested permissions. The command detects when it is not being run from a console and enables batch mode for you; specify this parameter when that detection fails, such as when installing from an automation script.

`-h, --help`
:   Returns all of the command parameters.

`install (<plugin_id>)+`
:   Installs one or more plugins. Each `<plugin_id>` can be the name of a core plugin or a custom location, as described in [Plugin IDs](#elasticsearch-plugin-ids). Installing several plugins in one invocation is treated as a single transaction, so either all of them are installed, or none are if any one installation fails.

`list`
:   Lists the plugins installed on this node. To find out which plugins are installed on every node in the cluster, use the [node info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-info) instead.

`-p, --purge`
:   Used with the `remove` command. Deletes the plugin's configuration files along with the plugin. Configuration files are preserved by default so that they survive a plugin upgrade. You can also run `remove --purge` for a plugin you have already removed, to clear configuration files it left behind.

`remove (<plugin_id>)+`
:   Removes one or more plugins. After removing a Java plugin, restart the node to complete the removal. You can also remove a plugin manually by deleting its directory under `plugins/`.

`-s, --silent`
:   Turns off all output, including the progress bar.

`-v, --verbose`
:   Outputs more debug information.

## Plugin IDs [elasticsearch-plugin-ids]

The `install` command accepts the following forms of `<plugin_id>`.

Core plugin name
:   Install a core plugin by name. The command installs the version matching your {{es}} version and shows a progress bar while downloading.

    ```shell
    sudo bin/elasticsearch-plugin install analysis-icu
    ```

URL
:   Install a plugin from any valid URL. The plugin name is taken from its descriptor rather than the URL.

    ```shell
    sudo bin/elasticsearch-plugin install [url]
    ```

Unix file system
:   To install a plugin from your local file system at `/path/to/plugin.zip`:

    ```shell
    sudo bin/elasticsearch-plugin install file:///path/to/plugin.zip
    ```

Windows file system
:   To install a plugin from your local file system at `C:\path\to\plugin.zip`:

    ```shell
    bin\elasticsearch-plugin install file:///C:/path/to/plugin.zip
    ```

    ::::{note}
    Any path that contains spaces must be wrapped in quotes!
    ::::

    ::::{note}
    If you are installing a plugin from the filesystem the plugin distribution must not be contained in the `plugins` directory for the node that you are installing the plugin to or installation will fail.
    ::::

HTTP
:   To install a plugin from an HTTP URL:

    ```shell
    sudo bin/elasticsearch-plugin install <EXAMPLE_PLUGIN_HOST_URL>/plugin.zip
    ```

    The plugin script will refuse to talk to an HTTPS URL with an untrusted certificate. To use a self-signed HTTPS cert, you will need to add the CA cert to a local Java truststore and pass the location to the script as follows:

    ```shell
    sudo CLI_JAVA_OPTS="-Djavax.net.ssl.trustStore=/path/to/trustStore.jks" bin/elasticsearch-plugin install <MY_HOST_URL>/plugin.zip
    ```

## Custom config directory [elasticsearch-plugin-custom-config-directory]

If your `elasticsearch.yml` config file is in a custom location, you will need to specify the path to the config file when using the `plugin` script. You can do this as follows:

```sh
sudo ES_PATH_CONF=/path/to/conf/dir bin/elasticsearch-plugin install <plugin name>
```

## Proxy settings [elasticsearch-plugin-proxy-settings]

To install a plugin via a proxy, you can add the proxy details to the `CLI_JAVA_OPTS` environment variable with the Java settings `http.proxyHost` and `http.proxyPort` (or `https.proxyHost` and `https.proxyPort`):

```shell
sudo CLI_JAVA_OPTS="-Dhttp.proxyHost=host_name -Dhttp.proxyPort=port_number -Dhttps.proxyHost=host_name -Dhttps.proxyPort=https_port_number" bin/elasticsearch-plugin install analysis-icu
```

Or on Windows:

```shell
set CLI_JAVA_OPTS="-Dhttp.proxyHost=host_name -Dhttp.proxyPort=port_number -Dhttps.proxyHost=host_name -Dhttps.proxyPort=https_port_number"
bin\elasticsearch-plugin install analysis-icu
```

## Exit codes [elasticsearch-plugin-exit-codes]

`0`
:   everything was OK

`64`
:   unknown command or incorrect option parameter

`70`
:   any other error

`74`
:   IO error

## Examples [elasticsearch-plugin-examples]

### Install a core plugin [elasticsearch-plugin-example-install]

```shell
sudo bin/elasticsearch-plugin install analysis-icu
```

### Install multiple plugins at once [elasticsearch-plugin-example-install-multiple]

```shell
sudo bin/elasticsearch-plugin install analysis-icu analysis-kuromoji
```

### Install a plugin without the privileges prompt [elasticsearch-plugin-example-batch]

```shell
sudo bin/elasticsearch-plugin install --batch [pluginname]
```

### List installed plugins [elasticsearch-plugin-example-list]

```shell
sudo bin/elasticsearch-plugin list
```

### Remove a plugin and its configuration files [elasticsearch-plugin-example-remove]

```shell
sudo bin/elasticsearch-plugin remove --purge [pluginname]
```

### Remove multiple plugins at once [elasticsearch-plugin-example-remove-multiple]

```shell
sudo bin/elasticsearch-plugin remove [pluginname] [pluginname] ... [pluginname]
```

### Update a plugin [elasticsearch-plugin-example-update]

Plugins cannot be updated in place. Remove the plugin and install it again:

```shell
sudo bin/elasticsearch-plugin remove [pluginname]
sudo bin/elasticsearch-plugin install [pluginname]
```
