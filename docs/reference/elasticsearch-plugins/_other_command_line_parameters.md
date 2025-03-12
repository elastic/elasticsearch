---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/_other_command_line_parameters.html
applies_to:
  deployment:
    self: ga
---

# Other command line parameters [_other_command_line_parameters]

The `plugin` scripts supports a number of other command line parameters:


## Silent/verbose mode [_silentverbose_mode]

The `--verbose` parameter outputs more debug information, while the `--silent` parameter turns off all output including the progress bar. The script may return the following exit codes:

`0`
:   everything was OK

`64`
:   unknown command or incorrect option parameter

`74`
:   IO error

`70`
:   any other error


## Batch mode [_batch_mode]

Certain plugins require more privileges than those provided by default in core Elasticsearch. These plugins will list the required privileges and ask the user for confirmation before continuing with installation.

When running the plugin install script from another program (e.g. install automation scripts), the plugin script should detect that it is not being called from the console and skip the confirmation response, automatically granting all requested permissions. If console detection fails, then batch mode can be forced by specifying `-b` or `--batch` as follows:

```shell
sudo bin/elasticsearch-plugin install --batch [pluginname]
```


## Custom config directory [_custom_config_directory]

If your `elasticsearch.yml` config file is in a custom location, you will need to specify the path to the config file when using the `plugin` script. You can do this as follows:

```sh
sudo ES_PATH_CONF=/path/to/conf/dir bin/elasticsearch-plugin install <plugin name>
```


## Proxy settings [_proxy_settings]

To install a plugin via a proxy, you can add the proxy details to the `CLI_JAVA_OPTS` environment variable with the Java settings `http.proxyHost` and `http.proxyPort` (or `https.proxyHost` and `https.proxyPort`):

```shell
sudo CLI_JAVA_OPTS="-Dhttp.proxyHost=host_name -Dhttp.proxyPort=port_number -Dhttps.proxyHost=host_name -Dhttps.proxyPort=https_port_number" bin/elasticsearch-plugin install analysis-icu
```

Or on Windows:

```shell
set CLI_JAVA_OPTS="-Dhttp.proxyHost=host_name -Dhttp.proxyPort=port_number -Dhttps.proxyHost=host_name -Dhttps.proxyPort=https_port_number"
bin\elasticsearch-plugin install analysis-icu
```

