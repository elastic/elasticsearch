---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-management-custom-url.html
applies_to:
  deployment:
    self: ga
---

# Custom URL or file system [plugin-management-custom-url]

A plugin can also be downloaded directly from a custom location by specifying the URL:

```shell
sudo bin/elasticsearch-plugin install [url] <1>
```

1. must be a valid URL, the plugin name is determined from its descriptor.


Unix
:   To install a plugin from your local file system at `/path/to/plugin.zip`, you could run:

    ```shell
    sudo bin/elasticsearch-plugin install file:///path/to/plugin.zip
    ```


Windows
:   To install a plugin from your local file system at `C:\path\to\plugin.zip`, you could run:

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
    sudo bin/elasticsearch-plugin install https://some.domain/path/to/plugin.zip
    ```

    The plugin script will refuse to talk to an HTTPS URL with an untrusted certificate. To use a self-signed HTTPS cert, you will need to add the CA cert to a local Java truststore and pass the location to the script as follows:

    ```shell
    sudo CLI_JAVA_OPTS="-Djavax.net.ssl.trustStore=/path/to/trustStore.jks" bin/elasticsearch-plugin install https://host/plugin.zip
    ```


