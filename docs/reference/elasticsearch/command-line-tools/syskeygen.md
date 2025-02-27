---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/syskeygen.html
---

# elasticsearch-syskeygen [syskeygen]

The `elasticsearch-syskeygen` command creates a system key file in the elasticsearch config directory.


## Synopsis [_synopsis_12]

```shell
bin/elasticsearch-syskeygen
[-E <KeyValuePair>] [-h, --help]
([-s, --silent] | [-v, --verbose])
```


## Description [_description_19]

The command generates a `system_key` file, which you can use to symmetrically encrypt sensitive data. For example, you can use this key to prevent {{watcher}} from returning and storing information that contains clear text credentials. See [*Encrypting sensitive data in {{watcher}}*](docs-content://explore-analyze/alerts-cases/watcher/encrypting-data.md).

::::{important}
The system key is a symmetric key, so the same key must be used on every node in the cluster.
::::



## Parameters [syskeygen-parameters]

`-E <KeyValuePair>`
:   Configures a setting. For example, if you have a custom installation of {{es}}, you can use this parameter to specify the `ES_PATH_CONF` environment variable.

`-h, --help`
:   Returns all of the command parameters.

`-s, --silent`
:   Shows minimal output.

`-v, --verbose`
:   Shows verbose output.


## Examples [_examples_23]

The following command generates a `system_key` file in the default `$ES_HOME/config` directory:

```sh
bin/elasticsearch-syskeygen
```

