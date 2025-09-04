---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/reconfigure-node.html
---

# elasticsearch-reconfigure-node [reconfigure-node]

The `elasticsearch-reconfigure-node` tool reconfigures an {{es}} node that was installed through an RPM or DEB package to join an existing cluster with security features enabled.


## Synopsis [_synopsis_6]

```shell
bin/elasticsearch-reconfigure-node
[--enrollment-token] [-h, --help] [-E <KeyValuePair>]
[-s, --silent] [-v, --verbose]
```


## Description [_description_13]

When installing {{es}} with a DEB or RPM package, the current node is assumed to be the first node in the cluster. {{es}} enables and configures security features on the node, generates a password for the `elastic` superuser, and configures TLS for the HTTP and transport layers.

Rather than form a single-node cluster, you can add a node to an existing cluster where security features are already enabled and configured. Before starting your new node, run the [`elasticsearch-create-enrollment-token`](/reference/elasticsearch/command-line-tools/create-enrollment-token.md) tool with the `-s node` option to generate an enrollment token on any node in your existing cluster. On your new node, run the `elasticsearch-reconfigure-node` tool and pass the enrollment token as a parameter.

::::{note}
This tool is intended only for use on DEB or RPM distributions of {{es}}.
::::


You must run this tool with `sudo` so that it can edit the necessary files in your {{es}} installation configuration directory that are owned by `root:elasticsearch`.


## Parameters [reconfigure-node-parameters]

`--enrollment-token`
:   The enrollment token, which can be generated on any of the nodes in an existing, secured cluster.

`-E <KeyValuePair>`
:   Configures a standard {{es}} or {{xpack}} setting.

`-h, --help`
:   Shows help information.

`-s, --silent`
:   Shows minimal output.

`-v, --verbose`
:   Shows verbose output.

$$$cli-tool-jvm-options-reconfigure-node$$$


### JVM options [_jvm_options_2]

CLI tools run with 64MB of heap. For most tools, this value is fine. However, if needed this can be overridden by setting the `CLI_JAVA_OPTS` environment variable. For example, the following increases the heap size used by the `elasticsearch-reconfigure-node` tool to 1GB.

```shell
export CLI_JAVA_OPTS="-Xmx1g"
bin/elasticsearch-reconfigure-node ...
```


## Examples [_examples_18]

The following example reconfigures an installed {{es}} node so that it can join an existing cluster when it starts for the first time.

```shell
sudo /usr/share/elasticsearch/elasticsearch-reconfigure-node --enrollment-token eyJ2ZXIiOiI4LjAuMCIsImFkciI6WyIxOTIuMTY4LjEuMTY6OTIwMCJdLCJmZ3IiOiI4NGVhYzkyMzAyMWQ1MjcyMmQxNTFhMTQwZmM2ODI5NmE5OWNiNmU0OGVhZjYwYWMxYzljM2I3ZDJjOTg2YTk3Iiwia2V5IjoiUy0yUjFINEJrNlFTMkNEY1dVV1g6QS0wSmJxM3hTRy1haWxoQTdPWVduZyJ9
```

