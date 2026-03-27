---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/commands.html
applies_to:
  deployment:
    self: ga
products:
  - id: elasticsearch 
---

# Command-line tools [commands]

{{es}} includes a set of command-line tools in its `bin` subdirectory (for example, `/usr/share/elasticsearch/bin`). These tools support tasks such as generating TLS/SSL certificates, configuring security settings and user credentials, enrolling or reconfiguring nodes, managing corrupted shards, and other node-level administrative operations that are only possible, or simpler to perform, outside the REST API.

:::{important}
The tools are intended for use in self-managed environments and are included with all {{es}} software packages and Docker images, but are not accessible in fully managed {{ecloud}} deployments such as {{ech}} or {{serverless-full}}. Similarly, they are not designed for use in {{ece}} or {{eck}}.
:::

The following tools are available:

* [*elasticsearch-certgen*](/reference/elasticsearch/command-line-tools/certgen.md)
* [*elasticsearch-certutil*](/reference/elasticsearch/command-line-tools/certutil.md)
* [*elasticsearch-create-enrollment-token*](/reference/elasticsearch/command-line-tools/create-enrollment-token.md)
* [*elasticsearch-croneval*](/reference/elasticsearch/command-line-tools/elasticsearch-croneval.md)
* [*elasticsearch-keystore*](/reference/elasticsearch/command-line-tools/elasticsearch-keystore.md)
* [*elasticsearch-node*](/reference/elasticsearch/command-line-tools/node-tool.md)
* [*elasticsearch-reconfigure-node*](/reference/elasticsearch/command-line-tools/reconfigure-node.md)
* [*elasticsearch-reset-password*](/reference/elasticsearch/command-line-tools/reset-password.md)
* [*elasticsearch-saml-metadata*](/reference/elasticsearch/command-line-tools/saml-metadata.md)
* [*elasticsearch-setup-passwords*](/reference/elasticsearch/command-line-tools/setup-passwords.md)
* [*elasticsearch-shard*](/reference/elasticsearch/command-line-tools/shard-tool.md)
* [*elasticsearch-syskeygen*](/reference/elasticsearch/command-line-tools/syskeygen.md)
* [*elasticsearch-users*](/reference/elasticsearch/command-line-tools/users-command.md)

