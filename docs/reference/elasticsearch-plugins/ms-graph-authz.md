---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/ms-graph-authz.html
applies_to:
  stack: ga 9.1
sub:
  plugin-id: microsoft-graph-authz
---

# Microsoft Graph Authz [ms-graph-authz]

The Microsoft Graph Authz plugin uses [Microsoft Graph](https://learn.microsoft.com/en-us/graph/api/user-list-memberof)
to look up group membership information from Microsoft Entra ID.

This is primarily intended to work around the Microsoft Entra ID maximum group
size limit (see [Group overages](https://learn.microsoft.com/en-us/security/zero-trust/develop/configure-tokens-group-claims-app-roles#group-overages)).

## Installation [ms-graph-authz-install]

:::{include} _snippets/plugin-installation.md
:::

:::{include} _snippets/plugin-deployment-types.md
:::

## Removal [ms-graph-authz-remove]

:::{include} _snippets/plugin-removal.md
:::

## Configuration

To learn how to configure the Microsoft Graph Authz plugin, refer to [configuration properties](/reference/elasticsearch-plugins/ms-graph-authz-configure-elasticsearch.md).
