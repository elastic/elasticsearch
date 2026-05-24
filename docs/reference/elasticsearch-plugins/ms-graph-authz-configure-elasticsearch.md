---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/ms-graph-authz-configure-elastic.html
applies_to:
  stack: ga 9.1
---

# Configuration properties [configuration-properties]

After the plugin is installed, the following configuration settings are
available:

`xpack.security.authc.realms.microsoft_graph.*.order`
:   The priority of the realm within the realm chain. Realms with a lower order
are consulted first. The value must be unique for each realm. This setting is
required.

`xpack.security.authc.realms.microsoft_graph.*.tenant_id`
:   Your Microsoft Entra
ID [Tenant ID](https://learn.microsoft.com/en-us/entra/fundamentals/how-to-find-tenant).
This setting is required.

`xpack.security.authc.realms.microsoft_graph.*.client_id`
:   The Application ID of the Enterprise Application you registered in the
previous section. This setting is required.

`xpack.security.authc.realms.microsoft_graph.*.client_secret`
:   The client secret value for the Application you registered in the previous
section. This is a sensitive setting, and must be configured in the
Elasticsearch keystore. This setting is required.

`xpack.security.authc.realms.microsoft_graph.*.access_token_host`
:   A Microsoft login URL. Defaults to `https://login.microsoftonline.com`.

`xpack.security.authc.realms.microsoft_graph.*.graph_host`
:   The Microsoft Graph base address. Defaults to
`https://graph.microsoft.com/v1.0`.

`xpack.security.authc.realms.microsoft_graph.*.http_request_timeout`
:   The timeout for individual Graph HTTP requests. Defaults to `10s`.

`xpack.security.authc.realms.microsoft_graph.*.execution_timeout`
:   The overall timeout for authorization requests to this plugin. Defaults to
`30s`.

Create a Microsoft Graph realm, following the above settings, then configure an
existing realm to delegate to it using `authorization_realms`.

For example, the following configuration authenticates via Microsoft Entra with
SAML, and uses the Microsoft Graph plugin to look up group membership:

```yaml
xpack.security.authc.realms.saml.kibana-realm:
  order: 2
  attributes.principal: nameid
  attributes.groups: "http://schemas.microsoft.com/ws/2008/06/identity/claims/groups"
  idp.metadata.path: "https://login.microsoftonline.com/<Tenant_ID>/federationmetadata/2007-06/federationmetadata.xml?appid=<Application_ID>"
  idp.entity_id: "https://sts.windows.net/<Tenant_ID>/"
  sp.entity_id: "<Kibana_Endpoint_URL>"
  sp.acs: "<Kibana_Endpoint_URL>/api/security/saml/callback"
  sp.logout: "<Kibana_Endpoint_URL>/logout"
  authorization_realms: microsoft_graph1

xpack.security.authc.realms.microsoft_graph.microsoft_graph1:
  order: 3
  tenant_id: "<Tenant_ID>"
  client_id: "<Graph_Application_ID>"
```
