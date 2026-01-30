---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-security.html
applies_to:
  stack: ga
  serverless: ga
products:
  - id: elasticsearch
---

# Security [sql-security]

{{es}} SQL integrates with security, if this is enabled on your cluster. In such a scenario, {{es}} SQL supports both security at the transport layer (by encrypting the communication between the consumer and the server) and authentication (for the access layer).


## SSL/TLS configuration [ssl-tls-config]

In case of an encrypted transport, the SSL/TLS support needs to be enabled in Elasticsearch SQL to properly establish communication with {{es}}. This is done by setting the `ssl` property to `true` or by using the `https` prefix in the URL.<br> Depending on your SSL configuration (whether the certificates are signed by a CA or not, whether they are global at JVM level or just local to one application), might require setting up the `keystore` and/or `truststore`, that is where the *credentials* are stored (`keystore` - which typically stores private keys and certificates) and how to *verify* them (`truststore` - which typically stores certificates from third party also known as CA - certificate authorities).<br> Typically (and again, do note that your environment might differ significantly), if the SSL setup for {{es}} SQL is not already done at the JVM level, one needs to setup the keystore if the Elasticsearch SQL security requires client authentication (PKI - Public Key Infrastructure), and setup `truststore` if SSL is enabled.


## Authentication [_authentication]

The authentication support in {{es}} SQL is of two types:

Username/Password
:   Set these through `user` and `password` properties.

PKI/X.509
:   Use X.509 certificates to authenticate {{es}} SQL to {{es}}. For this, one would need to setup the `keystore` containing the private key and certificate to the appropriate user (configured in {{es}}) and the `truststore` with the CA certificate used to sign the SSL/TLS certificates in the {{es}} cluster. That is, one should setup the key to authenticate {{es}} SQL and also to verify that is the right one. To do so, one should set the `ssl.keystore.location` and `ssl.truststore.location` properties to indicate the `keystore` and `truststore` to use. It is recommended to have these secured through a password in which case `ssl.keystore.pass` and `ssl.truststore.pass` properties are required.


## Permissions (server-side) [sql-security-permissions]

On the server, one needs to add a few permissions to users so they can run SQL. To run SQL, a user needs `read` and `indices:admin/get` permissions at minimum while some parts of the API require `cluster:monitor/main`.

You can add permissions by [creating a role](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/defining-roles.md), and assigning that role to the user. Roles can be created using {{kib}}, an [API call](#sql-role-api-example) or the [`roles.yml` configuration file](#sql-role-file-example). Using {{kib}} or the role management APIs is the preferred method for defining roles. File-based role management is useful if you want to define a role that doesn’t need to change. You cannot use the role management APIs to view or edit a role defined in `roles.yml`.


### Add permissions with the role management APIs [sql-role-api-example]

This example configures a role that can run SQL in JDBC querying the `test` index:

```console
POST /_security/role/cli_or_drivers_minimal
{
  "cluster": ["cluster:monitor/main"],
  "indices": [
    {
      "names": ["test"],
      "privileges": ["read", "indices:admin/get"]
    }
  ]
}
```


### Add permissions to `roles.yml` [sql-role-file-example]

This example configures a role that can run SQL in JDBC querying the `test` and `bort` indices. Add the following to `roles.yml`:

```yaml
cli_or_drivers_minimal:
  cluster:
    - "cluster:monitor/main"
  indices:
    - names: test
      privileges: [read, "indices:admin/get"]
    - names: bort
      privileges: [read, "indices:admin/get"]
```

