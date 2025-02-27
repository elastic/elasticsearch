---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-passwords.html
---

# elasticsearch-setup-passwords [setup-passwords]

::::{admonition} Deprecated in 8.0.
:class: warning

The `elasticsearch-setup-passwords` tool is deprecated and will be removed in a future release. To manually reset the password for the built-in users (including the `elastic` user), use the [`elasticsearch-reset-password`](/reference/elasticsearch/command-line-tools/reset-password.md) tool, the {{es}} change password API, or the User Management features in {{kib}}.
::::


The `elasticsearch-setup-passwords` command sets the passwords for the [built-in users](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/built-in-users.md).


## Synopsis [_synopsis_10]

```shell
bin/elasticsearch-setup-passwords auto|interactive
[-b, --batch] [-h, --help] [-E <KeyValuePair>]
[-s, --silent] [-u, --url "<URL>"] [-v, --verbose]
```


## Description [_description_17]

This command is intended for use only during the initial configuration of the {{es}} {{security-features}}. It uses the [`elastic` bootstrap password](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/built-in-users.md#bootstrap-elastic-passwords) to run user management API requests. If your {{es}} keystore is password protected, before you can set the passwords for the built-in users, you must enter the keystore password. After you set a password for the `elastic` user, the bootstrap password is no longer active and you cannot use this command. Instead, you can change passwords by using the **Management > Users** UI in {{kib}} or the [Change Password API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-security-change-password).

This command uses an HTTP connection to connect to the cluster and run the user management requests. If your cluster uses TLS/SSL on the HTTP layer, the command automatically attempts to establish the connection by using the HTTPS protocol. It configures the connection by using the `xpack.security.http.ssl` settings in the `elasticsearch.yml` file. If you do not use the default config directory location, ensure that the **ES_PATH_CONF** environment variable returns the correct path before you run the `elasticsearch-setup-passwords` command. You can override settings in your `elasticsearch.yml` file by using the `-E` command option. For more information about debugging connection failures, see [Setup-passwords command fails due to connection failure](docs-content://troubleshoot/elasticsearch/security/trb-security-setup.md).


## Parameters [setup-passwords-parameters]

`auto`
:   Outputs randomly-generated passwords to the console.

`-b, --batch`
:   If enabled, runs the change password process without prompting the user.

`-E <KeyValuePair>`
:   Configures a standard {{es}} or {{xpack}} setting.

`-h, --help`
:   Shows help information.

`interactive`
:   Prompts you to manually enter passwords.

`-s, --silent`
:   Shows minimal output.

`-u, --url "<URL>"`
:   Specifies the URL that the tool uses to submit the user management API requests. The default value is determined from the settings in your `elasticsearch.yml` file. If `xpack.security.http.ssl.enabled`  is set to `true`, you must specify an HTTPS URL.

`-v, --verbose`
:   Shows verbose output.


## Examples [_examples_22]

The following example uses the `-u` parameter to tell the tool where to submit its user management API requests:

```shell
bin/elasticsearch-setup-passwords auto -u "http://localhost:9201"
```

