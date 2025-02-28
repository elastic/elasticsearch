---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/reset-password.html
---

# elasticsearch-reset-password [reset-password]

The `elasticsearch-reset-password` command resets the passwords of users in the native realm and built-in users.


## Synopsis [_synopsis_7]

```shell
bin/elasticsearch-reset-password
[-a, --auto] [-b, --batch] [-E <KeyValuePair]
[-f, --force] [-h, --help] [-i, --interactive]
[-s, --silent] [-u, --username] [--url] [-v, --verbose]
```


## Description [_description_14]

Use this command to reset the password of any user in the native realm or any built-in user. By default, a strong password is generated for you. To explicitly set a password, run the tool in interactive mode with `-i`. The command generates (and subsequently removes) a temporary user in the [file realm](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/file-based.md) to run the request that changes the user password.

::::{important}
You cannot use this tool if the file realm is disabled in your `elasticsearch.yml` file.
::::


This command uses an HTTP connection to connect to the cluster and run the user management requests. The command automatically attempts to establish the connection over HTTPS by using the `xpack.security.http.ssl` settings in the `elasticsearch.yml` file. If you do not use the default configuration directory location, ensure that the `ES_PATH_CONF` environment variable returns the correct path before you run the `elasticsearch-reset-password` command. You can override settings in your `elasticsearch.yml` file by using the `-E` command option. For more information about debugging connection failures, see [Setup-passwords command fails due to connection failure](docs-content://troubleshoot/elasticsearch/security/trb-security-setup.md).


## Parameters [reset-password-parameters]

`-a, --auto`
:   Resets the password of the specified user to an auto-generated strong password. (Default)

`-b, --batch`
:   Runs the reset password process without prompting the user for verification.

`-E <KeyValuePair>`
:   Configures a standard {{es}} or {{xpack}} setting.

`-f, --force`
:   Forces the command to run against an unhealthy cluster.

`-h, --help`
:   Returns all of the command parameters.

`-i, --interactive`
:   Prompts for the password of the specified user. Use this option to explicitly set a password.

`-s --silent`
:   Shows minimal output in the console.

`-u, --username`
:   The username of the native realm user or built-in user.

`--url`
:   Specifies the base URL (hostname and port of the local node) that the tool uses to submit API requests to {{es}}. The default value is determined from the settings in your `elasticsearch.yml` file. If `xpack.security.http.ssl.enabled` is set to `true`, you must specify an HTTPS URL.

`-v --verbose`
:   Shows verbose output in the console.


## Examples [_examples_19]

The following example resets the password of the `elastic` user to an auto-generated value and prints the new password in the console:

```shell
bin/elasticsearch-reset-password -u elastic
```

The following example resets the password of a native user with username `user1` after prompting in the terminal for the desired password:

```shell
bin/elasticsearch-reset-password --username user1 -i
```

The following example resets the password of a native user with username `user2` to an auto-generated value prints the new password in the console. The specified URL indicates where the elasticsearch-reset-password tool attempts to reach the local {{es}} node:

```shell
bin/elasticsearch-reset-password --url "https://172.0.0.3:9200" --username user2 -i
```

