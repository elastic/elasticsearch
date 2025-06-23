---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/users-command.html
---

# elasticsearch-users [users-command]

If you use file-based user authentication, the `elasticsearch-users` command enables you to add and remove users, assign user roles, and manage passwords per node.


## Synopsis [_synopsis_13]

```shell
bin/elasticsearch-users
([useradd <username>] [-p <password>] [-r <roles>]) |
([list] <username>) |
([passwd <username>] [-p <password>]) |
([roles <username>] [-a <roles>] [-r <roles>]) |
([userdel <username>])
```


## Description [_description_20]

If you use the built-in `file` internal realm, users are defined in local files on each node in the cluster.

Usernames and roles must be at least 1 and no more than 1024 characters. They can contain alphanumeric characters (`a-z`, `A-Z`, `0-9`), spaces, punctuation, and printable symbols in the [Basic Latin (ASCII) block](https://en.wikipedia.org/wiki/Basic_Latin_(Unicode_block)). Leading or trailing whitespace is not allowed.

Passwords must be at least 6 characters long.

For more information, see [File-based user authentication](docs-content://deploy-manage/users-roles/cluster-or-deployment-auth/file-based.md).

::::{tip}
To ensure that {{es}} can read the user and role information at startup, run `elasticsearch-users useradd` as the same user you use to run {{es}}. Running the command as root or some other user updates the permissions for the `users` and `users_roles` files and prevents {{es}} from accessing them.
::::



## Parameters [users-command-parameters]

`-a <roles>`
:   If used with the `roles` parameter, adds a comma-separated list of roles to a user.

`list`
:   List the users that are registered with the `file` realm on the local node. If you also specify a user name, the command provides information for that user.

`-p <password>`
:   Specifies the user’s password. If you do not specify this parameter, the command prompts you for the password.

    ::::{tip}
    Omit the `-p` option to keep plaintext passwords out of the terminal session’s command history.
    ::::


`passwd <username>`
:   Resets a user’s password. You can specify the new password directly with the `-p` parameter.

`-r <roles>`
:   * If used with the `useradd` parameter, defines a user’s roles. This option accepts a comma-separated list of role names to assign to the user.
* If used with the `roles` parameter, removes a comma-separated list of roles from a user.


`roles`
:   Manages the roles of a particular user. You can combine adding and removing roles within the same command to change a user’s roles.

`useradd <username>`
:   Adds a user to your local node.

`userdel <username>`
:   Deletes a user from your local node.


## Examples [_examples_24]

The following example adds a new user named `jacknich` to the `file` realm. The password for this user is `theshining`, and this user is associated with the `network` and `monitoring` roles.

```shell
bin/elasticsearch-users useradd jacknich -p theshining -r network,monitoring
```

The following example lists the users that are registered with the `file` realm on the local node:

```shell
bin/elasticsearch-users list
rdeniro        : admin
alpacino       : power_user
jacknich       : monitoring,network
```

Users are in the left-hand column and their corresponding roles are listed in the right-hand column.

The following example resets the `jacknich` user’s password:

```shell
bin/elasticsearch-users passwd jachnich
```

Since the `-p` parameter was omitted, the command prompts you to enter and confirm a password in interactive mode.

The following example removes the `network` and `monitoring` roles from the `jacknich` user and adds the `user` role:

```shell
bin/elasticsearch-users roles jacknich -r network,monitoring -a user
```

The following example deletes the `jacknich` user:

```shell
bin/elasticsearch-users userdel jacknich
```

