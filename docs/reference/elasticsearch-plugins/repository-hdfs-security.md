---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs-security.html
---

# Hadoop security [repository-hdfs-security]

The HDFS repository plugin integrates seamlessly with Hadoop’s authentication model. The following authentication methods are supported by the plugin:

`simple`
:   Also means "no security" and is enabled by default. Uses information from underlying operating system account running Elasticsearch to inform Hadoop of the name of the current user. Hadoop makes no attempts to verify this information.

`kerberos`
:   Authenticates to Hadoop through the usage of a Kerberos principal and keytab. Interfacing with HDFS clusters secured with Kerberos requires a few additional steps to enable (See [Principals and keytabs](#repository-hdfs-security-keytabs) and [Creating the secure repository](#repository-hdfs-security-runtime) for more info)


## Principals and keytabs [repository-hdfs-security-keytabs]

Before attempting to connect to a secured HDFS cluster, provision the Kerberos principals and keytabs that the Elasticsearch nodes will use for authenticating to Kerberos. For maximum security and to avoid tripping up the Kerberos replay protection, you should create a service principal per node, following the pattern of `elasticsearch/hostname@REALM`.

::::{warning}
In some cases, if the same principal is authenticating from multiple clients at once, services may reject authentication for those principals under the assumption that they could be replay attacks. If you are running the plugin in production with multiple nodes you should be using a unique service principal for each node.
::::


On each Elasticsearch node, place the appropriate keytab file in the node’s configuration location under the `repository-hdfs` directory using the name `krb5.keytab`:

```bash
$> cd elasticsearch/config
$> ls
elasticsearch.yml  jvm.options        log4j2.properties  repository-hdfs/   scripts/
$> cd repository-hdfs
$> ls
krb5.keytab
```

::::{note}
Make sure you have the correct keytabs! If you are using a service principal per node (like `elasticsearch/hostname@REALM`) then each node will need its own unique keytab file for the principal assigned to that host!
::::



## Creating the secure repository [repository-hdfs-security-runtime]

Once your keytab files are in place and your cluster is started, creating a secured HDFS repository is simple. Just add the name of the principal that you will be authenticating as in the repository settings under the `security.principal` option:

```console
PUT _snapshot/my_hdfs_repository
{
  "type": "hdfs",
  "settings": {
    "uri": "hdfs://namenode:8020/",
    "path": "/user/elasticsearch/repositories/my_hdfs_repository",
    "security.principal": "elasticsearch@REALM"
  }
}
```

If you are using different service principals for each node, you can use the `_HOST` pattern in your principal name. Elasticsearch will automatically replace the pattern with the hostname of the node at runtime:

```console
PUT _snapshot/my_hdfs_repository
{
  "type": "hdfs",
  "settings": {
    "uri": "hdfs://namenode:8020/",
    "path": "/user/elasticsearch/repositories/my_hdfs_repository",
    "security.principal": "elasticsearch/_HOST@REALM"
  }
}
```


## Authorization [repository-hdfs-security-authorization]

Once Elasticsearch is connected and authenticated to HDFS, HDFS will infer a username to use for authorizing file access for the client. By default, it picks this username from the primary part of the kerberos principal used to authenticate to the service. For example, in the case of a principal like `elasticsearch@REALM` or `elasticsearch/hostname@REALM` then the username that HDFS extracts for file access checks will be `elasticsearch`.

::::{note}
The repository plugin makes no assumptions of what Elasticsearch’s principal name is. The main fragment of the Kerberos principal is not required to be `elasticsearch`. If you have a principal or service name that works better for you or your organization then feel free to use it instead!
::::


