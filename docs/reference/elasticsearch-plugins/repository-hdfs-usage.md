---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/repository-hdfs-usage.html
---

# Getting started with HDFS [repository-hdfs-usage]

The `repository-hdfs` snapshot/restore plugin uses the Apache Hadoop client libraries, version 3.4.1. Your HDFS implementation must be protocol-compatible with Apache Hadoop to use it with this plugin.

Even if Hadoop is already installed on the Elasticsearch nodes, for security reasons, the required libraries need to be placed under the plugin folder. Note that in most cases, if the distro is compatible, one simply needs to configure the repository with the appropriate Hadoop configuration files (see below).

Windows Users
:   Using Apache Hadoop on Windows is problematic and thus it is not recommended. For those *really* wanting to use it, make sure you place the elusive `winutils.exe` under the plugin folder and point `HADOOP_HOME` variable to it; this should minimize the amount of permissions Hadoop requires (though one would still have to add some more).

