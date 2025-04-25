---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/advanced-configuration.html
applies_to:
  stack: all
---

# JVM settings [advanced-configuration]

$$$set-jvm-options$$$
If needed, you can override the default JVM options by adding custom options files (preferred) or setting the `ES_JAVA_OPTS` environment variable.

JVM options files must have the suffix *.options* and contain a line-delimited list of JVM arguments. JVM processes options files in lexicographic order.

Where you put the JVM options files depends on the type of installation:

* tar.gz or .zip: Add custom JVM options files to `config/jvm.options.d/`.
* Debian or RPM: Add custom JVM options files to `/etc/elasticsearch/jvm.options.d/`.
* Docker: Bind mount custom JVM options files into `/usr/share/elasticsearch/config/jvm.options.d/`.

::::{warning}
Setting your own JVM options is generally not recommended and could negatively impact performance and stability. Using the {{es}}-provided defaults is recommended in most circumstances.
::::


::::{note}
:name: readiness-tcp-port

Do not modify the root `jvm.options` file. Use files in `jvm.options.d/` instead.
::::


## JVM options syntax [jvm-options-syntax]

A JVM options file contains a line-delimited list of JVM arguments. Arguments are preceded by a dash (`-`). To apply the setting to specific versions, prepend the version or a range of versions followed by a colon.

* Apply a setting to all versions:

    ```text
    -Xmx2g
    ```

* Apply a setting to a specific version:

    ```text
    17:-Xmx2g
    ```

* Apply a setting to a range of versions:

    ```text
    17-18:-Xmx2g
    ```

    To apply a setting to a specific version and any later versions, omit the upper bound of the range. For example, this setting applies to Java 17 and later:

    ```text
    17-:-Xmx2g
    ```


Blank lines are ignored. Lines beginning with `#` are treated as comments and ignored. Lines that aren’t commented out and aren’t recognized as valid JVM arguments are rejected and {{es}} will fail to start.


## Use environment variables to set JVM options [jvm-options-env]

In production, use JVM options files to override the default settings. In testing and development environments, you can also set JVM options through the `ES_JAVA_OPTS` environment variable.

```sh
export ES_JAVA_OPTS="$ES_JAVA_OPTS -Djava.io.tmpdir=/path/to/temp/dir"
./bin/elasticsearch
```

If you’re using the RPM or Debian packages, you can specify `ES_JAVA_OPTS` in the [system configuration file](docs-content://deploy-manage/deploy/self-managed/setting-system-settings.md#sysconfig).

::::{note}
{{es}} ignores the `JAVA_TOOL_OPTIONS` and `JAVA_OPTS` environment variables.
::::



## Set the JVM heap size [set-jvm-heap-size]

By default, {{es}} automatically sets the JVM heap size based on a node’s [roles](/reference/elasticsearch/configuration-reference/node-settings.md#node-roles) and total memory. Using the default sizing is recommended for most production environments.

To override the default heap size, set the minimum and maximum heap size settings, `Xms` and `Xmx`. The minimum and maximum values must be the same.

The heap size should be based on the available RAM:

* Set `Xms` and `Xmx` to no more than 50% of your total memory. {{es}} requires memory for purposes other than the JVM heap. For example, {{es}} uses off-heap buffers for efficient network communication and relies on the operating system’s filesystem cache for efficient access to files. The JVM itself also requires some memory. It’s normal for {{es}} to use more memory than the limit configured with the `Xmx` setting.

    ::::{note}
    When running in a container, such as [Docker](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-docker.md), total memory is defined as the amount of memory visible to the container, not the total system memory on the host.
    ::::

* Set `Xms` and `Xmx` to no more than the threshold for compressed ordinary object pointers (oops). The exact threshold varies but 26GB is safe on most systems and can be as large as 30GB on some systems. To verify you are under the threshold, check the {{es}} log for an entry like this:

    ```txt
    heap size [1.9gb], compressed ordinary object pointers [true]
    ```

    Or check the `jvm.using_compressed_ordinary_object_pointers` value for the nodes using the [nodes info API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-nodes-info):

    ```console
    GET _nodes/_all/jvm
    ```


The more heap available to {{es}}, the more memory it can use for its internal caches. This leaves less memory for the operating system to use for the filesystem cache. Larger heaps can also cause longer garbage collection pauses.

To configure the heap size, add the `Xms` and `Xmx` JVM arguments to a custom JVM options file with the extension `.options` and store it in the `jvm.options.d/` directory. For example, to set the maximum heap size to 2GB, set both `Xms` and `Xmx` to `2g`:

```txt
-Xms2g
-Xmx2g
```

For testing, you can also set the heap sizes using the `ES_JAVA_OPTS` environment variable:

```sh
ES_JAVA_OPTS="-Xms2g -Xmx2g" ./bin/elasticsearch
```

The `ES_JAVA_OPTS` variable overrides all other JVM options. We do not recommend using `ES_JAVA_OPTS` in production.

::::{note}
If you are running {{es}} as a Windows service, you can change the heap size using the service manager. See [Install and run {{es}} as a service on Windows](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-zip-on-windows.md#windows-service).
::::



## JVM heap dump path setting [heap-dump-path-setting]

By default, {{es}} configures the JVM to dump the heap on out of memory exceptions to the default logs directory. On [RPM](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-rpm.md) and [Debian](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-debian-package.md) packages, the logs directory is `/var/log/elasticsearch`. On [Linux and MacOS](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-from-archive-on-linux-macos.md) and [Windows](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-zip-on-windows.md) distributions, the `logs` directory is located under the root of the {{es}} installation.

If this path is not suitable for receiving heap dumps, add the `-XX:HeapDumpPath=...` entry in [`jvm.options`](#set-jvm-options):

* If you specify a directory, the JVM will generate a filename for the heap dump based on the PID of the running instance.
* If you specify a fixed filename instead of a directory, the file must not exist when the JVM needs to perform a heap dump on an out of memory exception. Otherwise, the heap dump will fail.


## GC logging settings [gc-logging]

By default, {{es}} enables garbage collection (GC) logs. These are configured in [`jvm.options`](#set-jvm-options) and output to the same default location as the {{es}} logs. The default configuration rotates the logs every 64 MB and can consume up to 2 GB of disk space.

You can reconfigure JVM logging using the command line options described in [JEP 158: Unified JVM Logging](https://openjdk.java.net/jeps/158). Unless you change the default `jvm.options` file directly, the {{es}} default configuration is applied in addition to your own settings. To disable the default configuration, first disable logging by supplying the `-Xlog:disable` option, then supply your own command line options. This disables *all* JVM logging, so be sure to review the available options and enable everything that you require.

To see further options not contained in the original JEP, see [Enable Logging with the JVM Unified Logging Framework](https://docs.oracle.com/en/java/javase/13/docs/specs/man/java.html#enable-logging-with-the-jvm-unified-logging-framework).


### Examples [_examples_2]

Change the default GC log output location to `/opt/my-app/gc.log` by creating `$ES_HOME/config/jvm.options.d/gc.options` with some sample options:

```shell
# Turn off all previous logging configuratons
-Xlog:disable

# Default settings from JEP 158, but with `utctime` instead of `uptime` to match the next line
-Xlog:all=warning:stderr:utctime,level,tags

# Enable GC logging to a custom location with a variety of options
-Xlog:gc*,gc+age=trace,safepoint:file=/opt/my-app/gc.log:utctime,level,pid,tags:filecount=32,filesize=64m
```

Configure an {{es}} [Docker container](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-docker.md) to send GC debug logs to standard error (`stderr`). This lets the container orchestrator handle the output. If using the `ES_JAVA_OPTS` environment variable, specify:

```sh
MY_OPTS="-Xlog:disable -Xlog:all=warning:stderr:utctime,level,tags -Xlog:gc=debug:stderr:utctime"
docker run -e ES_JAVA_OPTS="$MY_OPTS" # etc
```


## JVM fatal error log setting [error-file-path]

By default, {{es}} configures the JVM to write fatal error logs to the default logging directory. On [RPM](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-rpm.md) and [Debian](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-debian-package.md) packages, this directory is `/var/log/elasticsearch`. On [Linux and MacOS](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-from-archive-on-linux-macos.md) and [Windows](docs-content://deploy-manage/deploy/self-managed/install-elasticsearch-with-zip-on-windows.md) distributions, the `logs` directory is located under the root of the {{es}} installation.

These are logs produced by the JVM when it encounters a fatal error, such as a segmentation fault. If this path is not suitable for receiving logs, modify the `-XX:ErrorFile=...` entry in [`jvm.options`](#set-jvm-options).
