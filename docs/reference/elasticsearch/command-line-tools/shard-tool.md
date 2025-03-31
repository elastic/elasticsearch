---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/shard-tool.html
---

# elasticsearch-shard [shard-tool]

In some cases the Lucene index or translog of a shard copy can become corrupted. The `elasticsearch-shard` command enables you to remove corrupted parts of the shard if a good copy of the shard cannot be recovered automatically or restored from backup.

::::{warning}
You will lose the corrupted data when you run `elasticsearch-shard`. This tool should only be used as a last resort if there is no way to recover from another copy of the shard or restore a snapshot.
::::



## Synopsis [_synopsis_11]

```shell
bin/elasticsearch-shard remove-corrupted-data
  ([--index <Index>] [--shard-id <ShardId>] | [--dir <IndexPath>])
  [--truncate-clean-translog]
  [-E <KeyValuePair>]
  [-h, --help] ([-s, --silent] | [-v, --verbose])
```


## Description [_description_18]

When {{es}} detects that a shard’s data is corrupted, it fails that shard copy and refuses to use it. Under normal conditions, the shard is automatically recovered from another copy. If no good copy of the shard is available and you cannot restore one from a snapshot, you can use `elasticsearch-shard` to remove the corrupted data and restore access to any remaining data in unaffected segments.

::::{warning}
Stop Elasticsearch before running `elasticsearch-shard`.
::::


To remove corrupted shard data use the `remove-corrupted-data` subcommand.

There are two ways to specify the path:

* Specify the index name and shard name with the `--index` and `--shard-id` options.
* Use the `--dir` option to specify the full path to the corrupted index or translog files.

$$$cli-tool-jvm-options-shard$$$


### JVM options [_jvm_options_3]

CLI tools run with 64MB of heap. For most tools, this value is fine. However, if needed this can be overridden by setting the `CLI_JAVA_OPTS` environment variable. For example, the following increases the heap size used by the `elasticsearch-shard` tool to 1GB.

```shell
export CLI_JAVA_OPTS="-Xmx1g"
bin/elasticsearch-shard ...
```


### Removing corrupted data [_removing_corrupted_data]

`elasticsearch-shard` analyses the shard copy and provides an overview of the corruption found. To proceed you must then confirm that you want to remove the corrupted data.

::::{warning}
Back up your data before running `elasticsearch-shard`. This is a destructive operation that removes corrupted data from the shard.
::::


```txt
$ bin/elasticsearch-shard remove-corrupted-data --index my-index-000001 --shard-id 0


    WARNING: Elasticsearch MUST be stopped before running this tool.

  Please make a complete backup of your index before using this tool.


Opening Lucene index at /var/lib/elasticsearchdata/indices/P45vf_YQRhqjfwLMUvSqDw/0/index/

 >> Lucene index is corrupted at /var/lib/elasticsearchdata/indices/P45vf_YQRhqjfwLMUvSqDw/0/index/

Opening translog at /var/lib/elasticsearchdata/indices/P45vf_YQRhqjfwLMUvSqDw/0/translog/


 >> Translog is clean at /var/lib/elasticsearchdata/indices/P45vf_YQRhqjfwLMUvSqDw/0/translog/


  Corrupted Lucene index segments found - 32 documents will be lost.

            WARNING:              YOU WILL LOSE DATA.

Continue and remove docs from the index ? Y

WARNING: 1 broken segments (containing 32 documents) detected
Took 0.056 sec total.
Writing...
OK
Wrote new segments file "segments_c"
Marking index with the new history uuid : 0pIBd9VTSOeMfzYT6p0AsA
Changing allocation id V8QXk-QXSZinZMT-NvEq4w to tjm9Ve6uTBewVFAlfUMWjA

You should run the following command to allocate this shard:

POST /_cluster/reroute
{
  "commands" : [
    {
      "allocate_stale_primary" : {
        "index" : "index42",
        "shard" : 0,
        "node" : "II47uXW2QvqzHBnMcl2o_Q",
        "accept_data_loss" : false
      }
    }
  ]
}

You must accept the possibility of data loss by changing the `accept_data_loss` parameter to `true`.

Deleted corrupt marker corrupted_FzTSBSuxT7i3Tls_TgwEag from /var/lib/elasticsearchdata/indices/P45vf_YQRhqjfwLMUvSqDw/0/index/
```

When you use `elasticsearch-shard` to drop the corrupted data, the shard’s allocation ID changes. After restarting the node, you must use the [cluster reroute API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-cluster-reroute) to tell Elasticsearch to use the new ID. The `elasticsearch-shard` command shows the request that you need to submit.

You can also use the `-h` option to get a list of all options and parameters that the `elasticsearch-shard` tool supports.

Finally, you can use the `--truncate-clean-translog` option to truncate the shard’s translog even if it does not appear to be corrupt.

