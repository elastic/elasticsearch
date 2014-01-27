AWS Cloud Plugin for Elasticsearch
==================================

The AWS Cloud plugin allows to use AWS EC2 API for the unicast discovery mechanism as well as using S3 as a shared gateway.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-cloud-aws/2.0.0.RC1`.

|      AWS Cloud Plugin      |    elasticsearch    | Release date |
|----------------------------|---------------------|:------------:|
| 2.0.0-SNAPSHOT (master)    | 1.0.0.RC1 -> master |              |
| 2.0.0.RC1                  | 1.0.0.RC1 -> master |  2014-01-15  |
| 1.17.0-SNAPSHOT (1.x)      | 0.90.4 -> 0.90      |              |
| 1.16.0                     | 0.90.4 -> 0.90      |  2013-11-26  |
| 1.15.0                     | 0.90.4 -> 0.90      |  2013-09-16  |
| 1.14.0                     | 0.90.3              |  2013-08-09  |
| 1.13.0                     | broken              |  2013-08-09  |
| 1.12.0                     | 0.90 -> 0.90.2      |  2013-05-31  |
| 1.11.0                     | 0.90 -> 0.90.2      |  2013-02-26  |
| 1.10.0                     | 0.19                |  2012-12-05  |
| 1.9.0                      | 0.19                |  2012-08-25  |
| 1.8.0                      | 0.19                |  2012-06-27  |
| 1.7.0                      | 0.19                |  2012-06-20  |
| 1.6.0                      | 0.19                |  2012-06-01  |
| 1.5.0                      | 0.19                |  2012-03-07  |
| 1.4.0                      | 0.19                |  2012-03-03  |
| 1.3.0                      | 0.19                |  2012-02-07  |
| 1.2.0                      | 0.18                |  2012-01-02  |
| 1.1.0                      | 0.18                |  2012-01-02  |
| 1.0.0                      | 0.18                |  2011-12-05  |

## Generic Configuration

The plugin will automatically use the instance level security credentials (as of 1.7.0), but they can be provided explicitly using `cloud.aws.access_key` and `cloud.aws.secret_key`:

    cloud:
        aws:
            access_key: AKVAIQBF2RECL7FJWGJQ
            secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br


### Region

The `cloud.aws.region` can be set to a region and will automatically use the relevant settings for both `ec2` and `s3`. The available values are: `us-east` (`us-east-1`), `us-west` (`us-west-1`), `us-west-1`, `us-west-2` `ap-southeast` (`ap-southeast-1`), `ap-southeast-1`, `ap-southeast-2`, `ap-northeast` (`ap-northeast-1`) `eu-west` (`eu-west-1`), `sa-east` (`sa-east-1`).


## EC2 Discovery

ec2 discovery allows to use the ec2 APIs to perform automatic discovery (similar to multicast in non hostile multicast environments). Here is a simple sample configuration:

    cloud:
        aws:
            access_key: AKVAIQBF2RECL7FJWGJQ
            secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br
    
    discovery:
        type: ec2

The following are a list of settings (prefixed with `discovery.ec2`) that can further control the discovery:

* `groups`: Either a comma separated list or array based list of (security) groups. Only instances with the provided security groups will be used in the cluster discovery. (NOTE: You could provide either group NAME or group ID.)
* `host_type`: The type of host type to use to communicate with other instances. Can be one of `private_ip`, `public_ip`, `private_dns`, `public_dns`. Defaults to `private_ip`.
* `availability_zones`: Either a comma separated list or array based list of availability zones. Only instances within the provided availability zones will be used in the cluster discovery.
* `any_group`: If set to `false`, will require all security groups to be present for the instance to be used for the discovery. Defaults to `true`.
* `ping_timeout`: How long to wait for existing EC2 nodes to reply during discovery. Defaults to `3s`. If no unit like `ms`, `s` or `m` is specified, milliseconds are used.

### Filtering by Tags

The ec2 discovery can also filter machines to include in the cluster based on tags (and not just groups). The settings to use include the `discovery.ec2.tag.` prefix. For example, setting `discovery.ec2.tag.stage` to `dev` will only filter instances with a tag key set to `stage`, and a value of `dev`. Several tags set will require all of those tags to be set for the instance to be included.

One practical use for tag filtering is when an ec2 cluster contains many nodes that are not running elasticsearch. In this case (particularly with high `ping_timeout` values) there is a risk that a new node's discovery phase will end before it has found the cluster (which will result in it declaring itself master of a new cluster with the same name - highly undesirable). Tagging elasticsearch ec2 nodes and then filtering by that tag will resolve this issue.

### Automatic Node Attributes

Though not dependent on actually using `ec2` as discovery (but still requires the cloud aws plugin installed), the plugin can automatically add node attributes relating to ec2 (for example, availability zone, that can be used with the awareness allocation feature). In order to enable it, set `cloud.node.auto_attributes` to `true` in the settings.

## S3 Gateway

*note*: As explained [here](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-gateway-s3.html) S3 Gateway functionality is being deprecated. Please use [local gateway](http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-gateway-local.html) instead.

s3 based gateway allows to do long term reliable async persistency of the cluster state and indices directly to Amazon s3. Note, this is a shared gateway where the indices are periodically persisted to s3 while being served from the data location of each node. 

Here is how it can be configured:

    cloud:
        aws:
            access_key: AKVAIQBF2RECL7FJWGJQ
            secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br
    
    
    gateway:
        type: s3
        s3:
            bucket: bucket_name

The following are a list of settings (prefixed with `gateway.s3`) that can further control the s3 gateway:

* `chunk_size`: Big files are broken down into chunks (to overcome AWS 5g limit and use concurrent snapshotting). Default set to `100m`.

### concurrent_streams

The `gateway.s3.concurrent_streams` allow to throttle the number of streams (per node) opened against the shared gateway performing the snapshot operation. It defaults to `5`.

## S3 Repository

The S3 repository is using S3 to store snapshots. The S3 repository can be created using the following command:

    $ curl -XPUT 'http://localhost:9200/_snapshot/my_s3_repository' -d '{
        "type": "s3",
        "settings": {
            "bucket": "my_backet_name",
            "region": "us-west"
        }
    }'

The following settings are supported:

* `bucket`: The name of the bucket to be used for snapshots. (Mandatory)
* `region`: The region where bucket is located. Defaults to US Standard
* `base_path`: Specifies the path within bucket to repository data. Defaults to root directory.
* `concurrent_streams`: Throttles the number of streams (per node) preforming snapshot operation. Defaults to `5`.
* `chunk_size`: Big files can be broken down into chunks during snapshotting if needed. The chunk size can be specified in bytes or by using size value notation, i.e. `1g`, `10m`, `5k`. Defaults to `100m`.
* `compress`: When set to `true` metadata files are stored in compressed format. This setting doesn't affect index files that are already compressed by default. Defaults to `false`.

The S3 repositories are using the same credentials as the rest of the S3 services provided by this plugin (`discovery` and `gateway`). They can be configured the following way:

    cloud:
        aws:
            access_key: AKVAIQBF2RECL7FJWGJQ
            secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br


Multiple S3 repositories can be created as long as they share the same credential.

## Testing

Integrations tests in this plugin require working AWS configuration and therefore disabled by default. To enable tests prepare a config file elasticsearch.yml with the following content:

```
cloud:
    aws:
        access_key: AKVAIQBF2RECL7FJWGJQ
        secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br

repositories:
    s3:
        bucket: "bucket_name"
        region: "us-west-2"

```

Replaces `access_key`, `secret_key`, `bucket` and `region` with your settings. Please, note that the test will delete all snapshot/restore related files in the specified bucket.

To run test:

```sh
mvn -Dtests.aws=true -Des.config=/path/to/config/file/elasticsearch.yml clean test
```


License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2014 Elasticsearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
