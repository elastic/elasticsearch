AWS Cloud Plugin for ElasticSearch
==================================

The AWS Cloud plugin allows to use AWS EC2 API for the unicast discovery mechanism as well as using S3 as a shared gateway.

In order to install the plugin, simply run: `bin/plugin -install elasticsearch/elasticsearch-cloud-aws/1.11.0`.

    ---------------------------------------
    | AWS Cloud Plugin | ElasticSearch    |
    ---------------------------------------
    | master           | 0.90 -> master   |
    ---------------------------------------
    | 1.11.0           | 0.90 -> master   |
    ---------------------------------------
    | 1.10.0           | 0.19 -> master   |
    ---------------------------------------
    | 1.9.0            | 0.19 -> master   |
    ---------------------------------------
    | 1.8.0            | 0.19 -> master   |
    ---------------------------------------
    | 1.7.0            | 0.19 -> master   |
    ---------------------------------------
    | 1.6.0            | 0.19 -> master   |
    ---------------------------------------
    | 1.5.0            | 0.19 -> master   |
    ---------------------------------------
    | 1.4.0            | 0.19 -> master   |
    ---------------------------------------
    | 1.3.0            | 0.19 -> master   |
    ---------------------------------------
    | 1.2.0            | 0.18             |
    ---------------------------------------
    | 1.1.0            | 0.18             |
    ---------------------------------------
    | 1.0.0            | 0.18             |
    ---------------------------------------

## Generic Configuration

The plugin will automatically use the instance level security credentials (as of 0.17), but they can be provided explicitly using `cloud.aws.access_key` and `cloud.aws.secret_key`:

    cloud:
        aws:
            access_key: AKVAIQBF2RECL7FJWGJQ
            secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br


### Region

The `cloud.aws.region` can be set to a region and will automatically use the relevant settings for both `ec2` and `s3`. The available values are: `us-east` (`us-east-1`), `us-west` (`us-west-1`), `us-west-1`, `us-west-2` `ap-southeast` (`ap-southeast-1`), `ap-southeast-1`, `ap-southeast-2`, `ap-northeast` (`ap-northeast-1`) `eu-west` (`eu-west-1`).


## EC2 Discovery

ec2 discovery allows to use the ec2 APIs to perform automatic discovery (similar to multicast in non hostile multicast environments). Here is a simple sample configuration:

    cloud:
        aws:
            access_key: AKVAIQBF2RECL7FJWGJQ
            secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br
    
    discovery:
        type: ec2

The following are a list of settings (prefixed with `discovery.ec2`) that can further control the discovery:

* `groups`: Either a comma separated list or array based list of (security) groups. Only instances with the provided security groups will be used in the cluster discovery.
* `host_type`: The type of host type to use to communicate with other instances. Can be one of `private_ip`, `public_ip`, `private_dns`, `public_dns`. Defaults to `private_ip`.
* `availability_zones`: Either a comma separated list or array based list of availability zones. Only instances within the provided availability zones will be used in the cluster discovery.
* `any_group`: If set to `false`, will require all security groups to be present for the instance to be used for the discovery. Defaults to `true`.
* `ping_timeout`: How long to wait for existing EC2 nodes to reply during discovery. Defaults to 3s.

### Filtering by Tags

The ec2 discovery can also filter machines to include in the cluster based on tags (and not just groups). The settings to use include the `discovery.ec2.tag.` prefix. For example, setting `discovery.ec2.tag.stage` to `dev` will only filter instances with a tag key set to `stage`, and a value of `dev`. Several tags set will require all of those tags to be set for the instance to be included.

One practical use for tag filtering is when an ec2 cluster contains many nodes that are not running elasticsearch. In this case (particularly with high `ping_timeout` values) there is a risk that a new node's discovery phase will end before it has found the cluster (which will result in it declaring itself master of a new cluster with the same name - highly undesirable). Tagging elasticsearch ec2 nodes and then filtering by that tag will resolve this issue.

### Automatic Node Attributes

Though not dependent on actually using `ec2` as discovery (but still requires the cloud aws plugin installed), the plugin can automatically add node attributes relating to ec2 (for example, availability zone, that can be used with the awareness allocation feature). In order to enable it, set `cloud.node.auto_attributes` to `true` in the settings.

## S3 Gateway

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

License
-------

    This software is licensed under the Apache 2 license, quoted below.

    Copyright 2009-2012 Shay Banon and ElasticSearch <http://www.elasticsearch.org>

    Licensed under the Apache License, Version 2.0 (the "License"); you may not
    use this file except in compliance with the License. You may obtain a copy of
    the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
    License for the specific language governing permissions and limitations under
    the License.
