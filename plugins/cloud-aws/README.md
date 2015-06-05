AWS Cloud Plugin for Elasticsearch
==================================

The Amazon Web Service (AWS) Cloud plugin allows to use [AWS API](https://github.com/aws/aws-sdk-java)
for the unicast discovery mechanism and add S3 repositories.

In order to install the plugin, run: 

```sh
bin/plugin install elasticsearch/elasticsearch-cloud-aws/2.5.1
```

You need to install a version matching your Elasticsearch version:

|       Elasticsearch    |  AWS Cloud Plugin |                                                             Docs                                                                   |
|------------------------|-------------------|------------------------------------------------------------------------------------------------------------------------------------|
|    master              | Build from source | See below                                                                                                                          |
|    es-1.x              | Build from source | [2.6.0-SNAPSHOT](https://github.com/elasticsearch/elasticsearch-cloud-aws/tree/es-1.x/#version-260-snapshot-for-elasticsearch-1x)  |
|    es-1.5              |     2.5.1         | [2.5.1](https://github.com/elastic/elasticsearch-cloud-aws/tree/v2.5.1/#version-251-for-elasticsearch-15)                  |
|    es-1.4              |     2.4.2         | [2.4.2](https://github.com/elasticsearch/elasticsearch-cloud-aws/tree/v2.4.2/#version-242-for-elasticsearch-14)                  |
|    es-1.3              |     2.3.0         | [2.3.0](https://github.com/elasticsearch/elasticsearch-cloud-aws/tree/v2.3.0/#version-230-for-elasticsearch-13)                    |
|    es-1.2              |     2.2.0         | [2.2.0](https://github.com/elasticsearch/elasticsearch-cloud-aws/tree/v2.2.0/#aws-cloud-plugin-for-elasticsearch)                  |
|    es-1.1              |     2.1.1         | [2.1.1](https://github.com/elasticsearch/elasticsearch-cloud-aws/tree/v2.1.1/#aws-cloud-plugin-for-elasticsearch)                  |
|    es-1.0              |     2.0.0         | [2.0.0](https://github.com/elasticsearch/elasticsearch-cloud-aws/tree/v2.0.0/#aws-cloud-plugin-for-elasticsearch)                  |
|    es-0.90             |     1.16.0        | [1.16.0](https://github.com/elasticsearch/elasticsearch-cloud-aws/tree/v1.16.0/#aws-cloud-plugin-for-elasticsearch)                |

To build a `SNAPSHOT` version, you need to build it with Maven:

```bash
mvn clean install
plugin --install cloud-aws \ 
       --url file:target/releases/elasticsearch-cloud-aws-X.X.X-SNAPSHOT.zip
```

## Generic Configuration

The plugin will default to using [IAM Role](http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html) credentials 
for authentication. These can be overridden by, in increasing order of precedence, system properties `aws.accessKeyId` and `aws.secretKey`, 
environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_KEY`, or the elasticsearch config using `cloud.aws.access_key` and `cloud.aws.secret_key`:
 
```
cloud:
    aws:
        access_key: AKVAIQBF2RECL7FJWGJQ
        secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br
```

### Transport security

By default this plugin uses HTTPS for all API calls to AWS endpoints. If you wish to configure HTTP you can set 
`cloud.aws.protocol` in the elasticsearch config. You can optionally override this setting per individual service 
via: `cloud.aws.ec2.protocol` or `cloud.aws.s3.protocol`. 

```
cloud:
    aws:
        protocol: https
        s3: 
            protocol: http
        ec2: 
            protocol: https
```

In addition, a proxy can be configured with the `proxy_host` and `proxy_port` settings (note that protocol can be `http` or `https`):

```
cloud:
    aws:
        protocol: https
        proxy_host: proxy1.company.com
        proxy_port: 8083
```

You can also set different proxies for `ec2` and `s3`:

```
cloud:
    aws:
        s3:
            proxy_host: proxy1.company.com
            proxy_port: 8083
        ec2:
            proxy_host: proxy2.company.com
            proxy_port: 8083
```

### Region

The `cloud.aws.region` can be set to a region and will automatically use the relevant settings for both `ec2` and `s3`. The available values are:

* `us-east` (`us-east-1`)
* `us-west` (`us-west-1`)
* `us-west-1`
* `us-west-2`
* `ap-southeast` (`ap-southeast-1`)
* `ap-southeast-1`
* `ap-southeast-2`
* `ap-northeast` (`ap-northeast-1`)
* `eu-west` (`eu-west-1`)
* `eu-central` (`eu-central-1`)
* `sa-east` (`sa-east-1`)
* `cn-north` (`cn-north-1`)


### EC2/S3 Signer API

If you are using a compatible EC2 or S3 service, they might be using an older API to sign the requests.
You can set your compatible signer API using `cloud.aws.signer` (or `cloud.aws.ec2.signer` and `cloud.aws.s3.signer`)
with the right signer to use. Defaults to `AWS4SignerType`.


## EC2 Discovery

ec2 discovery allows to use the ec2 APIs to perform automatic discovery (similar to multicast in non hostile multicast environments). Here is a simple sample configuration:

```
discovery:
    type: ec2
```

The ec2 discovery is using the same credentials as the rest of the AWS services provided by this plugin (`repositories`).
See [Generic Configuration](#generic-configuration) for details.

The following are a list of settings (prefixed with `discovery.ec2`) that can further control the discovery:

* `groups`: Either a comma separated list or array based list of (security) groups. Only instances with the provided security groups will be used in the cluster discovery. (NOTE: You could provide either group NAME or group ID.)
* `host_type`: The type of host type to use to communicate with other instances. Can be one of `private_ip`, `public_ip`, `private_dns`, `public_dns`. Defaults to `private_ip`.
* `availability_zones`: Either a comma separated list or array based list of availability zones. Only instances within the provided availability zones will be used in the cluster discovery.
* `any_group`: If set to `false`, will require all security groups to be present for the instance to be used for the discovery. Defaults to `true`.
* `ping_timeout`: How long to wait for existing EC2 nodes to reply during discovery. Defaults to `3s`. If no unit like `ms`, `s` or `m` is specified, milliseconds are used.

### Recommended EC2 Permissions

EC2 discovery requires making a call to the EC2 service. You'll want to setup an IAM policy to allow this. You can create a custom policy via the IAM Management Console. It should look similar to this.

```js
{
    "Statement": [
        {
            "Action": [
                "ec2:DescribeInstances"
            ],
            "Effect": "Allow",
            "Resource": [
                "*"
            ]
        }
    ],
    "Version": "2012-10-17"
}
```


### Filtering by Tags

The ec2 discovery can also filter machines to include in the cluster based on tags (and not just groups). The settings to use include the `discovery.ec2.tag.` prefix. For example, setting `discovery.ec2.tag.stage` to `dev` will only filter instances with a tag key set to `stage`, and a value of `dev`. Several tags set will require all of those tags to be set for the instance to be included.

One practical use for tag filtering is when an ec2 cluster contains many nodes that are not running elasticsearch. In this case (particularly with high `ping_timeout` values) there is a risk that a new node's discovery phase will end before it has found the cluster (which will result in it declaring itself master of a new cluster with the same name - highly undesirable). Tagging elasticsearch ec2 nodes and then filtering by that tag will resolve this issue.

### Automatic Node Attributes

Though not dependent on actually using `ec2` as discovery (but still requires the cloud aws plugin installed), the plugin can automatically add node attributes relating to ec2 (for example, availability zone, that can be used with the awareness allocation feature). In order to enable it, set `cloud.node.auto_attributes` to `true` in the settings.


### Using other EC2 endpoint

If you are using any EC2 api compatible service, you can set the endpoint you want to use by setting `cloud.aws.ec2.endpoint`
to your URL provider.

## S3 Repository

The S3 repository is using S3 to store snapshots. The S3 repository can be created using the following command:

```sh
$ curl -XPUT 'http://localhost:9200/_snapshot/my_s3_repository' -d '{
    "type": "s3",
    "settings": {
        "bucket": "my_bucket_name",
        "region": "us-west"
    }
}'
```

The following settings are supported:

* `bucket`: The name of the bucket to be used for snapshots. (Mandatory)
* `region`: The region where bucket is located. Defaults to US Standard
* `endpoint`: The endpoint to the S3 API. Defaults to AWS's default S3 endpoint. Note that setting a region overrides the endpoint setting.
* `protocol`: The protocol to use (`http` or `https`). Defaults to value of `cloud.aws.protocol` or `cloud.aws.s3.protocol`.
* `base_path`: Specifies the path within bucket to repository data. Defaults to root directory.
* `access_key`: The access key to use for authentication. Defaults to value of `cloud.aws.access_key`.
* `secret_key`: The secret key to use for authentication. Defaults to value of `cloud.aws.secret_key`.
* `chunk_size`: Big files can be broken down into chunks during snapshotting if needed. The chunk size can be specified in bytes or by using size value notation, i.e. `1g`, `10m`, `5k`. Defaults to `100m`.
* `compress`: When set to `true` metadata files are stored in compressed format. This setting doesn't affect index files that are already compressed by default. Defaults to `false`.
* `server_side_encryption`: When set to `true` files are encrypted on server side using AES256 algorithm. Defaults to `false`.
* `buffer_size`: Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold, the S3 repository will use the [AWS Multipart Upload API](http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html) to split the chunk into several parts, each of `buffer_size` length, and to upload each part in its own request. Note that positionning a buffer size lower than `5mb` is not allowed since it will prevents the use of the Multipart API and may result in upload errors. Defaults to `5mb`.
* `max_retries`: Number of retries in case of S3 errors. Defaults to `3`.

The S3 repositories are using the same credentials as the rest of the AWS services provided by this plugin (`discovery`).
See [Generic Configuration](#generic-configuration) for details.

Multiple S3 repositories can be created. If the buckets require different credentials, then define them as part of the repository settings.

### Recommended S3 Permissions

In order to restrict the Elasticsearch snapshot process to the minimum required resources, we recommend using Amazon IAM in conjunction with pre-existing S3 buckets. Here is an example policy which will allow the snapshot access to an S3 bucket named "snaps.example.com". This may be configured through the AWS IAM console, by creating a Custom Policy, and using a Policy Document similar to this (changing snaps.example.com to your bucket name).

```js
{
    "Statement": [
        {
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:ListBucketMultipartUploads",
                "s3:ListBucketVersions"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::snaps.example.com"
            ]
        },
        {
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::snaps.example.com/*"
            ]
        }
    ],
    "Version": "2012-10-17"
}
```

You may further restrict the permissions by specifying a prefix within the bucket, in this example, named "foo".

```js
{
    "Statement": [
        {
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:ListBucketMultipartUploads",
                "s3:ListBucketVersions"
            ],
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "foo/*"
                    ]
                }
            },
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::snaps.example.com"
            ]
        },
        {
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:AbortMultipartUpload",
                "s3:ListMultipartUploadParts"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::snaps.example.com/foo/*"
            ]
        }
    ],
    "Version": "2012-10-17"
}
```

The bucket needs to exist to register a repository for snapshots. If you did not create the bucket then the repository registration will fail. If you want elasticsearch to create the bucket instead, you can add the permission to create a specific bucket like this:

```js
{
   "Action": [
      "s3:CreateBucket"
   ],
   "Effect": "Allow",
   "Resource": [
      "arn:aws:s3:::snaps.example.com"
   ]
}
```

### Using other S3 endpoint

If you are using any S3 api compatible service, you can set a global endpoint by setting `cloud.aws.s3.endpoint`
to your URL provider. Note that this setting will be used for all S3 repositories.

Different `endpoint`, `region` and `protocol` settings can be set on a per-repository basis (see [S3 Repository](#s3-repository) section for detail).


## Testing

Integrations tests in this plugin require working AWS configuration and therefore disabled by default. Three buckets and two iam users have to be created. The first iam user needs access to two buckets in different regions and the final bucket is exclusive for the other iam user. To enable tests prepare a config file elasticsearch.yml with the following content:

```
cloud:
    aws:
        access_key: AKVAIQBF2RECL7FJWGJQ
        secret_key: vExyMThREXeRMm/b/LRzEB8jWwvzQeXgjqMX+6br

repositories:
    s3:
        bucket: "bucket_name"
        region: "us-west-2"
        private-bucket:
            bucket: <bucket not accessible by default key>
            access_key: <access key>
            secret_key: <secret key>
        remote-bucket:
            bucket: <bucket in other region>
            region: <region>
	external-bucket:
	    bucket: <bucket>
	    access_key: <access key>
	    secret_key: <secret key>
	    endpoint: <endpoint>
	    protocol: <protocol>

```

Replace all occurrences of `access_key`, `secret_key`, `endpoint`, `protocol`, `bucket` and `region` with your settings. Please, note that the test will delete all snapshot/restore related files in the specified buckets.

To run test:

```sh
mvn -Dtests.aws=true -Dtests.config=/path/to/config/file/elasticsearch.yml clean test
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
