---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/discovery-ec2-usage.html
---

# Using the EC2 discovery plugin [discovery-ec2-usage]

The `discovery-ec2` plugin allows {{es}} to find the master-eligible nodes in a cluster running on AWS EC2 by querying the [AWS API](https://github.com/aws/aws-sdk-java) for the addresses of the EC2 instances running these nodes.

It is normally a good idea to restrict the discovery process just to the master-eligible nodes in the cluster. This plugin allows you to identify these nodes by certain criteria including their tags, their membership of security groups, and their placement within availability zones. The discovery process will work correctly even if it finds master-ineligible nodes, but master elections will be more efficient if this can be avoided.

The interaction with the AWS API can be authenticated using the [instance role](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html), or else custom credentials can be supplied.

## Enabling EC2 discovery [_enabling_ec2_discovery]

To enable EC2 discovery, configure {{es}} to use the `ec2` seed hosts provider:

```yaml
discovery.seed_providers: ec2
```


## Configuring EC2 discovery [_configuring_ec2_discovery]

EC2 discovery supports a number of settings. Some settings are sensitive and must be stored in the {{es}} keystore. For example, to authenticate using a particular access key and secret key, add these keys to the keystore by running the following commands:

```sh
bin/elasticsearch-keystore add discovery.ec2.access_key
bin/elasticsearch-keystore add discovery.ec2.secret_key
```

All **secure** settings of this plugin are reloadable, allowing you to update the secure settings for this plugin without needing to restart each node. For more information about secure and reloadable settings, go to [Secure your settings](docs-content://deploy-manage/security/secure-settings.md).

The available settings for the EC2 discovery plugin are as follows.

`discovery.ec2.access_key` (Secure, reloadable)
:   An EC2 access key. If set, you must also set `discovery.ec2.secret_key`. If unset, `discovery-ec2` will instead use the instance role. This setting is sensitive and must be stored in the {{es}} keystore.

`discovery.ec2.secret_key` (Secure, reloadable)
:   An EC2 secret key. If set, you must also set `discovery.ec2.access_key`. This setting is sensitive and must be stored in the {{es}} keystore.

`discovery.ec2.session_token` (Secure, reloadable)
:   An EC2 session token. If set, you must also set `discovery.ec2.access_key` and `discovery.ec2.secret_key`. This setting is sensitive and must be stored in the {{es}} keystore.

`discovery.ec2.endpoint`
:   The EC2 service endpoint to which to connect. See [https://docs.aws.amazon.com/general/latest/gr/rande.html#ec2_region](https://docs.aws.amazon.com/general/latest/gr/rande.html#ec2_region) to find the appropriate endpoint for the region. This setting defaults to `ec2.us-east-1.amazonaws.com` which is appropriate for clusters running in the `us-east-1` region.

`discovery.ec2.proxy.host`
:   The address or host name of an HTTP proxy through which to connect to EC2. If not set, no proxy is used.

`discovery.ec2.proxy.port`
:   When the address of an HTTP proxy is given in `discovery.ec2.proxy.host`, this setting determines the port to use to connect to the proxy. Defaults to `80`.

`discovery.ec2.proxy.scheme`
:   The scheme to use when connecting to the EC2 service endpoint through proxy specified in `discovery.ec2.proxy.host`. Valid values are `http` or `https`. Defaults to `http`.

`discovery.ec2.proxy.username` (Secure, reloadable)
:   When the address of an HTTP proxy is given in `discovery.ec2.proxy.host`, this setting determines the username to use to connect to the proxy. When not set, no username is used. This setting is sensitive and must be stored in the {{es}} keystore.

`discovery.ec2.proxy.password` (Secure, reloadable)
:   When the address of an HTTP proxy is given in `discovery.ec2.proxy.host`, this setting determines the password to use to connect to the proxy. When not set, no password is used. This setting is sensitive and must be stored in the {{es}} keystore.

`discovery.ec2.read_timeout`
:   The socket timeout for connections to EC2, [including the units](/reference/elasticsearch/rest-apis/api-conventions.md#time-units). For example, a value of `60s` specifies a 60-second timeout. Defaults to 50 seconds.

`discovery.ec2.groups`
:   A list of the names or IDs of the security groups to use for discovery. The `discovery.ec2.any_group` setting determines the behaviour of this setting. Defaults to an empty list, meaning that security group membership is ignored by EC2 discovery.

`discovery.ec2.any_group`
:   Defaults to `true`, meaning that instances belonging to *any* of the security groups specified in `discovery.ec2.groups` will be used for discovery. If set to `false`, only instances that belong to *all* of the security groups specified in `discovery.ec2.groups` will be used for discovery.

`discovery.ec2.host_type`
:   Each EC2 instance has a number of different addresses that might be suitable for discovery. This setting allows you to select which of these addresses is used by the discovery process. It can be set to one of `private_ip`, `public_ip`, `private_dns`, `public_dns` or `tag:TAGNAME` where `TAGNAME` refers to a name of a tag. This setting defaults to `private_ip`.

If you set `discovery.ec2.host_type` to a value of the form `tag:TAGNAME` then the value of the tag `TAGNAME` attached to each instance will be used as that instance’s address for discovery. Instances which do not have this tag set will be ignored by the discovery process.

For example if you tag some EC2 instances with a tag named `elasticsearch-host-name` and set `host_type: tag:elasticsearch-host-name` then the `discovery-ec2` plugin will read each instance’s host name from the value of the `elasticsearch-host-name` tag. [Read more about EC2 Tags](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html).


`discovery.ec2.availability_zones`
:   A list of the names of the availability zones to use for discovery. The name of an availability zone is the [region code followed by a letter](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html), such as `us-east-1a`. Only instances placed in one of the given availability zones will be used for discovery.

$$$discovery-ec2-filtering$$$

`discovery.ec2.tag.TAGNAME`
:   A list of the values of a tag called `TAGNAME` to use for discovery. If set, only instances that are tagged with one of the given values will be used for discovery. For instance, the following settings will only use nodes with a `role` tag set to `master` and an `environment` tag set to either `dev` or `staging`.

```yaml
discovery.ec2.tag.role: master
discovery.ec2.tag.environment: dev,staging
```

::::{note}
The names of tags used for discovery may only contain ASCII letters, numbers, hyphens and underscores. In particular you cannot use tags whose name includes a colon.
::::



`discovery.ec2.node_cache_time`
:   Sets the length of time for which the collection of discovered instances is cached. {{es}} waits at least this long between requests for discovery information from the EC2 API. AWS may reject discovery requests if they are made too often, and this would cause discovery to fail. Defaults to `10s`.

## Recommended EC2 permissions [discovery-ec2-permissions]

The `discovery-ec2` plugin works by making a `DescribeInstances` call to the AWS EC2 API. You must configure your AWS account to allow this, which is normally done using an IAM policy. You can create a custom policy via the IAM Management Console. It should look similar to this.

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


## Automatic node attributes [discovery-ec2-attributes]

The `discovery-ec2` plugin can automatically set the `aws_availability_zone` node attribute to the availability zone of each node. This node attribute allows you to ensure that each shard has copies allocated redundantly across multiple availability zones by using the [Allocation Awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md) feature.

In order to enable the automatic definition of the `aws_availability_zone` attribute, set `cloud.node.auto_attributes` to `true`. For example:

```yaml
cloud.node.auto_attributes: true
cluster.routing.allocation.awareness.attributes: aws_availability_zone
```

The `aws_availability_zone` attribute can be automatically set like this when using any discovery type. It is not necessary to set `discovery.seed_providers: ec2`. However this feature does require that the `discovery-ec2` plugin is installed.


## Binding to the correct address [discovery-ec2-network-host]

It is important to define `network.host` correctly when deploying a cluster on EC2. By default each {{es}} node only binds to `localhost`, which will prevent it from being discovered by nodes running on any other instances.

You can use the [core network host settings](/reference/elasticsearch/configuration-reference/networking-settings.md) to bind each node to the desired address, or you can set `network.host` to one of the following EC2-specific settings provided by the `discovery-ec2` plugin:

| EC2 Host Value | Description |
| --- | --- |
| `_ec2:privateIpv4_` | The private IP address (ipv4) of the machine. |
| `_ec2:privateDns_` | The private host of the machine. |
| `_ec2:publicIpv4_` | The public IP address (ipv4) of the machine. |
| `_ec2:publicDns_` | The public host of the machine. |
| `_ec2:privateIp_` | Equivalent to `_ec2:privateIpv4_`. |
| `_ec2:publicIp_` | Equivalent to `_ec2:publicIpv4_`. |
| `_ec2_` | Equivalent to `_ec2:privateIpv4_`. |

These values are acceptable when using any discovery type. They do not require you to set `discovery.seed_providers: ec2`. However they do require that the `discovery-ec2` plugin is installed.
