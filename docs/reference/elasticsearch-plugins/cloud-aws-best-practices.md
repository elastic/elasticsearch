---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/plugins/current/cloud-aws-best-practices.html
---

# Best Practices in AWS [cloud-aws-best-practices]

This section contains some other information about designing and managing an {{es}} cluster on your own AWS infrastructure. If you would prefer to avoid these operational details then you may be interested in a hosted {{es}} installation available on AWS-based infrastructure from [https://www.elastic.co/cloud](https://www.elastic.co/cloud).

## Storage [_storage]

EC2 instances offer a number of different kinds of storage. Please be aware of the following when selecting the storage for your cluster:

* [Instance Store](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/InstanceStorage.html) is recommended for {{es}} clusters as it offers excellent performance and is cheaper than EBS-based storage. {{es}} is designed to work well with this kind of ephemeral storage because it replicates each shard across multiple nodes. If a node fails and its Instance Store is lost then {{es}} will rebuild any lost shards from other copies.
* [EBS-based storage](https://aws.amazon.com/ebs/) may be acceptable for smaller clusters (1-2 nodes). Be sure to use provisioned IOPS to ensure your cluster has satisfactory performance.
* [EFS-based storage](https://aws.amazon.com/efs/) is not recommended or supported as it does not offer satisfactory performance. Historically, shared network filesystems such as EFS have not always offered precisely the behaviour that {{es}} requires of its filesystem, and this has been known to lead to index corruption. Although EFS offers durability, shared storage, and the ability to grow and shrink filesystems dynamically, you can achieve the same benefits using {{es}} directly.


## Choice of AMI [_choice_of_ami]

Prefer the [Amazon Linux 2 AMIs](https://aws.amazon.com/amazon-linux-2/) as these allow you to benefit from the lightweight nature, support, and EC2-specific performance enhancements that these images offer.


## Networking [_networking]

* Smaller instance types have limited network performance, in terms of both [bandwidth and number of connections](https://lab.getbase.com/how-we-discovered-limitations-on-the-aws-tcp-stack/). If networking is a bottleneck, avoid [instance types](https://aws.amazon.com/ec2/instance-types/) with networking labelled as `Moderate` or `Low`.
* It is a good idea to distribute your nodes across multiple [availability zones](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html) and use [shard allocation awareness](docs-content://deploy-manage/distributed-architecture/shard-allocation-relocation-recovery/shard-allocation-awareness.md) to ensure that each shard has copies in more than one availability zone.
* Do not span a cluster across regions. {{es}} expects that node-to-node connections within a cluster are reasonably reliable and offer high bandwidth and low latency, and these properties do not hold for connections between regions. Although an {{es}} cluster will behave correctly when node-to-node connections are unreliable or slow, it is not optimised for this case and its performance may suffer. If you wish to geographically distribute your data, you should provision multiple clusters and use features such as [cross-cluster search](docs-content://solutions/search/cross-cluster-search.md) and [cross-cluster replication](docs-content://deploy-manage/tools/cross-cluster-replication.md).


## Other recommendations [_other_recommendations]

* If you have split your nodes into roles, consider [tagging the EC2 instances](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/Using_Tags.html) by role to make it easier to filter and view your EC2 instances in the AWS console.
* Consider [enabling termination protection](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/terminating-instances.html#Using_ChangingDisableAPITermination) for all of your data and master-eligible nodes. This will help to prevent accidental termination of these nodes which could temporarily reduce the resilience of the cluster and which could cause a potentially disruptive reallocation of shards.
* If running your cluster using one or more [auto-scaling groups](https://docs.aws.amazon.com/autoscaling/ec2/userguide/AutoScalingGroup.html), consider protecting your data and master-eligible nodes [against termination during scale-in](https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-instance-termination.html#instance-protection-instance). This will help to prevent automatic termination of these nodes which could temporarily reduce the resilience of the cluster and which could cause a potentially disruptive reallocation of shards. If these instances are protected against termination during scale-in then you can use shard allocation filtering to gracefully migrate any data off these nodes before terminating them manually. Refer to [](/reference/elasticsearch/index-settings/shard-allocation.md).
