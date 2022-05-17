package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.index.shard.ShardId;

import java.util.SortedSet;

record ShardsSize(long sizeInBytes, SortedSet<ShardId> shardIds) {}
