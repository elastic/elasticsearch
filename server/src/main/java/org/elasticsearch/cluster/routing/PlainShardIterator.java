/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.shard.ShardId;

import java.util.List;

/**
 * The {@link PlainShardIterator} is a {@link ShardsIterator} which iterates all
 * shards or a given {@link ShardId shard id}
 */
public class PlainShardIterator extends PlainShardsIterator implements ShardIterator {

    private final ShardId shardId;

    /**
     * Creates a {@link PlainShardIterator} instance that iterates over a subset of the given shards
     * this the a given <code>shardId</code>.
     *
     * @param shardId shard id of the group
     * @param shards  shards to iterate
     */
    public PlainShardIterator(ShardId shardId, List<ShardRouting> shards) {
        super(shards);
        this.shardId = shardId;
    }

    @Override
    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardIterator that = (ShardIterator) o;
        return shardId.equals(that.shardId());
    }

    @Override
    public int hashCode() {
        return shardId.hashCode();
    }

    @Override
    public int compareTo(ShardIterator o) {
        return shardId.compareTo(o.shardId());
    }
}
