/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link PlainShardIterator} iterates shard copies for a given {@link ShardId shard id}
 */
public class PlainShardIterator extends PlainShardsIterator implements Comparable<PlainShardIterator> {

    private final ShardId shardId;

    public static PlainShardIterator allSearchableShards(PlainShardIterator shardIterator) {
        return new PlainShardIterator(shardIterator.shardId(), shardsThatCanHandleSearches(shardIterator));
    }

    private static List<ShardRouting> shardsThatCanHandleSearches(PlainShardIterator iterator) {
        final List<ShardRouting> shardsThatCanHandleSearches = new ArrayList<>(iterator.size());
        for (ShardRouting shardRouting : iterator) {
            if (shardRouting.isSearchable()) {
                shardsThatCanHandleSearches.add(shardRouting);
            }
        }
        return shardsThatCanHandleSearches;
    }

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

    public ShardId shardId() {
        return this.shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PlainShardIterator that = (PlainShardIterator) o;
        return shardId.equals(that.shardId());
    }

    @Override
    public int hashCode() {
        return shardId.hashCode();
    }

    @Override
    public int compareTo(PlainShardIterator o) {
        return shardId.compareTo(o.shardId());
    }
}
