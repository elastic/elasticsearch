/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
