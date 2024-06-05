/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class IndexShardStats implements Iterable<ShardStats>, Writeable {

    private final ShardId shardId;

    private final ShardStats[] shards;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexShardStats that = (IndexShardStats) o;
        return shardId.equals(that.shardId) && Arrays.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, Arrays.hashCode(shards));
    }

    public IndexShardStats(StreamInput in) throws IOException {
        shardId = new ShardId(in);
        shards = in.readArray(ShardStats::new, ShardStats[]::new);
    }

    public IndexShardStats(ShardId shardId, ShardStats[] shards) {
        this.shardId = shardId;
        this.shards = shards;
    }

    public ShardId getShardId() {
        return this.shardId;
    }

    public ShardStats[] getShards() {
        return shards;
    }

    public ShardStats getAt(int position) {
        return shards[position];
    }

    @Override
    public Iterator<ShardStats> iterator() {
        return Iterators.forArray(shards);
    }

    private CommonStats total = null;

    public CommonStats getTotal() {
        if (total != null) {
            return total;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            stats.add(shard.getStats());
        }
        total = stats;
        return stats;
    }

    private CommonStats primary = null;

    public CommonStats getPrimary() {
        if (primary != null) {
            return primary;
        }
        CommonStats stats = new CommonStats();
        for (ShardStats shard : shards) {
            if (shard.getShardRouting().primary()) {
                stats.add(shard.getStats());
            }
        }
        primary = stats;
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        out.writeArray(shards);
    }
}
