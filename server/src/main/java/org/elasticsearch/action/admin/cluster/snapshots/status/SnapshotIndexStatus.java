/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * Represents snapshot status of all shards in the index
 */
public class SnapshotIndexStatus implements Iterable<SnapshotIndexShardStatus>, ToXContentFragment {

    private final String index;

    private final Map<Integer, SnapshotIndexShardStatus> indexShards;

    private final SnapshotShardsStats shardsStats;

    private final SnapshotStats stats;

    SnapshotIndexStatus(String index, Collection<SnapshotIndexShardStatus> shards) {
        this.index = index;

        Map<Integer, SnapshotIndexShardStatus> indexShards = new HashMap<>();
        stats = new SnapshotStats();
        for (SnapshotIndexShardStatus shard : shards) {
            indexShards.put(shard.getShardId().getId(), shard);
            stats.add(shard.getStats(), true);
        }
        shardsStats = new SnapshotShardsStats(shards);
        this.indexShards = unmodifiableMap(indexShards);
    }

    public SnapshotIndexStatus(
        String index,
        Map<Integer, SnapshotIndexShardStatus> indexShards,
        SnapshotShardsStats shardsStats,
        SnapshotStats stats
    ) {
        this.index = index;
        this.indexShards = indexShards;
        this.shardsStats = shardsStats;
        this.stats = stats;
    }

    /**
     * Returns the index name
     */
    public String getIndex() {
        return this.index;
    }

    /**
     * A shard id to index snapshot shard status map
     */
    public Map<Integer, SnapshotIndexShardStatus> getShards() {
        return this.indexShards;
    }

    /**
     * Shards stats
     */
    public SnapshotShardsStats getShardsStats() {
        return shardsStats;
    }

    /**
     * Returns snapshot stats
     */
    public SnapshotStats getStats() {
        return stats;
    }

    @Override
    public Iterator<SnapshotIndexShardStatus> iterator() {
        return indexShards.values().iterator();
    }

    static final class Fields {
        static final String SHARDS = "shards";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getIndex());
        builder.field(SnapshotShardsStats.Fields.SHARDS_STATS, shardsStats, params);
        builder.field(SnapshotStats.Fields.STATS, stats, params);
        builder.startObject(Fields.SHARDS);
        for (SnapshotIndexShardStatus shard : indexShards.values()) {
            shard.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SnapshotIndexStatus that = (SnapshotIndexStatus) o;
        return Objects.equals(index, that.index)
            && Objects.equals(indexShards, that.indexShards)
            && Objects.equals(shardsStats, that.shardsStats)
            && Objects.equals(stats, that.stats);
    }

    @Override
    public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + (indexShards != null ? indexShards.hashCode() : 0);
        result = 31 * result + (shardsStats != null ? shardsStats.hashCode() : 0);
        result = 31 * result + (stats != null ? stats.hashCode() : 0);
        return result;
    }
}
