/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.features.NodeFeature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class IndexStats implements Iterable<IndexShardStats> {

    // feature was effectively reverted but we still need to keep this constant around
    public static final NodeFeature REVERTED_TIER_CREATION_DATE = new NodeFeature("stats.tier_creation_date");

    private final String index;

    private final String uuid;

    private final ClusterHealthStatus health;

    private final IndexMetadata.State state;

    private final ShardStats shards[];

    public IndexStats(
        String index,
        String uuid,
        @Nullable ClusterHealthStatus health,
        @Nullable IndexMetadata.State state,
        ShardStats[] shards
    ) {
        this.index = index;
        this.uuid = uuid;
        this.health = health;
        this.state = state;
        this.shards = shards;
    }

    public String getIndex() {
        return this.index;
    }

    public String getUuid() {
        return uuid;
    }

    public ClusterHealthStatus getHealth() {
        return health;
    }

    public IndexMetadata.State getState() {
        return state;
    }

    public ShardStats[] getShards() {
        return this.shards;
    }

    private Map<Integer, IndexShardStats> indexShards;

    public Map<Integer, IndexShardStats> getIndexShards() {
        if (indexShards != null) {
            return indexShards;
        }
        Map<Integer, List<ShardStats>> tmpIndexShards = new HashMap<>();
        for (ShardStats shard : shards) {
            List<ShardStats> lst = tmpIndexShards.computeIfAbsent(shard.getShardRouting().id(), ignored -> new ArrayList<>());
            lst.add(shard);
        }
        indexShards = new HashMap<>();
        for (Map.Entry<Integer, List<ShardStats>> entry : tmpIndexShards.entrySet()) {
            indexShards.put(
                entry.getKey(),
                new IndexShardStats(
                    entry.getValue().get(0).getShardRouting().shardId(),
                    entry.getValue().toArray(new ShardStats[entry.getValue().size()])
                )
            );
        }
        return indexShards;
    }

    @Override
    public Iterator<IndexShardStats> iterator() {
        return getIndexShards().values().iterator();
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

    public CommonStats getPrimaries() {
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

    public static class IndexStatsBuilder {
        private final String indexName;
        private final String uuid;
        private final ClusterHealthStatus health;
        private final IndexMetadata.State state;
        private final List<ShardStats> shards = new ArrayList<>();

        public IndexStatsBuilder(String indexName, String uuid, @Nullable ClusterHealthStatus health, @Nullable IndexMetadata.State state) {
            this.indexName = indexName;
            this.uuid = uuid;
            this.health = health;
            this.state = state;
        }

        public IndexStatsBuilder add(ShardStats shardStats) {
            shards.add(shardStats);
            return this;
        }

        public IndexStats build() {
            return new IndexStats(indexName, uuid, health, state, shards.toArray(new ShardStats[shards.size()]));
        }
    }
}
