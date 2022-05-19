/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.stats.IndexStats.IndexStatsBuilder;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

public class IndicesStatsResponse extends BroadcastResponse {

    private final Map<String, ClusterHealthStatus> indexHealthMap;

    private final Map<String, IndexMetadata.State> indexStateMap;

    private final ShardStats[] shards;

    private Map<ShardRouting, ShardStats> shardStatsMap;

    IndicesStatsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(ShardStats::new, ShardStats[]::new);
        if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
            indexHealthMap = in.readMap(StreamInput::readString, ClusterHealthStatus::readFrom);
            indexStateMap = in.readMap(StreamInput::readString, IndexMetadata.State::readFrom);
        } else {
            indexHealthMap = Map.of();
            indexStateMap = Map.of();
        }
    }

    IndicesStatsResponse(
        ShardStats[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
        Objects.requireNonNull(clusterState);
        Objects.requireNonNull(shards);
        Map<String, ClusterHealthStatus> indexHealthModifiableMap = new HashMap<>();
        Map<String, IndexMetadata.State> indexStateModifiableMap = new HashMap<>();
        for (ShardStats shard : shards) {
            Index index = shard.getShardRouting().index();
            IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
            if (indexMetadata != null) {
                indexHealthModifiableMap.computeIfAbsent(
                    index.getName(),
                    ignored -> new ClusterIndexHealth(indexMetadata, clusterState.routingTable().index(index)).getStatus()
                );
                indexStateModifiableMap.computeIfAbsent(index.getName(), ignored -> indexMetadata.getState());
            }
        }
        indexHealthMap = unmodifiableMap(indexHealthModifiableMap);
        indexStateMap = unmodifiableMap(indexStateModifiableMap);
    }

    public Map<ShardRouting, ShardStats> asMap() {
        if (this.shardStatsMap == null) {
            Map<ShardRouting, ShardStats> shardStatsMap = new HashMap<>();
            for (ShardStats ss : shards) {
                shardStatsMap.put(ss.getShardRouting(), ss);
            }
            this.shardStatsMap = unmodifiableMap(shardStatsMap);
        }
        return this.shardStatsMap;
    }

    public ShardStats[] getShards() {
        return this.shards;
    }

    public ShardStats getAt(int position) {
        return shards[position];
    }

    public IndexStats getIndex(String index) {
        return getIndices().get(index);
    }

    private Map<String, IndexStats> indicesStats;

    public Map<String, IndexStats> getIndices() {
        if (indicesStats != null) {
            return indicesStats;
        }

        final Map<String, IndexStatsBuilder> indexToIndexStatsBuilder = new HashMap<>();
        for (ShardStats shard : shards) {
            Index index = shard.getShardRouting().index();
            IndexStatsBuilder indexStatsBuilder = indexToIndexStatsBuilder.computeIfAbsent(
                index.getName(),
                k -> new IndexStatsBuilder(k, index.getUUID(), indexHealthMap.get(index.getName()), indexStateMap.get(index.getName()))
            );
            indexStatsBuilder.add(shard);
        }

        indicesStats = indexToIndexStatsBuilder.entrySet()
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build()));
        return indicesStats;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            out.writeMap(indexHealthMap, StreamOutput::writeString, (o, s) -> s.writeTo(o));
            out.writeMap(indexStateMap, StreamOutput::writeString, (o, s) -> s.writeTo(o));
        }
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        final String level = params.param("level", "indices");
        final boolean isLevelValid = "cluster".equalsIgnoreCase(level)
            || "indices".equalsIgnoreCase(level)
            || "shards".equalsIgnoreCase(level);
        if (isLevelValid == false) {
            throw new IllegalArgumentException("level parameter must be one of [cluster] or [indices] or [shards] but was [" + level + "]");
        }

        builder.startObject("_all");

        builder.startObject("primaries");
        getPrimaries().toXContent(builder, params);
        builder.endObject();

        builder.startObject("total");
        getTotal().toXContent(builder, params);
        builder.endObject();

        builder.endObject();

        if ("indices".equalsIgnoreCase(level) || "shards".equalsIgnoreCase(level)) {
            builder.startObject(Fields.INDICES);
            for (IndexStats indexStats : getIndices().values()) {
                builder.startObject(indexStats.getIndex());
                builder.field("uuid", indexStats.getUuid());
                if (indexStats.getHealth() != null) {
                    builder.field("health", indexStats.getHealth().toString().toLowerCase(Locale.ROOT));
                }
                if (indexStats.getState() != null) {
                    builder.field("status", indexStats.getState().toString().toLowerCase(Locale.ROOT));
                }
                builder.startObject("primaries");
                indexStats.getPrimaries().toXContent(builder, params);
                builder.endObject();

                builder.startObject("total");
                indexStats.getTotal().toXContent(builder, params);
                builder.endObject();

                if ("shards".equalsIgnoreCase(level)) {
                    builder.startObject(Fields.SHARDS);
                    for (IndexShardStats indexShardStats : indexStats) {
                        builder.startArray(Integer.toString(indexShardStats.getShardId().id()));
                        for (ShardStats shardStats : indexShardStats) {
                            builder.startObject();
                            shardStats.toXContent(builder, params);
                            builder.endObject();
                        }
                        builder.endArray();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    static final class Fields {
        static final String INDICES = "indices";
        static final String SHARDS = "shards";
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, false);
    }
}
