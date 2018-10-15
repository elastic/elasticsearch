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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

public class IndicesStatsResponse extends BroadcastResponse {

    private ShardStats[] shards;

    private Map<ShardRouting, ShardStats> shardStatsMap;

    private MetaData metaData;
    private RoutingTable routingTable;

    IndicesStatsResponse() {

    }

    IndicesStatsResponse(ShardStats[] shards, int totalShards, int successfulShards, int failedShards,
                         List<DefaultShardOperationFailedException> shardFailures) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
    }

    IndicesStatsResponse(ShardStats[] shards, int totalShards, int successfulShards, int failedShards,
                         List<DefaultShardOperationFailedException> shardFailures, ClusterState clusterState) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
        this.metaData = clusterState.metaData();
        this.routingTable = clusterState.routingTable();
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
        Map<String, IndexStats> indicesStats = new HashMap<>();

        Set<Index> indices = new HashSet<>();
        for (ShardStats shard : shards) {
            indices.add(shard.getShardRouting().index());
        }

        for (Index index : indices) {
            List<ShardStats> shards = new ArrayList<>();
            String indexName = index.getName();
            for (ShardStats shard : this.shards) {
                if (shard.getShardRouting().getIndexName().equals(indexName)) {
                    shards.add(shard);
                }
            }
            indicesStats.put(
                indexName, new IndexStats(indexName, index.getUUID(), shards.toArray(new ShardStats[shards.size()]))
            );
        }
        this.indicesStats = indicesStats;
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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shards = in.readArray(ShardStats::readShardStats, (size) -> new ShardStats[size]);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray(shards);
    }

    @Override
    protected void addCustomXContentFields(XContentBuilder builder, Params params) throws IOException {
        final String level = params.param("level", "indices");
        final boolean isLevelValid =
            "cluster".equalsIgnoreCase(level) || "indices".equalsIgnoreCase(level) || "shards".equalsIgnoreCase(level);
        if (!isLevelValid) {
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
            if (getIndices().values().isEmpty() == false) {
                for (IndexStats indexStats : getIndices().values()) {
                    builder.startObject(indexStats.getIndex());
                    builder.field("uuid", indexStats.getUuid());
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
            } else if (metaData != null && routingTable != null) {
                for (IndexRoutingTable indexRoutingTable : routingTable) {
                    final Index index = indexRoutingTable.getIndex();
                    if (params.param("index") == null || Objects.equals(params.param("index"), index.getName())) {
                        final IndexMetaData indexMetaData = metaData.index(index);
                        final ClusterIndexHealth health = new ClusterIndexHealth(indexMetaData, indexRoutingTable);

                        final int totalNumberOfShards = indexMetaData.getTotalNumberOfShards();
                        final int numberOfShards = metaData.getNumberOfShards();

                        builder.startObject(index.getName());
                        builder.field("uuid", index.getUUID());
                        builder.field("primaries", numberOfShards);
                        builder.field("total", totalNumberOfShards);

                        if ("shards".equalsIgnoreCase(level)) {
                            final int activeTotal = health.getActiveShards();
                            final int activePrimaries = health.getActivePrimaryShards();
                            final int unassignedTotal = health.getUnassignedShards() + health.getInitializingShards();
                            final int unassignedPrimaries = numberOfShards - health.getActivePrimaryShards();

                            builder.startObject("shards");
                            builder.field("replicas", metaData.index(index).getNumberOfReplicas());
                            builder.field("active_total", activeTotal);
                            builder.field("active_primaries", activePrimaries);
                            builder.field("active_replicas", activeTotal - activePrimaries);
                            builder.field("unassigned_total", unassignedTotal);
                            builder.field("unassigned_primaries", unassignedPrimaries);
                            builder.field("unassigned_replicas", unassignedTotal - unassignedPrimaries);
                            builder.field("initializing", health.getInitializingShards());
                            builder.field("relocating", health.getRelocatingShards());
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                }
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
