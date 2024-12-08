/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.action.admin.indices.stats.IndexStats.IndexStatsBuilder;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.ChunkedBroadcastResponse;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

public class IndicesStatsResponse extends ChunkedBroadcastResponse {

    private final Map<String, ClusterHealthStatus> indexHealthMap;

    private final Map<String, IndexMetadata.State> indexStateMap;

    private final ShardStats[] shards;

    private Map<ShardRouting, ShardStats> shardStatsMap;

    IndicesStatsResponse(StreamInput in) throws IOException {
        super(in);
        shards = in.readArray(ShardStats::new, ShardStats[]::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.INDEX_STATS_ADDITIONAL_FIELDS_REVERT)) {
            indexHealthMap = in.readMap(ClusterHealthStatus::readFrom);
            indexStateMap = in.readMap(IndexMetadata.State::readFrom);
        } else if (in.getTransportVersion().onOrAfter(TransportVersions.INDEX_STATS_ADDITIONAL_FIELDS)) {
            indexHealthMap = in.readMap(ClusterHealthStatus::readFrom);
            indexStateMap = in.readMap(IndexMetadata.State::readFrom);
            in.readMap(StreamInput::readStringCollectionAsList); // unused, reverted
            in.readMap(StreamInput::readLong); // unused, reverted
        } else if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0)) {
            // Between 8.1 and INDEX_STATS_ADDITIONAL_FIELDS, we had a different format for the response
            // where we only had health and state available.
            indexHealthMap = in.readMap(ClusterHealthStatus::readFrom);
            indexStateMap = in.readMap(IndexMetadata.State::readFrom);
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
        Metadata metadata,
        RoutingTable routingTable
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.shards = shards;
        Objects.requireNonNull(metadata);
        Objects.requireNonNull(routingTable);
        Objects.requireNonNull(shards);
        Map<String, ClusterHealthStatus> indexHealthModifiableMap = new HashMap<>();
        Map<String, IndexMetadata.State> indexStateModifiableMap = new HashMap<>();
        for (ShardStats shard : shards) {
            Index index = shard.getShardRouting().index();
            IndexMetadata indexMetadata = metadata.index(index);
            if (indexMetadata != null) {
                indexHealthModifiableMap.computeIfAbsent(
                    index.getName(),
                    ignored -> new ClusterIndexHealth(indexMetadata, routingTable.index(index)).getStatus()
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.INDEX_STATS_ADDITIONAL_FIELDS_REVERT)) {
            out.writeMap(indexHealthMap, StreamOutput::writeWriteable);
            out.writeMap(indexStateMap, StreamOutput::writeWriteable);
        } else if (out.getTransportVersion().onOrAfter(TransportVersions.INDEX_STATS_ADDITIONAL_FIELDS)) {
            out.writeMap(indexHealthMap, StreamOutput::writeWriteable);
            out.writeMap(indexStateMap, StreamOutput::writeWriteable);
            out.writeMap(Map.of(), StreamOutput::writeStringCollection);
            out.writeMap(Map.of(), StreamOutput::writeLong);
        } else if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0)) {
            out.writeMap(indexHealthMap, StreamOutput::writeWriteable);
            out.writeMap(indexStateMap, StreamOutput::writeWriteable);
        }
    }

    @Override
    protected Iterator<ToXContent> customXContentChunks(ToXContent.Params params) {
        final ClusterStatsLevel level = ClusterStatsLevel.of(params, ClusterStatsLevel.INDICES);
        if (level == ClusterStatsLevel.INDICES || level == ClusterStatsLevel.SHARDS) {
            return Iterators.concat(

                ChunkedToXContentHelper.singleChunk((builder, p) -> {
                    commonStats(builder, p);
                    return builder.startObject(Fields.INDICES);
                }),
                Iterators.flatMap(
                    getIndices().values().iterator(),
                    indexStats -> Iterators.concat(

                        ChunkedToXContentHelper.singleChunk((builder, p) -> {
                            builder.startObject(indexStats.getIndex());
                            builder.field("uuid", indexStats.getUuid());
                            if (indexStats.getHealth() != null) {
                                builder.field("health", indexStats.getHealth().toString().toLowerCase(Locale.ROOT));
                            }
                            if (indexStats.getState() != null) {
                                builder.field("status", indexStats.getState().toString().toLowerCase(Locale.ROOT));
                            }
                            builder.startObject("primaries");
                            indexStats.getPrimaries().toXContent(builder, p);
                            builder.endObject();

                            builder.startObject("total");
                            indexStats.getTotal().toXContent(builder, p);
                            builder.endObject();
                            return builder;
                        }),

                        level == ClusterStatsLevel.SHARDS
                            ? Iterators.concat(
                                ChunkedToXContentHelper.startObject(Fields.SHARDS),
                                Iterators.flatMap(
                                    indexStats.iterator(),
                                    indexShardStats -> Iterators.concat(
                                        ChunkedToXContentHelper.startArray(Integer.toString(indexShardStats.getShardId().id())),
                                        Iterators.<ShardStats, ToXContent>map(indexShardStats.iterator(), shardStats -> (builder, p) -> {
                                            builder.startObject();
                                            shardStats.toXContent(builder, p);
                                            builder.endObject();
                                            return builder;
                                        }),
                                        ChunkedToXContentHelper.endArray()
                                    )
                                ),
                                ChunkedToXContentHelper.endObject()
                            )
                            : Collections.emptyIterator(),

                        ChunkedToXContentHelper.endObject()
                    )
                ),
                ChunkedToXContentHelper.endObject()
            );
        } else {
            return ChunkedToXContentHelper.singleChunk((builder, p) -> {
                commonStats(builder, p);
                return builder;
            });
        }
    }

    private void commonStats(XContentBuilder builder, ToXContent.Params p) throws IOException {
        builder.startObject("_all");

        builder.startObject("primaries");
        getPrimaries().toXContent(builder, p);
        builder.endObject();

        builder.startObject("total");
        getTotal().toXContent(builder, p);
        builder.endObject();

        builder.endObject();
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
