/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.ClusterBalanceStats;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceStats;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.singleChunk;

public class DesiredBalanceResponse extends ActionResponse implements ChunkedToXContentObject {

    private static final TransportVersion CLUSTER_BALANCE_STATS_VERSION = TransportVersion.V_8_7_0;
    private static final TransportVersion CLUSTER_INFO_VERSION = TransportVersion.V_8_8_0;

    private final DesiredBalanceStats stats;
    private final ClusterBalanceStats clusterBalanceStats;
    private final Map<String, Map<Integer, DesiredShards>> routingTable;
    private final ClusterInfo clusterInfo;

    public DesiredBalanceResponse(
        DesiredBalanceStats stats,
        ClusterBalanceStats clusterBalanceStats,
        Map<String, Map<Integer, DesiredShards>> routingTable,
        ClusterInfo clusterInfo
    ) {
        this.stats = stats;
        this.clusterBalanceStats = clusterBalanceStats;
        this.routingTable = routingTable;
        this.clusterInfo = clusterInfo;
    }

    public static DesiredBalanceResponse from(StreamInput in) throws IOException {
        return new DesiredBalanceResponse(
            DesiredBalanceStats.readFrom(in),
            in.getTransportVersion().onOrAfter(CLUSTER_BALANCE_STATS_VERSION)
                ? ClusterBalanceStats.readFrom(in)
                : ClusterBalanceStats.EMPTY,
            in.readImmutableMap(v -> v.readImmutableMap(StreamInput::readVInt, DesiredShards::from)),
            in.getTransportVersion().onOrAfter(CLUSTER_INFO_VERSION) ? new ClusterInfo(in) : ClusterInfo.EMPTY
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
        if (out.getTransportVersion().onOrAfter(CLUSTER_BALANCE_STATS_VERSION)) {
            out.writeWriteable(clusterBalanceStats);
        }
        out.writeMap(
            routingTable,
            StreamOutput::writeString,
            (shardsOut, shards) -> shardsOut.writeMap(
                shards,
                StreamOutput::writeVInt,
                (desiredShardsOut, desiredShards) -> desiredShards.writeTo(desiredShardsOut)
            )
        );
        if (out.getTransportVersion().onOrAfter(CLUSTER_INFO_VERSION)) {
            out.writeWriteable(clusterInfo);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            singleChunk(
                (builder, p) -> builder.startObject(),
                (builder, p) -> builder.field("stats", stats),
                (builder, p) -> builder.field("cluster_balance_stats", clusterBalanceStats),
                (builder, p) -> builder.startObject("routing_table")
            ),
            routingTableToXContentChunked(),
            singleChunk(
                (builder, p) -> builder.endObject(),
                (builder, p) -> builder.startObject("cluster_info").value(clusterInfo).endObject(),
                (builder, p) -> builder.endObject()
            )
        );
    }

    private Iterator<ToXContent> routingTableToXContentChunked() {
        return routingTable.entrySet().stream().map(indexEntry -> (ToXContent) (builder, p) -> {
            builder.startObject(indexEntry.getKey());
            for (Map.Entry<Integer, DesiredShards> shardEntry : indexEntry.getValue().entrySet()) {
                builder.field(String.valueOf(shardEntry.getKey()));
                shardEntry.getValue().toXContent(builder, p);
            }
            return builder.endObject();
        }).iterator();
    }

    public DesiredBalanceStats getStats() {
        return stats;
    }

    public ClusterBalanceStats getClusterBalanceStats() {
        return clusterBalanceStats;
    }

    public Map<String, Map<Integer, DesiredShards>> getRoutingTable() {
        return routingTable;
    }

    public ClusterInfo getClusterInfo() {
        return clusterInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof DesiredBalanceResponse that
            && Objects.equals(stats, that.stats)
            && Objects.equals(clusterBalanceStats, that.clusterBalanceStats)
            && Objects.equals(routingTable, that.routingTable)
            && Objects.equals(clusterInfo, that.clusterInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats, clusterBalanceStats, routingTable, clusterInfo);
    }

    @Override
    public String toString() {
        return "DesiredBalanceResponse{stats="
            + stats
            + ", clusterBalanceStats="
            + clusterBalanceStats
            + ", routingTable="
            + routingTable
            + ", clusterInfo="
            + clusterInfo
            + "}";
    }

    public record DesiredShards(List<ShardView> current, ShardAssignmentView desired) implements Writeable, ToXContentObject {

        public static DesiredShards from(StreamInput in) throws IOException {
            return new DesiredShards(in.readList(ShardView::from), ShardAssignmentView.from(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeList(current);
            desired.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.startArray("current");
            for (ShardView shardView : current) {
                shardView.toXContent(builder, params);
            }
            builder.endArray();
            desired.toXContent(builder.field("desired"), params);
            return builder.endObject();
        }

    }

    public record ShardView(
        ShardRoutingState state,
        boolean primary,
        String node,
        boolean nodeIsDesired,
        @Nullable String relocatingNode,
        @Nullable Boolean relocatingNodeIsDesired,
        int shardId,
        String index,
        @Nullable Double forecastWriteLoad,
        @Nullable Long forecastShardSizeInBytes,
        List<String> tierPreference
    ) implements Writeable, ToXContentObject {

        private static final TransportVersion ADD_FORECASTS_VERSION = TransportVersion.V_8_7_0;
        private static final TransportVersion ADD_TIER_PREFERENCE = TransportVersion.V_8_8_0;
        private static final TransportVersion NULLABLE_RELOCATING_NODE_IS_DESIRED = TransportVersion.V_8_8_0;

        public ShardView {
            assert (relocatingNode == null) == (relocatingNodeIsDesired == null)
                : "relocatingNodeIsDesired should only be set when relocatingNode is set";
        }

        public static ShardView from(StreamInput in) throws IOException {
            ShardRoutingState state = ShardRoutingState.fromValue(in.readByte());
            boolean primary = in.readBoolean();
            String node = in.readOptionalString();
            boolean nodeIsDesired = in.readBoolean();
            String relocatingNode = in.readOptionalString();
            Boolean relocatingNodeIsDesired;
            if (in.getTransportVersion().onOrAfter(NULLABLE_RELOCATING_NODE_IS_DESIRED)) {
                relocatingNodeIsDesired = in.readOptionalBoolean();
            } else {
                boolean wireRelocatingNodeIsDesired = in.readBoolean();
                relocatingNodeIsDesired = relocatingNode == null ? null : wireRelocatingNodeIsDesired;
            }
            int shardId = in.readVInt();
            String index = in.readString();
            Double forecastWriteLoad = in.getTransportVersion().onOrAfter(ADD_FORECASTS_VERSION) ? in.readOptionalDouble() : null;
            Long forecastShardSizeInBytes = in.getTransportVersion().onOrAfter(ADD_FORECASTS_VERSION) ? in.readOptionalLong() : null;
            if (in.getTransportVersion().onOrAfter(ADD_FORECASTS_VERSION) == false) {
                in.readOptionalWriteable(AllocationId::new);
            }
            List<String> tierPreference = in.getTransportVersion().onOrAfter(ADD_TIER_PREFERENCE) ? in.readStringList() : List.of();
            return new ShardView(
                state,
                primary,
                node,
                nodeIsDesired,
                relocatingNode,
                relocatingNodeIsDesired,
                shardId,
                index,
                forecastWriteLoad,
                forecastShardSizeInBytes,
                tierPreference
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(state.value());
            out.writeBoolean(primary);
            out.writeOptionalString(node);
            out.writeBoolean(nodeIsDesired);
            out.writeOptionalString(relocatingNode);
            if (out.getTransportVersion().onOrAfter(NULLABLE_RELOCATING_NODE_IS_DESIRED)) {
                out.writeOptionalBoolean(relocatingNodeIsDesired);
            } else {
                out.writeBoolean(relocatingNodeIsDesired != null && relocatingNodeIsDesired);
            }
            out.writeVInt(shardId);
            out.writeString(index);
            if (out.getTransportVersion().onOrAfter(ADD_FORECASTS_VERSION)) {
                out.writeOptionalDouble(forecastWriteLoad);
                out.writeOptionalLong(forecastShardSizeInBytes);
            } else {
                out.writeMissingWriteable(AllocationId.class);
            }
            if (out.getTransportVersion().onOrAfter(ADD_TIER_PREFERENCE)) {
                out.writeStringCollection(tierPreference);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("index", index)
                .field("shard_id", shardId)
                .field("state", state.toString())
                .field("primary", primary)
                .field("node", node)
                .field("node_is_desired", nodeIsDesired)
                .field("relocating_node", relocatingNode)
                .field("relocating_node_is_desired", relocatingNodeIsDesired)
                .field("forecast_write_load", forecastWriteLoad)
                .field("forecast_shard_size_in_bytes", forecastShardSizeInBytes)
                .field("tier_preference", tierPreference)
                .endObject();
        }
    }

    public record ShardAssignmentView(Set<String> nodeIds, int total, int unassigned, int ignored) implements Writeable, ToXContentObject {

        public static ShardAssignmentView from(StreamInput in) throws IOException {
            return new ShardAssignmentView(in.readSet(StreamInput::readString), in.readVInt(), in.readVInt(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(nodeIds, StreamOutput::writeString);
            out.writeVInt(total);
            out.writeVInt(unassigned);
            out.writeVInt(ignored);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .array("node_ids", nodeIds.toArray(String[]::new))
                .field("total", total)
                .field("unassigned", unassigned)
                .field("ignored", ignored)
                .endObject();
        }
    }
}
