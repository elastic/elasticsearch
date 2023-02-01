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

    private final DesiredBalanceStats stats;
    private final ClusterBalanceStats clusterBalanceStats;
    private final Map<String, Map<Integer, DesiredShards>> routingTable;

    public DesiredBalanceResponse(
        DesiredBalanceStats stats,
        ClusterBalanceStats clusterBalanceStats,
        Map<String, Map<Integer, DesiredShards>> routingTable
    ) {
        this.stats = stats;
        this.clusterBalanceStats = clusterBalanceStats;
        this.routingTable = routingTable;
    }

    public static DesiredBalanceResponse from(StreamInput in) throws IOException {
        return new DesiredBalanceResponse(
            DesiredBalanceStats.readFrom(in),
            in.getTransportVersion().onOrAfter(CLUSTER_BALANCE_STATS_VERSION)
                ? ClusterBalanceStats.readFrom(in)
                : ClusterBalanceStats.EMPTY,
            in.readImmutableMap(StreamInput::readString, v -> v.readImmutableMap(StreamInput::readVInt, DesiredShards::from))
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
        if (out.getTransportVersion().onOrAfter(CLUSTER_BALANCE_STATS_VERSION)) {
            clusterBalanceStats.writeTo(out);
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
            singleChunk((builder, p) -> builder.endObject(), (builder, p) -> builder.endObject())
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof DesiredBalanceResponse that
            && Objects.equals(stats, that.stats)
            && Objects.equals(clusterBalanceStats, that.clusterBalanceStats)
            && Objects.equals(routingTable, that.routingTable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stats, clusterBalanceStats, routingTable);
    }

    @Override
    public String toString() {
        return "DesiredBalanceResponse{stats="
            + stats
            + ", clusterBalanceStats="
            + clusterBalanceStats
            + ", routingTable="
            + routingTable
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
        boolean relocatingNodeIsDesired,
        int shardId,
        String index,
        @Nullable Double forecastedWriteLoad,
        @Nullable Long forecastedShardSizeInBytes
    ) implements Writeable, ToXContentObject {

        private static final TransportVersion ADD_FORECASTS_VERSION = TransportVersion.V_8_7_0;

        public static ShardView from(StreamInput in) throws IOException {
            if (in.getTransportVersion().onOrAfter(ADD_FORECASTS_VERSION)) {
                return new ShardView(
                    ShardRoutingState.fromValue(in.readByte()),
                    in.readBoolean(),
                    in.readOptionalString(),
                    in.readBoolean(),
                    in.readOptionalString(),
                    in.readBoolean(),
                    in.readVInt(),
                    in.readString(),
                    in.readOptionalDouble(),
                    in.readOptionalLong()
                );
            } else {
                var shardView = new ShardView(
                    ShardRoutingState.fromValue(in.readByte()),
                    in.readBoolean(),
                    in.readOptionalString(),
                    in.readBoolean(),
                    in.readOptionalString(),
                    in.readBoolean(),
                    in.readVInt(),
                    in.readString(),
                    null,
                    null
                );
                in.readOptionalWriteable(AllocationId::new);
                return shardView;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(state.value());
            out.writeBoolean(primary);
            out.writeOptionalString(node);
            out.writeBoolean(nodeIsDesired);
            out.writeOptionalString(relocatingNode);
            out.writeBoolean(relocatingNodeIsDesired);
            out.writeVInt(shardId);
            out.writeString(index);
            if (out.getTransportVersion().onOrAfter(ADD_FORECASTS_VERSION)) {
                out.writeOptionalDouble(forecastedWriteLoad);
                out.writeOptionalLong(forecastedShardSizeInBytes);
            } else {
                out.writeMissingWriteable(AllocationId.class);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("state", state.toString())
                .field("primary", primary)
                .field("node", node)
                .field("node_is_desired", nodeIsDesired)
                .field("relocating_node", relocatingNode)
                .field("relocating_node_is_desired", relocatingNodeIsDesired)
                .field("shard_id", shardId)
                .field("index", index)
                .field("forecasted_write_load", forecastedWriteLoad)
                .field("forecasted_shard_size_in_bytes", forecastedShardSizeInBytes)
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
