/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.ClusterBalanceStats;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceStats;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
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

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;

public class DesiredBalanceResponse extends ActionResponse implements ChunkedToXContentObject {

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
            ClusterBalanceStats.readFrom(in),
            in.readImmutableMap(v -> v.readImmutableMap(StreamInput::readVInt, DesiredShards::from)),
            new ClusterInfo(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        stats.writeTo(out);
        out.writeWriteable(clusterBalanceStats);
        out.writeMap(
            routingTable,
            (shardsOut, shards) -> shardsOut.writeMap(shards, StreamOutput::writeVInt, StreamOutput::writeWriteable)
        );
        out.writeWriteable(clusterInfo);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            chunk(
                (builder, p) -> builder.startObject()
                    .field("stats", stats)
                    .field("cluster_balance_stats", clusterBalanceStats)
                    .startObject("routing_table")
            ),
            Iterators.flatMap(
                routingTable.entrySet().iterator(),
                indexEntry -> ChunkedToXContentHelper.object(
                    indexEntry.getKey(),
                    Iterators.flatMap(
                        indexEntry.getValue().entrySet().iterator(),
                        shardEntry -> Iterators.concat(
                            chunk((builder, p) -> builder.field(String.valueOf(shardEntry.getKey()))),
                            shardEntry.getValue().toXContentChunked(params)
                        )
                    )
                )
            ),
            chunk((builder, p) -> builder.endObject().startObject("cluster_info")),
            clusterInfo.toXContentChunked(params),
            chunk((builder, p) -> builder.endObject().endObject())
        );
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

    public record DesiredShards(List<ShardView> current, ShardAssignmentView desired) implements Writeable, ChunkedToXContentObject {

        public static DesiredShards from(StreamInput in) throws IOException {
            return new DesiredShards(in.readCollectionAsList(ShardView::from), ShardAssignmentView.from(in));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(current);
            desired.writeTo(out);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                chunk((builder, p) -> builder.startObject().startArray("current")),
                current().iterator(),
                chunk((builder, p) -> builder.endArray().field("desired").value(desired, p).endObject())
            );
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
            relocatingNodeIsDesired = in.readOptionalBoolean();
            int shardId = in.readVInt();
            String index = in.readString();
            Double forecastWriteLoad = in.readOptionalDouble();
            Long forecastShardSizeInBytes = in.readOptionalLong();
            List<String> tierPreference = in.readStringCollectionAsList();
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
            out.writeOptionalBoolean(relocatingNodeIsDesired);
            out.writeVInt(shardId);
            out.writeString(index);
            out.writeOptionalDouble(forecastWriteLoad);
            out.writeOptionalLong(forecastShardSizeInBytes);
            out.writeStringCollection(tierPreference);
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

        public static final ShardAssignmentView EMPTY = new ShardAssignmentView(Set.of(), 0, 0, 0);

        public static ShardAssignmentView from(StreamInput in) throws IOException {
            final var nodeIds = in.readCollectionAsSet(StreamInput::readString);
            final var total = in.readVInt();
            final var unassigned = in.readVInt();
            final var ignored = in.readVInt();
            if (nodeIds.isEmpty() && total == 0 && unassigned == 0 && ignored == 0) {
                return EMPTY;
            } else {
                return new ShardAssignmentView(nodeIds, total, unassigned, ignored);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringCollection(nodeIds);
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
