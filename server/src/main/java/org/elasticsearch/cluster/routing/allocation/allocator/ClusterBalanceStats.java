/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;

public record ClusterBalanceStats(
    int shards,
    int undesiredShardAllocations,
    Map<String, TierBalanceStats> tiers,
    Map<String, NodeBalanceStats> nodes
) implements Writeable, ToXContentObject {

    public static final ClusterBalanceStats EMPTY = new ClusterBalanceStats(0, 0, Map.of(), Map.of());

    public static ClusterBalanceStats createFrom(
        ClusterState clusterState,
        DesiredBalance desiredBalance,
        ClusterInfo clusterInfo,
        WriteLoadForecaster writeLoadForecaster
    ) {
        var tierToNodeStats = new HashMap<String, List<NodeBalanceStats>>();
        var nodes = new HashMap<String, NodeBalanceStats>();
        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            var dataRoles = routingNode.node().getRoles().stream().filter(DiscoveryNodeRole::canContainData).toList();
            if (dataRoles.isEmpty()) {
                continue;
            }
            var nodeStats = NodeBalanceStats.createFrom(
                routingNode,
                clusterState.metadata(),
                desiredBalance,
                clusterInfo,
                writeLoadForecaster
            );
            nodes.put(routingNode.node().getName(), nodeStats);
            for (DiscoveryNodeRole role : dataRoles) {
                tierToNodeStats.computeIfAbsent(role.roleName(), ignored -> new ArrayList<>()).add(nodeStats);
            }
        }
        return new ClusterBalanceStats(
            nodes.values().stream().mapToInt(NodeBalanceStats::shards).sum(),
            nodes.values().stream().mapToInt(NodeBalanceStats::undesiredShardAllocations).sum(),
            Maps.transformValues(tierToNodeStats, TierBalanceStats::createFrom),
            nodes
        );
    }

    public static ClusterBalanceStats readFrom(StreamInput in) throws IOException {
        return new ClusterBalanceStats(
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0) ? in.readVInt() : -1,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0) ? in.readVInt() : -1,
            in.readImmutableMap(TierBalanceStats::readFrom),
            in.readImmutableMap(NodeBalanceStats::readFrom)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeVInt(shards);
            out.writeVInt(undesiredShardAllocations);
        }
        out.writeMap(tiers, StreamOutput::writeWriteable);
        out.writeMap(nodes, StreamOutput::writeWriteable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("shard_count", shards)
            .field("undesired_shard_allocation_count", undesiredShardAllocations)
            .field("tiers", tiers)
            .field("nodes", nodes)
            .endObject();
    }

    public record TierBalanceStats(
        MetricStats shardCount,
        MetricStats undesiredShardAllocations,
        MetricStats forecastWriteLoad,
        MetricStats forecastShardSize,
        MetricStats actualShardSize
    ) implements Writeable, ToXContentObject {

        private static TierBalanceStats createFrom(List<NodeBalanceStats> nodes) {
            return new TierBalanceStats(
                MetricStats.createFrom(nodes, it -> it.shards),
                MetricStats.createFrom(nodes, it -> it.undesiredShardAllocations),
                MetricStats.createFrom(nodes, it -> it.forecastWriteLoad),
                MetricStats.createFrom(nodes, it -> it.forecastShardSize),
                MetricStats.createFrom(nodes, it -> it.actualShardSize)
            );
        }

        public static TierBalanceStats readFrom(StreamInput in) throws IOException {
            return new TierBalanceStats(
                MetricStats.readFrom(in),
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)
                    ? MetricStats.readFrom(in)
                    : new MetricStats(0.0, 0.0, 0.0, 0.0, 0.0),
                MetricStats.readFrom(in),
                MetricStats.readFrom(in),
                MetricStats.readFrom(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardCount.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                undesiredShardAllocations.writeTo(out);
            }
            forecastWriteLoad.writeTo(out);
            forecastShardSize.writeTo(out);
            actualShardSize.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("shard_count", shardCount)
                .field("undesired_shard_allocation_count", undesiredShardAllocations)
                .field("forecast_write_load", forecastWriteLoad)
                .field("forecast_disk_usage", forecastShardSize)
                .field("actual_disk_usage", actualShardSize)
                .endObject();
        }
    }

    public record MetricStats(double total, double min, double max, double average, double stdDev) implements Writeable, ToXContentObject {

        private static MetricStats createFrom(List<NodeBalanceStats> nodes, ToDoubleFunction<NodeBalanceStats> metricExtractor) {
            assert nodes.isEmpty() == false : "Stats must be created from non empty nodes";
            double total = 0.0;
            double total2 = 0.0;
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            int count = 0;
            for (NodeBalanceStats node : nodes) {
                var metric = metricExtractor.applyAsDouble(node);
                if (Double.isNaN(metric)) {
                    continue;
                }
                total += metric;
                total2 += Math.pow(metric, 2);
                min = Math.min(min, metric);
                max = Math.max(max, metric);
                count++;
            }
            double average = count == 0 ? Double.NaN : total / count;
            double stdDev = count == 0 ? Double.NaN : Math.sqrt(total2 / count - Math.pow(average, 2));
            return new MetricStats(total, min, max, average, stdDev);
        }

        public static MetricStats readFrom(StreamInput in) throws IOException {
            return new MetricStats(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(total);
            out.writeDouble(min);
            out.writeDouble(max);
            out.writeDouble(average);
            out.writeDouble(stdDev);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("total", total)
                .field("min", min)
                .field("max", max)
                .field("average", average)
                .field("std_dev", stdDev)
                .endObject();
        }
    }

    public record NodeBalanceStats(
        String nodeId,
        List<String> roles,
        int shards,
        int undesiredShardAllocations,
        double forecastWriteLoad,
        long forecastShardSize,
        long actualShardSize
    ) implements Writeable, ToXContentObject {

        private static final String UNKNOWN_NODE_ID = "UNKNOWN";

        private static NodeBalanceStats createFrom(
            RoutingNode routingNode,
            Metadata metadata,
            DesiredBalance desiredBalance,
            ClusterInfo clusterInfo,
            WriteLoadForecaster writeLoadForecaster
        ) {
            int undesired = 0;
            double forecastWriteLoad = 0.0;
            long forecastShardSize = 0L;
            long actualShardSize = 0L;

            for (ShardRouting shardRouting : routingNode) {
                var indexMetadata = metadata.indexMetadata(shardRouting.index());
                var shardSize = clusterInfo.getShardSize(shardRouting, 0L);
                forecastWriteLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
                forecastShardSize += indexMetadata.getForecastedShardSizeInBytes().orElse(shardSize);
                actualShardSize += shardSize;
                if (isDesiredShardAllocation(shardRouting, desiredBalance) == false) {
                    undesired++;
                }
            }

            return new NodeBalanceStats(
                routingNode.nodeId(),
                routingNode.node().getRoles().stream().map(DiscoveryNodeRole::roleName).toList(),
                routingNode.size(),
                undesired,
                forecastWriteLoad,
                forecastShardSize,
                actualShardSize
            );
        }

        private static boolean isDesiredShardAllocation(ShardRouting shardRouting, DesiredBalance desiredBalance) {
            if (shardRouting.relocating()) {
                // relocating out shards are temporarily accepted
                return true;
            }
            var assignment = desiredBalance.getAssignment(shardRouting.shardId());
            return assignment != null && assignment.nodeIds().contains(shardRouting.currentNodeId());
        }

        public static NodeBalanceStats readFrom(StreamInput in) throws IOException {
            return new NodeBalanceStats(
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0) ? in.readString() : UNKNOWN_NODE_ID,
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0) ? in.readStringCollectionAsList() : List.of(),
                in.readInt(),
                in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0) ? in.readVInt() : -1,
                in.readDouble(),
                in.readLong(),
                in.readLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_8_0)) {
                out.writeString(nodeId);
                out.writeStringCollection(roles);
            }
            out.writeInt(shards);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
                out.writeVInt(undesiredShardAllocations);
            }
            out.writeDouble(forecastWriteLoad);
            out.writeLong(forecastShardSize);
            out.writeLong(actualShardSize);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            if (UNKNOWN_NODE_ID.equals(nodeId) == false) {
                builder.field("node_id", nodeId);
            }
            return builder.field("roles", roles)
                .field("shard_count", shards)
                .field("undesired_shard_allocation_count", undesiredShardAllocations)
                .field("forecast_write_load", forecastWriteLoad)
                .humanReadableField("forecast_disk_usage_bytes", "forecast_disk_usage", ByteSizeValue.ofBytes(forecastShardSize))
                .humanReadableField("actual_disk_usage_bytes", "actual_disk_usage", ByteSizeValue.ofBytes(actualShardSize))
                .endObject();
        }
    }
}
