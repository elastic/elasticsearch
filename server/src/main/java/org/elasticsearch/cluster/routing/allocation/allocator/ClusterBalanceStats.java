/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.TransportVersion;
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

public record ClusterBalanceStats(Map<String, TierBalanceStats> tiers, Map<String, NodeBalanceStats> nodes)
    implements
        Writeable,
        ToXContentObject {

    public static ClusterBalanceStats EMPTY = new ClusterBalanceStats(Map.of(), Map.of());

    public static ClusterBalanceStats createFrom(
        ClusterState clusterState,
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
            var nodeStats = NodeBalanceStats.createFrom(routingNode, clusterState.metadata(), clusterInfo, writeLoadForecaster);
            nodes.put(routingNode.node().getName(), nodeStats);
            for (DiscoveryNodeRole role : dataRoles) {
                tierToNodeStats.computeIfAbsent(role.roleName(), ignored -> new ArrayList<>()).add(nodeStats);
            }
        }
        return new ClusterBalanceStats(Maps.transformValues(tierToNodeStats, TierBalanceStats::createFrom), nodes);
    }

    public static ClusterBalanceStats readFrom(StreamInput in) throws IOException {
        return new ClusterBalanceStats(
            in.readImmutableMap(StreamInput::readString, TierBalanceStats::readFrom),
            in.readImmutableMap(StreamInput::readString, NodeBalanceStats::readFrom)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(tiers, StreamOutput::writeString, StreamOutput::writeWriteable);
        out.writeMap(nodes, StreamOutput::writeString, StreamOutput::writeWriteable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("tiers").map(tiers).field("nodes").map(nodes).endObject();
    }

    public record TierBalanceStats(
        MetricStats shardCount,
        MetricStats forecastWriteLoad,
        MetricStats forecastShardSize,
        MetricStats actualShardSize
    ) implements Writeable, ToXContentObject {

        private static TierBalanceStats createFrom(List<NodeBalanceStats> nodes) {
            return new TierBalanceStats(
                MetricStats.createFrom(nodes, it -> it.shards),
                MetricStats.createFrom(nodes, it -> it.forecastWriteLoad),
                MetricStats.createFrom(nodes, it -> it.forecastShardSize),
                MetricStats.createFrom(nodes, it -> it.actualShardSize)
            );
        }

        public static TierBalanceStats readFrom(StreamInput in) throws IOException {
            return new TierBalanceStats(
                MetricStats.readFrom(in),
                MetricStats.readFrom(in),
                MetricStats.readFrom(in),
                MetricStats.readFrom(in)
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            shardCount.writeTo(out);
            forecastWriteLoad.writeTo(out);
            forecastShardSize.writeTo(out);
            actualShardSize.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject()
                .field("shard_count", shardCount)
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
        double forecastWriteLoad,
        long forecastShardSize,
        long actualShardSize
    ) implements Writeable, ToXContentObject {

        private static final String UNKNOWN_NODE_ID = "UNKNOWN";

        private static NodeBalanceStats createFrom(
            RoutingNode routingNode,
            Metadata metadata,
            ClusterInfo clusterInfo,
            WriteLoadForecaster writeLoadForecaster
        ) {
            double forecastWriteLoad = 0.0;
            long forecastShardSize = 0L;
            long actualShardSize = 0L;

            for (ShardRouting shardRouting : routingNode) {
                var indexMetadata = metadata.index(shardRouting.index());
                var shardSize = clusterInfo.getShardSize(shardRouting, 0L);
                assert indexMetadata != null;
                forecastWriteLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
                forecastShardSize += indexMetadata.getForecastedShardSizeInBytes().orElse(shardSize);
                actualShardSize += shardSize;
            }

            return new NodeBalanceStats(
                routingNode.nodeId(),
                routingNode.node().getRoles().stream().map(DiscoveryNodeRole::roleName).toList(),
                routingNode.size(),
                forecastWriteLoad,
                forecastShardSize,
                actualShardSize
            );
        }

        public static NodeBalanceStats readFrom(StreamInput in) throws IOException {
            return new NodeBalanceStats(
                in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0) ? in.readString() : UNKNOWN_NODE_ID,
                in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0) ? in.readStringList() : List.of(),
                in.readInt(),
                in.readDouble(),
                in.readLong(),
                in.readLong()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
                out.writeString(nodeId);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_0)) {
                out.writeStringCollection(roles);
            }
            out.writeInt(shards);
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
                .field("forecast_write_load", forecastWriteLoad)
                .humanReadableField("forecast_disk_usage_bytes", "forecast_disk_usage", ByteSizeValue.ofBytes(forecastShardSize))
                .humanReadableField("actual_disk_usage_bytes", "actual_disk_usage", ByteSizeValue.ofBytes(actualShardSize))
                .endObject();
        }
    }
}
