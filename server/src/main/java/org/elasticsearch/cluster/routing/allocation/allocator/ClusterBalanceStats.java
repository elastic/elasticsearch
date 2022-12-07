/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;

import static java.util.stream.Collectors.summarizingDouble;

public record ClusterBalanceStats(Map<String, TierBalanceStats> tiers) implements Writeable, ToXContentObject {

    public static ClusterBalanceStats EMPTY = new ClusterBalanceStats(Map.of());

    public static ClusterBalanceStats createFrom(RoutingNodes routingNodes, Metadata metadata, WriteLoadForecaster writeLoadForecaster) {
        var tierToNodeStats = new HashMap<String, List<NodeStats>>();
        for (RoutingNode routingNode : routingNodes) {
            var dataRoles = routingNode.node().getRoles().stream().filter(DiscoveryNodeRole::canContainData).toList();
            if (dataRoles.isEmpty()) {
                continue;
            }
            var nodeStats = NodeStats.createFrom(routingNode, metadata, writeLoadForecaster);
            for (DiscoveryNodeRole role : dataRoles) {
                tierToNodeStats.computeIfAbsent(role.roleName(), ignored -> new ArrayList<>()).add(nodeStats);
            }
        }
        return new ClusterBalanceStats(Maps.transformValues(tierToNodeStats, TierBalanceStats::createFrom));
    }

    public static ClusterBalanceStats readFrom(StreamInput in) throws IOException {
        return new ClusterBalanceStats(in.readImmutableMap(StreamInput::readString, TierBalanceStats::readFrom));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(tiers, StreamOutput::writeString, StreamOutput::writeWriteable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.map(tiers);
    }

    public record TierBalanceStats(Map<String, MetricStats> metrics) implements Writeable, ToXContentObject {

        private static TierBalanceStats createFrom(List<NodeStats> nodes) {
            return new TierBalanceStats(
                Map.of(
                    "shard_count",
                    MetricStats.createFrom(nodes, it -> it.shards),
                    "total_write_load",
                    MetricStats.createFrom(nodes, it -> it.totalWriteLoad),
                    "total_shard_size",
                    MetricStats.createFrom(nodes, it -> it.totalShardSize)
                )
            );
        }

        public static TierBalanceStats readFrom(StreamInput in) throws IOException {
            return new TierBalanceStats(in.readImmutableMap(StreamInput::readString, MetricStats::readFrom));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(metrics, StreamOutput::writeString, StreamOutput::writeWriteable);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.map(metrics);
        }
    }

    public record MetricStats(double total, double min, double max, double average, double stdDev) implements Writeable, ToXContentObject {

        private static MetricStats createFrom(List<NodeStats> nodes, ToDoubleFunction<NodeStats> metricExtractor) {
            var summary = nodes.stream().collect(summarizingDouble(metricExtractor));
            var stdDev = stdDev(nodes.stream().mapToDouble(metricExtractor).toArray(), summary.getAverage());
            return new MetricStats(summary.getSum(), summary.getMin(), summary.getMax(), summary.getAverage(), stdDev);
        }

        private static double stdDev(double[] data, double avg) {
            var stdDev = 0.0;
            for (double d : data) {
                stdDev += Math.pow(d - avg, 2);
            }
            return Math.sqrt(stdDev / data.length);
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

    private record NodeStats(int shards, double totalWriteLoad, long totalShardSize) {

        private static NodeStats createFrom(RoutingNode routingNode, Metadata metadata, WriteLoadForecaster writeLoadForecaster) {
            double totalWriteLoad = 0.0;
            long totalShardSize = 0L;

            for (ShardRouting shardRouting : routingNode) {
                var indexMetadata = metadata.index(shardRouting.index());
                assert indexMetadata != null;
                totalWriteLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
                totalShardSize += indexMetadata.getForecastedShardSizeInBytes().orElse(0);
            }

            return new NodeStats(routingNode.size(), totalWriteLoad, totalShardSize);
        }
    }
}
