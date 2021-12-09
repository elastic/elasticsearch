/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.master;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.summarizingLong;

public class DedicatedMasterNodesDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "dedicated_masters_decider";

    public static final int DEFAULT_NUMBER_OF_MASTER_NODES = 3;
    public static final ByteSizeValue DEFAULT_MASTER_MEMORY = ByteSizeValue.ofGb(1);
    public static final int DEFAULT_MIN_HOT_NODES = 6;
    public static final ByteSizeValue DEFAULT_HOT_NODE_MIN_MEMORY = ByteSizeValue.ofGb(64);

    public static final Setting<ByteSizeValue> MASTER_NODE_MEMORY = Setting.byteSizeSetting(
        "master_node_memory",
        (ignored) -> DEFAULT_MASTER_MEMORY.getStringRep(),
        DEFAULT_MASTER_MEMORY,
        ByteSizeValue.ofBytes(Long.MAX_VALUE)
    );
    public static final Setting<Integer> MIN_HOT_NODES = Setting.intSetting("min_hot_nodes", DEFAULT_MIN_HOT_NODES, 1);
    public static final Setting<ByteSizeValue> MIN_HOT_NODE_MEMORY = Setting.byteSizeSetting(
        "min_hot_node_memory",
        (ignored) -> DEFAULT_HOT_NODE_MIN_MEMORY.getStringRep(),
        ByteSizeValue.ofGb(1),
        ByteSizeValue.ofBytes(Long.MAX_VALUE)
    );

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        AutoscalingCapacity currentCapacity = context.currentCapacity();
        if (currentCapacity == null || currentCapacity.total().memory() == null) {
            return new AutoscalingDeciderResult(null, new DedicatedMasterNodesReason("current capacity not available", -1, -1));
        }
        final ByteSizeValue masterNodeMemory = MASTER_NODE_MEMORY.get(configuration);
        long currentTotalCapacityInBytes = currentCapacity.total().memory().getBytes();
        if (currentTotalCapacityInBytes > 0) {
            // If we have dedicated masters but not the total desired capacity (i.e. the operator is still adding them)
            // return the required capacity, otherwise just return the current capacity.
            final AutoscalingCapacity capacity;
            if (currentTotalCapacityInBytes >= DEFAULT_NUMBER_OF_MASTER_NODES * masterNodeMemory.getBytes()) {
                capacity = currentCapacity;
            } else {
                capacity = getDedicatedMastersCapacity(configuration);
            }

            return new AutoscalingDeciderResult(capacity, new DedicatedMasterNodesReason("The cluster has dedicated masters", -1, -1));
        }

        var hotAndContentNodesSummary = StreamSupport.stream(context.state().nodes().spliterator(), false)
            .filter(this::hasHotAndContentRole)
            .collect(summarizingLong(node -> getNodeMemory(node, context)));
        final int hotAndContentNodes = (int) hotAndContentNodesSummary.getCount();
        final long totalHotAndContentNodesMemoryBytes = hotAndContentNodesSummary.getSum();

        final int minHotNodes = MIN_HOT_NODES.get(configuration);
        final ByteSizeValue minHotNodeMemory = MIN_HOT_NODE_MEMORY.get(configuration);
        final AutoscalingCapacity autoscalingCapacity;
        if (hotAndContentNodes >= minHotNodes || totalHotAndContentNodesMemoryBytes >= minHotNodes * minHotNodeMemory.getBytes()) {
            autoscalingCapacity = getDedicatedMastersCapacity(configuration);
        } else {
            autoscalingCapacity = AutoscalingCapacity.ZERO;
        }

        final String summaryMessage = getSummaryMessage(hotAndContentNodes, totalHotAndContentNodesMemoryBytes);
        return new AutoscalingDeciderResult(
            autoscalingCapacity,
            new DedicatedMasterNodesReason(summaryMessage, hotAndContentNodes, totalHotAndContentNodesMemoryBytes)
        );
    }

    private AutoscalingCapacity getDedicatedMastersCapacity(Settings configuration) {
        final ByteSizeValue masterNodeMemory = MASTER_NODE_MEMORY.get(configuration);

        return AutoscalingCapacity.builder()
            .node(null, masterNodeMemory)
            .total(null, ByteSizeValue.ofBytes(DEFAULT_NUMBER_OF_MASTER_NODES * masterNodeMemory.getBytes()))
            .build();
    }

    public String getSummaryMessage(int hotAndContentNodes, long totalHotAndContentNodesMemoryBytes) {
        return "Number of hot and content nodes ["
            + hotAndContentNodes
            + "] "
            + "Total memory size of hot and content nodes ["
            + new ByteSizeValue(totalHotAndContentNodesMemoryBytes, ByteSizeUnit.BYTES)
            + "]";
    }

    protected long getNodeMemory(DiscoveryNode node, AutoscalingDeciderContext context) {
        final Long memory = context.autoscalingMemoryInfo().get(node);
        return memory == null ? 0 : memory;
    }

    private boolean hasHotAndContentRole(DiscoveryNode discoveryNode) {
        final Set<DiscoveryNodeRole> nodeRoles = discoveryNode.getRoles();
        return nodeRoles.contains(DiscoveryNodeRole.DATA_HOT_NODE_ROLE) && nodeRoles.contains(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE);
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(MASTER_NODE_MEMORY, MIN_HOT_NODES, MIN_HOT_NODE_MEMORY);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.MASTER_ROLE);
    }

    @Override
    public String name() {
        return NAME;
    }

    public static class DedicatedMasterNodesReason implements AutoscalingDeciderResult.Reason {
        private final String reason;
        private final int hotAndContentNodes;
        private final long totalHotAndContentNodesMemoryBytes;

        public DedicatedMasterNodesReason(String reason, int hotAndContentNodes, long totalHotAndContentNodesMemory) {
            this.reason = reason;
            this.hotAndContentNodes = hotAndContentNodes;
            this.totalHotAndContentNodesMemoryBytes = totalHotAndContentNodesMemory;
        }

        public DedicatedMasterNodesReason(StreamInput in) throws IOException {
            this.reason = in.readString();
            this.hotAndContentNodes = in.readInt();
            this.totalHotAndContentNodesMemoryBytes = in.readLong();
        }

        public int getHotAndContentNodes() {
            return hotAndContentNodes;
        }

        public long getTotalHotAndContentNodesMemoryBytes() {
            return totalHotAndContentNodesMemoryBytes;
        }

        @Override
        public String summary() {
            return reason;
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(reason);
            out.writeInt(hotAndContentNodes);
            out.writeLong(totalHotAndContentNodesMemoryBytes);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("reason", reason);
            builder.field("hot_and_content_nodes", hotAndContentNodes);
            builder.field("total_hot_and_content_nodes_memory", totalHotAndContentNodesMemoryBytes);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DedicatedMasterNodesReason that = (DedicatedMasterNodesReason) o;
            return hotAndContentNodes == that.hotAndContentNodes
                && Objects.equals(reason, that.reason)
                && Objects.equals(totalHotAndContentNodesMemoryBytes, that.totalHotAndContentNodesMemoryBytes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(reason, hotAndContentNodes, totalHotAndContentNodesMemoryBytes);
        }
    }
}
