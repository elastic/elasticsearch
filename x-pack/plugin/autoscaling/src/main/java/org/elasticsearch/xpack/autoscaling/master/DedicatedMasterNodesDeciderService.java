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

public class DedicatedMasterNodesDeciderService implements AutoscalingDeciderService {
    public static final String NAME = "dedicated-masters-decider";

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
        int hotAndContentNodes = (int) StreamSupport.stream(context.state().nodes().spliterator(), false)
            .filter(this::hasHotAndContentRole)
            .count();
        long totalHotAndContentNodesMemory = StreamSupport.stream(context.state().nodes().spliterator(), false)
            .filter(this::hasHotAndContentRole)
            .mapToLong(node -> getNodeMemory(node, context))
            .sum();

        final int minHotNodes = MIN_HOT_NODES.get(configuration);
        final ByteSizeValue minHotNodeMemory = MIN_HOT_NODE_MEMORY.get(configuration);
        final ByteSizeValue masterNodeMemory = MASTER_NODE_MEMORY.get(configuration);
        final AutoscalingCapacity autoscalingCapacity;
        if (hotAndContentNodes >= minHotNodes && totalHotAndContentNodesMemory >= minHotNodes * minHotNodeMemory.getBytes()) {
            autoscalingCapacity = AutoscalingCapacity.builder()
                .node(null, masterNodeMemory)
                .total(null, ByteSizeValue.ofBytes(DEFAULT_NUMBER_OF_MASTER_NODES * masterNodeMemory.getBytes()))
                .build();
        } else {
            autoscalingCapacity = AutoscalingCapacity.ZERO;
        }

        return new AutoscalingDeciderResult(
            autoscalingCapacity,
            new DedicatedMasterNodesReason(hotAndContentNodes, new ByteSizeValue(totalHotAndContentNodesMemory, ByteSizeUnit.BYTES))
        );
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
        private final int hotAndContentNodes;
        private final ByteSizeValue totalHotAndContentNodesMemory;

        public DedicatedMasterNodesReason(int hotAndContentNodes, ByteSizeValue totalHotAndContentNodesMemory) {
            this.hotAndContentNodes = hotAndContentNodes;
            this.totalHotAndContentNodesMemory = totalHotAndContentNodesMemory;
        }

        public DedicatedMasterNodesReason(StreamInput in) throws IOException {
            this.hotAndContentNodes = in.readInt();
            this.totalHotAndContentNodesMemory = new ByteSizeValue(in);
        }

        public int getHotAndContentNodes() {
            return hotAndContentNodes;
        }

        public ByteSizeValue getTotalHotAndContentNodesMemory() {
            return totalHotAndContentNodesMemory;
        }

        @Override
        public String summary() {
            return "Number of hot and content nodes ["
                + hotAndContentNodes
                + "] "
                + "Total memory size of hot and content nodes ["
                + totalHotAndContentNodesMemory
                + "]";
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(hotAndContentNodes);
            totalHotAndContentNodesMemory.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("hot_and_content_nodes", hotAndContentNodes);
            builder.field("total_hot_and_content_nodes_memory", totalHotAndContentNodesMemory);
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DedicatedMasterNodesReason that = (DedicatedMasterNodesReason) o;
            return hotAndContentNodes == that.hotAndContentNodes
                && Objects.equals(totalHotAndContentNodesMemory, that.totalHotAndContentNodesMemory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(hotAndContentNodes, totalHotAndContentNodesMemory);
        }
    }
}
