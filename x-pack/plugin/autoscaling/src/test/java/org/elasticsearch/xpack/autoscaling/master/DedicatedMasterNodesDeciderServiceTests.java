/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.master;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.xpack.autoscaling.AutoscalingTestCase;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.memory.AutoscalingMemoryInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.autoscaling.master.DedicatedMasterNodesDeciderService.DEFAULT_NUMBER_OF_MASTER_NODES;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DedicatedMasterNodesDeciderServiceTests extends AutoscalingTestCase {
    public void testScaleUp() {
        var minNumHotNodes = randomIntBetween(4, 6);
        var minHotNodeMemory = ByteSizeValue.ofGb(randomIntBetween(4, 64));
        var masterNodeMemory = ByteSizeValue.ofGb(randomIntBetween(1, 4));
        final Settings policySettings = Settings.builder()
            .put(DedicatedMasterNodesDeciderService.MIN_HOT_NODES.getKey(), minNumHotNodes)
            .put(DedicatedMasterNodesDeciderService.MIN_HOT_NODE_MEMORY.getKey(), minHotNodeMemory)
            .put(DedicatedMasterNodesDeciderService.MASTER_NODE_MEMORY.getKey(), masterNodeMemory)
            .build();
        var hotNodes = new DiscoveryNode[minNumHotNodes];
        for (int i = 0; i < hotNodes.length; i++) {
            hotNodes[i] = createNode(
                DiscoveryNodeRole.MASTER_ROLE,
                DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE,
                DiscoveryNodeRole.DATA_HOT_NODE_ROLE
            );
        }
        var clusterState = ClusterStateCreationUtils.state(hotNodes[0], hotNodes[0], hotNodes);
        var context = new TestContext(clusterState, (node) -> minHotNodeMemory.getBytes());

        assertAutoscalingDeciderResult(context, policySettings, (result) -> {
            AutoscalingCapacity capacity = result.requiredCapacity();
            assertThat(
                capacity.total().memory(),
                equalTo(ByteSizeValue.ofBytes(DEFAULT_NUMBER_OF_MASTER_NODES * masterNodeMemory.getBytes()))
            );
            assertThat(capacity.node().memory(), equalTo(masterNodeMemory));
            assertThat(capacity.total().storage(), is(nullValue()));
            assertThat(capacity.node().storage(), is(nullValue()));
            assertThat(
                result.reason().summary(),
                containsString("Number of hot and content nodes [" + minNumHotNodes + "] Total memory size of hot and content nodes")
            );
        });
    }

    public void testDoesNotScaleDownWhenThereAreDedicatedMasters() {
        var minNumHotNodes = randomIntBetween(4, 6);
        var minHotNodeMemory = ByteSizeValue.ofGb(randomIntBetween(4, 64));
        var masterNodeMemory = ByteSizeValue.ofGb(randomIntBetween(1, 4));
        final Settings policySettings = Settings.builder()
            .put(DedicatedMasterNodesDeciderService.MIN_HOT_NODES.getKey(), minNumHotNodes)
            .put(DedicatedMasterNodesDeciderService.MIN_HOT_NODE_MEMORY.getKey(), minHotNodeMemory)
            .put(DedicatedMasterNodesDeciderService.MASTER_NODE_MEMORY.getKey(), masterNodeMemory)
            .build();

        int numOfHotNodes = randomIntBetween(1, minNumHotNodes - 1);
        var hotNodes = new DiscoveryNode[numOfHotNodes];
        for (int i = 0; i < hotNodes.length; i++) {
            hotNodes[i] = createNode(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE, DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        }
        final int numberOfMasterNodes = randomIntBetween(1, 3);
        AutoscalingCapacity currentCapacity = AutoscalingCapacity.builder()
            .node(null, ByteSizeValue.ofGb(1).getBytes())
            .total(null, ByteSizeValue.ofGb(numberOfMasterNodes).getBytes())
            .build();
        var clusterState = ClusterStateCreationUtils.state(hotNodes[0], hotNodes[0], hotNodes);
        var context = new TestContext(clusterState, (node) -> minHotNodeMemory.getBytes(), currentCapacity);

        assertAutoscalingDeciderResult(context, policySettings, (result) -> {
            AutoscalingCapacity capacity = result.requiredCapacity();
            assertThat(
                capacity.total().memory(),
                equalTo(ByteSizeValue.ofBytes(DEFAULT_NUMBER_OF_MASTER_NODES * masterNodeMemory.getBytes()))
            );
            assertThat(capacity.node().memory(), equalTo(masterNodeMemory));
            assertThat(capacity.total().storage(), is(nullValue()));
            assertThat(capacity.node().storage(), is(nullValue()));
            assertThat(result.reason().summary(), containsString("The cluster has dedicated masters"));
        });
    }

    public void testScaleReturnsEmptyCapacityWhenClusterDoesNotNeedDedicatedMasters() {
        List<DiscoveryNode> discoveryNodes = new ArrayList<>(6);
        for (int i = 0; i < 2; i++) {
            discoveryNodes.add(
                createNode(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE, DiscoveryNodeRole.DATA_HOT_NODE_ROLE)
            );
        }

        for (int i = 0; i < 2; i++) {
            discoveryNodes.add(createNode(DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
        }

        for (int i = 0; i < 2; i++) {
            discoveryNodes.add(createNode(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE));
        }

        var clusterState = ClusterStateCreationUtils.state(
            discoveryNodes.get(0),
            discoveryNodes.get(0),
            discoveryNodes.toArray(new DiscoveryNode[0])
        );
        var context = new TestContext(
            clusterState,
            (node) -> DedicatedMasterNodesDeciderService.MIN_HOT_NODE_MEMORY.get(Settings.EMPTY).getBytes()
        );

        assertAutoscalingDeciderResult(context, (result) -> {
            AutoscalingCapacity capacity = result.requiredCapacity();
            assertThat(capacity.total().memory(), equalTo(ByteSizeValue.ZERO));
            assertThat(capacity.total().storage(), equalTo(ByteSizeValue.ZERO));
            assertThat(capacity.node().memory(), equalTo(ByteSizeValue.ZERO));
            assertThat(capacity.node().storage(), equalTo(ByteSizeValue.ZERO));
        });
    }

    public void testReturnsNullCapacityWhenCurrentCapacityIsUnknown() {
        var minNumHotNodes = randomIntBetween(4, 6);
        var minHotNodeMemory = ByteSizeValue.ofGb(randomIntBetween(4, 64));
        var masterNodeMemory = ByteSizeValue.ofGb(randomIntBetween(1, 4));
        final Settings policySettings = Settings.builder()
            .put(DedicatedMasterNodesDeciderService.MIN_HOT_NODES.getKey(), minNumHotNodes)
            .put(DedicatedMasterNodesDeciderService.MIN_HOT_NODE_MEMORY.getKey(), minHotNodeMemory)
            .put(DedicatedMasterNodesDeciderService.MASTER_NODE_MEMORY.getKey(), masterNodeMemory)
            .build();
        var hotNodes = new DiscoveryNode[minNumHotNodes];
        for (int i = 0; i < hotNodes.length; i++) {
            hotNodes[i] = createNode(
                DiscoveryNodeRole.MASTER_ROLE,
                DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE,
                DiscoveryNodeRole.DATA_HOT_NODE_ROLE
            );
        }
        var clusterState = ClusterStateCreationUtils.state(hotNodes[0], hotNodes[0], hotNodes);
        var context = new TestContext(clusterState, (node) -> minHotNodeMemory.getBytes(), null);

        assertAutoscalingDeciderResult(context, policySettings, (result) -> {
            AutoscalingCapacity capacity = result.requiredCapacity();
            assertThat(capacity, is(nullValue()));
            assertThat(result.reason().summary(), containsString("current capacity not available"));
        });
    }

    private void assertAutoscalingDeciderResult(AutoscalingDeciderContext context, Consumer<AutoscalingDeciderResult> consumer) {
        assertAutoscalingDeciderResult(context, Settings.EMPTY, consumer);
    }

    private void assertAutoscalingDeciderResult(
        AutoscalingDeciderContext context,
        Settings settings,
        Consumer<AutoscalingDeciderResult> consumer
    ) {
        var service = new DedicatedMasterNodesDeciderService();
        consumer.accept(service.scale(settings, context));
    }

    private DiscoveryNode createNode(DiscoveryNodeRole... roles) {
        List<DiscoveryNodeRole> discoveryNodeRoles = Arrays.asList(roles);
        return new DiscoveryNode(
            UUIDs.randomBase64UUID(),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.unmodifiableSortedSet(new TreeSet<>(discoveryNodeRoles)),
            Version.CURRENT
        );
    }

    static class TestContext implements AutoscalingDeciderContext {
        private final ClusterState clusterState;
        private final AutoscalingMemoryInfo autoscalingMemoryInfo;
        private final AutoscalingCapacity currentCapacity;

        TestContext(ClusterState clusterState, AutoscalingMemoryInfo autoscalingMemoryInfo) {
            this(clusterState, autoscalingMemoryInfo, AutoscalingCapacity.ZERO);
        }

        TestContext(ClusterState clusterState, AutoscalingMemoryInfo autoscalingMemoryInfo, AutoscalingCapacity currentCapacity) {
            this.clusterState = clusterState;
            this.autoscalingMemoryInfo = autoscalingMemoryInfo;
            this.currentCapacity = currentCapacity;
        }

        @Override
        public ClusterState state() {
            return clusterState;
        }

        @Override
        public AutoscalingCapacity currentCapacity() {
            return currentCapacity;
        }

        @Override
        public Set<DiscoveryNode> nodes() {
            return null;
        }

        @Override
        public Set<DiscoveryNodeRole> roles() {
            return null;
        }

        @Override
        public ClusterInfo info() {
            return null;
        }

        @Override
        public SnapshotShardSizeInfo snapshotShardSizeInfo() {
            return null;
        }

        @Override
        public AutoscalingMemoryInfo autoscalingMemoryInfo() {
            return autoscalingMemoryInfo;
        }

        @Override
        public void ensureNotCancelled() {

        }
    }
}
