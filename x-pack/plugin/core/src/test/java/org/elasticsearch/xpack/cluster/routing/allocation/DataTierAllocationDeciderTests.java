/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.xpack.core.DataTier;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataTierAllocationDeciderTests extends ESAllocationTestCase {

    private static final Set<Setting<?>> ALL_SETTINGS;
    private static final DiscoveryNode HOT_NODE = newNode("node-hot", Collections.singleton(DataTier.DATA_HOT_NODE_ROLE));
    private static final DiscoveryNode WARM_NODE = newNode("node-warm", Collections.singleton(DataTier.DATA_WARM_NODE_ROLE));
    private static final DiscoveryNode COLD_NODE = newNode("node-cold", Collections.singleton(DataTier.DATA_COLD_NODE_ROLE));
    private static final DiscoveryNode FROZEN_NODE = newNode("node-frozen", Collections.singleton(DataTier.DATA_FROZEN_NODE_ROLE));
    private static final DiscoveryNode DATA_NODE = newNode("node-data", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ALL_SETTINGS);
    private final DataTierAllocationDecider decider = new DataTierAllocationDecider(clusterSettings);
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(decider,
            new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()));
    private final AllocationService service = new AllocationService(allocationDeciders,
        new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE);

    private final ShardRouting shard = ShardRouting.newUnassigned(new ShardId("myindex", "myindex", 0), true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"));

    static {
        Set<Setting<?>> allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(DataTierAllocationDecider.CLUSTER_ROUTING_REQUIRE_SETTING);
        allSettings.add(DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE_SETTING);
        allSettings.add(DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE_SETTING);
        ALL_SETTINGS = allSettings;
    }

    public void testClusterRequires() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"));
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        clusterSettings.applySettings(Settings.builder()
            .put(DataTierAllocationDecider.CLUSTER_ROUTING_REQUIRE, "data_hot")
            .build());
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
        }

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, COLD_NODE, FROZEN_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match all cluster setting [cluster.routing.allocation.require._tier] " +
                    "tier filters [data_hot]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match all cluster setting [cluster.routing.allocation.require._tier] " +
                    "tier filters [data_hot]"));
        }
    }

    public void testClusterIncludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"));
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        clusterSettings.applySettings(Settings.builder()
            .put(DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE, "data_warm,data_frozen")
            .build());
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, FROZEN_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
        }

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any cluster setting [cluster.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_frozen]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any cluster setting [cluster.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_frozen]"));
        }
    }


    public void testClusterExcludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"));
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        clusterSettings.applySettings(Settings.builder()
            .put(DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE, "data_warm,data_frozen")
            .build());
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, FROZEN_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm,data_frozen]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm,data_frozen]"));

        }

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
        }
    }

    public void testIndexRequires() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"),
            Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_REQUIRE, "data_hot")
                .build());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
        }

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, COLD_NODE, FROZEN_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match all index setting [index.routing.allocation.require._tier] tier filters [data_hot]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match all index setting [index.routing.allocation.require._tier] tier filters [data_hot]"));
        }
    }

    public void testIndexIncludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"),
            Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE, "data_warm,data_frozen")
                .build());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, FROZEN_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
        }

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_frozen]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_frozen]"));
        }
    }

    public void testIndexExcludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"),
            Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE, "data_warm,data_frozen")
                .build());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, FROZEN_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm,data_frozen]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm,data_frozen]"));

        }

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
        }
    }

    public void testClusterAndIndex() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"),
            Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE, "data_warm,data_frozen")
                .build());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, 0);
        clusterSettings.applySettings(Settings.builder()
            .put(DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE, "data_frozen")
            .build());
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_frozen]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_frozen]"));
        }

        for (DiscoveryNode n : Arrays.asList(FROZEN_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] tier filters [data_frozen]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] tier filters [data_frozen]"));
        }

        for (DiscoveryNode n : Arrays.asList(WARM_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
        }
    }

    private ClusterState prepareState(ClusterState initialState) {
        return prepareState(initialState, Settings.EMPTY);
    }

    private ClusterState prepareState(ClusterState initialState, Settings indexSettings) {
        return ClusterState.builder(initialState)
            .nodes(DiscoveryNodes.builder()
                .add(HOT_NODE)
                .add(WARM_NODE)
                .add(COLD_NODE)
                .add(FROZEN_NODE)
                .add(DATA_NODE)
                .build())
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("myindex")
                    .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(indexSettings)
                        .build()))
                .build())
            .build();
    }
}
