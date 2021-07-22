/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import joptsimple.internal.Strings;

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
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotsConstants;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.elasticsearch.xpack.core.DataTier.DATA_FROZEN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataTierAllocationDeciderTests extends ESAllocationTestCase {

    public static final Set<Setting<?>> ALL_SETTINGS;
    private static final DiscoveryNode HOT_NODE = newNode("node-hot", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
    private static final DiscoveryNode WARM_NODE = newNode("node-warm", Collections.singleton(DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
    private static final DiscoveryNode COLD_NODE = newNode("node-cold", Collections.singleton(DiscoveryNodeRole.DATA_COLD_NODE_ROLE));
    private static final DiscoveryNode CONTENT_NODE =
        newNode("node-content", Collections.singleton(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE));
    private static final DiscoveryNode DATA_NODE = newNode("node-data", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ALL_SETTINGS);
    private final DataTierAllocationDecider decider = new DataTierAllocationDecider(Settings.EMPTY, clusterSettings);
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(decider,
            new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()));
    private final AllocationService service = new AllocationService(allocationDeciders,
        new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
        EmptySnapshotsInfoService.INSTANCE);

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
            null, null, 0);
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

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, COLD_NODE)) {
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
        assertWarnings("[cluster.routing.allocation.require._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testClusterIncludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"));
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, null, 0);
        allocation.debugDecision(true);
        clusterSettings.applySettings(Settings.builder()
            .put(DataTierAllocationDecider.CLUSTER_ROUTING_INCLUDE, "data_warm,data_cold")
            .build());
        Decision d;
        RoutingNode node;
        assertWarnings("[cluster.routing.allocation.include._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, DATA_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
        }

        for (DiscoveryNode n : Arrays.asList(HOT_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any cluster setting [cluster.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any cluster setting [cluster.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_cold]"));
        }
    }


    public void testClusterExcludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"));
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, null, 0);
        allocation.debugDecision(true);
        clusterSettings.applySettings(Settings.builder()
            .put(DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE, "data_warm")
            .build());
        Decision d;
        RoutingNode node;
        assertWarnings("[cluster.routing.allocation.exclude._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
        for (DiscoveryNode n : Arrays.asList(WARM_NODE, DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm]"));

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
            null, null, 0);
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

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, COLD_NODE)) {
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
        assertWarnings("[index.routing.allocation.require._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testIndexIncludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"),
            Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE, "data_warm,data_cold")
                .build());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, DATA_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
        }

        for (DiscoveryNode n : Arrays.asList(HOT_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_cold]"));
        }
        assertWarnings("[index.routing.allocation.include._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testIndexExcludes() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"),
            Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE, "data_warm,data_cold")
                .build());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, null,0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(WARM_NODE, DATA_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm,data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] " +
                    "tier filters [data_warm,data_cold]"));

        }

        for (DiscoveryNode n : Arrays.asList(HOT_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
        }
        assertWarnings("[index.routing.allocation.exclude._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testIndexPrefer() {
        ClusterState state = ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(DiscoveryNodes.builder()
                .add(HOT_NODE)
                .build())
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("myindex")
                    .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_warm,data_cold")
                        .build()))
                .build())
            .build();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold], " +
                    "but no nodes for any of those tiers are available in the cluster"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold], " +
                    "but no nodes for any of those tiers are available in the cluster"));
        }

        state = ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(DiscoveryNodes.builder()
                .add(HOT_NODE)
                .add(COLD_NODE)
                .build())
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("myindex")
                    .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_warm,data_cold")
                        .build()))
                .build())
            .build();
        allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_cold] tier"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_cold] tier"));
        }

        for (DiscoveryNode n : Arrays.asList(COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_cold]"));
        }
    }

    public void testIndexPreferWithInclude() {
        ClusterState state = ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(DiscoveryNodes.builder()
                .add(WARM_NODE)
                .add(COLD_NODE)
                .build())
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("myindex")
                    .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE, "data_cold")
                        .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_warm,data_cold")
                        .build()))
                .build())
            .build();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE, CONTENT_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] tier filters [data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] tier filters [data_cold]"));
        }

        for (DiscoveryNode n : Arrays.asList(COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_warm] tier"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_warm] tier"));
        }

        for (DiscoveryNode n : Arrays.asList(DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_warm]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_warm]"));
        }

        assertWarnings("[index.routing.allocation.include._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testIndexPreferWithExclude() {
        ClusterState state = ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(DiscoveryNodes.builder()
                .add(WARM_NODE)
                .add(COLD_NODE)
                .build())
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("myindex")
                    .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE, "data_warm")
                        .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_warm,data_cold")
                        .build()))
                .build())
            .build();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;


        for (DiscoveryNode n : Arrays.asList(HOT_NODE, COLD_NODE, CONTENT_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_warm] tier"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_warm] tier"));
        }

        for (DiscoveryNode n : Arrays.asList(WARM_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] tier filters [data_warm]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] tier filters [data_warm]"));
        }

        for (DiscoveryNode n : Arrays.asList(DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] tier filters [data_warm]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node matches any index setting [index.routing.allocation.exclude._tier] tier filters [data_warm]"));
        }
        assertWarnings("[index.routing.allocation.exclude._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testIndexPreferWithRequire() {
        ClusterState state = ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(DiscoveryNodes.builder()
                .add(WARM_NODE)
                .add(COLD_NODE)
                .build())
            .metadata(Metadata.builder()
                .put(IndexMetadata.builder("myindex")
                    .settings(Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(DataTierAllocationDecider.INDEX_ROUTING_REQUIRE, "data_cold")
                        .put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, "data_warm,data_cold")
                        .build()))
                .build())
            .build();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE, CONTENT_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match all index setting [index.routing.allocation.require._tier] tier filters [data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match all index setting [index.routing.allocation.require._tier] tier filters [data_cold]"));
        }

        for (DiscoveryNode n : Arrays.asList(COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_warm] tier"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] " +
                    "and node does not meet the required [data_warm] tier"));
        }

        for (DiscoveryNode n : Arrays.asList(DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_warm]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
            assertThat(node.toString(), d.getExplanation(),
                containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_warm]"));
        }
        assertWarnings("[index.routing.allocation.require._tier] setting was deprecated in Elasticsearch " +
            "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testClusterAndIndex() {
        ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"),
            Settings.builder()
                .put(DataTierAllocationDecider.INDEX_ROUTING_INCLUDE, "data_warm,data_cold")
                .build());
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state,
            null, null,0);
        clusterSettings.applySettings(Settings.builder()
            .put(DataTierAllocationDecider.CLUSTER_ROUTING_EXCLUDE, "data_cold")
            .build());
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(node.toString(), d.getExplanation(),
                containsString("node does not match any index setting [index.routing.allocation.include._tier] " +
                    "tier filters [data_warm,data_cold]"));
        }

        for (DiscoveryNode n : Arrays.asList(DATA_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] tier filters [data_cold]"));
            d = decider.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(d.getExplanation(),
                containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] tier filters [data_cold]"));
        }

        for (DiscoveryNode n : Arrays.asList(WARM_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = decider.canAllocate(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
            d = decider.canRemain(shard, node, allocation);
            assertThat(n.toString(), d.type(), equalTo(Decision.Type.YES));
        }
        assertWarnings("[cluster.routing.allocation.exclude._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.",
            "[index.routing.allocation.include._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testTierNodesPresent() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().build();

        assertFalse(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));

        nodes = DiscoveryNodes.builder()
            .add(WARM_NODE)
            .add(CONTENT_NODE)
            .build();

        assertFalse(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));

        nodes = DiscoveryNodes.builder()
            .add(DATA_NODE)
            .build();

        assertTrue(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));
    }

    public void testPreferredTierAvailable() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().build();

        assertThat(DataTierAllocationDecider.preferredAvailableTier("data", nodes), equalTo(Optional.empty()));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_hot,data_warm", nodes), equalTo(Optional.empty()));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_warm,data_content", nodes), equalTo(Optional.empty()));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_cold", nodes), equalTo(Optional.empty()));

        nodes = DiscoveryNodes.builder()
            .add(WARM_NODE)
            .add(CONTENT_NODE)
            .build();

        assertThat(DataTierAllocationDecider.preferredAvailableTier("data", nodes), equalTo(Optional.empty()));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_hot,data_warm", nodes), equalTo(Optional.of("data_warm")));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_warm,data_content", nodes), equalTo(Optional.of("data_warm")));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_content,data_warm", nodes), equalTo(Optional.of("data_content")));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_hot,data_content,data_warm", nodes),
            equalTo(Optional.of("data_content")));
        assertThat(DataTierAllocationDecider.preferredAvailableTier("data_hot,data_cold,data_warm", nodes),
            equalTo(Optional.of("data_warm")));
    }

    public void testExistedClusterFilters() {
        Settings existedSettings = Settings.builder()
            .put("cluster.routing.allocation.include._tier", "data_hot,data_warm")
            .put("cluster.routing.allocation.exclude._tier", "data_cold")
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ALL_SETTINGS);
        DataTierAllocationDecider dataTierAllocationDecider = new DataTierAllocationDecider(existedSettings, clusterSettings);
        AllocationDeciders allocationDeciders = new AllocationDeciders(Arrays.asList(dataTierAllocationDecider));
        AllocationService service = new AllocationService(allocationDeciders,
            new TestGatewayAllocator(), new BalancedShardsAllocator(Settings.EMPTY), EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE);

        ClusterState clusterState = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state"));
        assertWarnings("[cluster.routing.allocation.include._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.",
            "[cluster.routing.allocation.exclude._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.");
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState.getRoutingNodes(), clusterState,
            null, null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = dataTierAllocationDecider.canAllocate(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
            d = dataTierAllocationDecider.canRemain(shard, node, allocation);
            assertThat(d.type(), equalTo(Decision.Type.YES));
        }

        node = new RoutingNode(DATA_NODE.getId(), DATA_NODE, shard);
        d = dataTierAllocationDecider.canAllocate(shard, node, allocation);
        assertThat(d.type(), equalTo(Decision.Type.NO));
        assertThat(d.getExplanation(),
            containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] " +
                "tier filters [data_cold]"));
        d = dataTierAllocationDecider.canRemain(shard, node, allocation);
        assertThat(d.type(), equalTo(Decision.Type.NO));
        assertThat(d.getExplanation(),
            containsString("node matches any cluster setting [cluster.routing.allocation.exclude._tier] " +
                "tier filters [data_cold]"));

        node = new RoutingNode(COLD_NODE.getId(), COLD_NODE, shard);
        d = dataTierAllocationDecider.canAllocate(shard, node, allocation);
        assertThat(d.type(), equalTo(Decision.Type.NO));
        assertThat(d.getExplanation(),
            containsString("node does not match any cluster setting [cluster.routing.allocation.include._tier] " +
                "tier filters [data_hot,data_warm]"));
        d = dataTierAllocationDecider.canRemain(shard, node, allocation);
        assertThat(d.type(), equalTo(Decision.Type.NO));
        assertThat(d.getExplanation(),
            containsString("node does not match any cluster setting [cluster.routing.allocation.include._tier] " +
                "tier filters [data_hot,data_warm]"));
    }

    public void testFrozenIllegalForRegularIndices() {
        List<String> tierList = new ArrayList<>(randomSubsetOf(DataTier.ALL_DATA_TIERS));
        if (tierList.contains(DATA_FROZEN) == false) {
            tierList.add(DATA_FROZEN);
        }
        Randomness.shuffle(tierList);

        String value = Strings.join(tierList, ",");
        Setting<String> setting = randomTierSetting();
        Settings.Builder builder = Settings.builder().put(setting.getKey(), value);
        if (randomBoolean()) {
            builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
        }

        Settings settings = builder.build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(exception.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));
        allowedWarnings("[index.routing.allocation.exclude._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.",
            "[index.routing.allocation.include._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.",
            "[index.routing.allocation.require._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testFrozenLegalForPartialSnapshot() {
        Setting<String> setting = randomTierSetting();
        Settings.Builder builder = Settings.builder().put(setting.getKey(), DATA_FROZEN);
        builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
        builder.put(SearchableSnapshotsConstants.SNAPSHOT_PARTIAL_SETTING.getKey(), true);

        Settings settings = builder.build();

        // validate do not throw
        assertThat(setting.get(settings), equalTo(DATA_FROZEN));
        allowedWarnings("[index.routing.allocation.exclude._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.",
            "[index.routing.allocation.include._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.",
            "[index.routing.allocation.require._tier] setting was deprecated in Elasticsearch " +
                "and will be removed in a future release! See the breaking changes documentation for the next major version.");
    }

    public void testDefaultValueForPreference() {
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(Settings.EMPTY), equalTo(""));

        Settings.Builder builder = Settings.builder();
        builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
        builder.put(SearchableSnapshotsConstants.SNAPSHOT_PARTIAL_SETTING.getKey(), true);

        Settings settings = builder.build();
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(settings), equalTo(DATA_FROZEN));
    }

    public Setting<String> randomTierSetting() {
        //noinspection unchecked
        return randomFrom(
            DataTierAllocationDecider.INDEX_ROUTING_EXCLUDE_SETTING,
            DataTierAllocationDecider.INDEX_ROUTING_INCLUDE_SETTING,
            DataTierAllocationDecider.INDEX_ROUTING_REQUIRE_SETTING,
            DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING);
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
