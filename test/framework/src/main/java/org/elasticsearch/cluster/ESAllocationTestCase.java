/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;

public abstract class ESAllocationTestCase extends ESTestCase {
    private static final ClusterSettings EMPTY_CLUSTER_SETTINGS =
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    public static MockAllocationService createAllocationService() {
        return createAllocationService(Settings.Builder.EMPTY_SETTINGS);
    }

    public static MockAllocationService createAllocationService(Settings settings) {
        return createAllocationService(settings, random());
    }

    public static MockAllocationService createAllocationService(Settings settings, Random random) {
        return createAllocationService(settings, EMPTY_CLUSTER_SETTINGS, random);
    }

    public static MockAllocationService createAllocationService(Settings settings, ClusterSettings clusterSettings, Random random) {
        return new MockAllocationService(settings,
                randomAllocationDeciders(settings, clusterSettings, random),
                new TestGatewayAllocator(), new BalancedShardsAllocator(settings), EmptyClusterInfoService.INSTANCE);
    }

    public static MockAllocationService createAllocationService(Settings settings, ClusterInfoService clusterInfoService) {
        return new MockAllocationService(settings,
                randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
            new TestGatewayAllocator(), new BalancedShardsAllocator(settings), clusterInfoService);
    }

    public static MockAllocationService createAllocationService(Settings settings, GatewayAllocator gatewayAllocator) {
        return new MockAllocationService(settings,
                randomAllocationDeciders(settings, EMPTY_CLUSTER_SETTINGS, random()),
                gatewayAllocator, new BalancedShardsAllocator(settings), EmptyClusterInfoService.INSTANCE);
    }

    public static AllocationDeciders randomAllocationDeciders(Settings settings, ClusterSettings clusterSettings, Random random) {
        List<AllocationDecider> deciders = new ArrayList<>(
            ClusterModule.createAllocationDeciders(settings, clusterSettings, Collections.emptyList()));
        Collections.shuffle(deciders, random);
        return new AllocationDeciders(settings, deciders);
    }

    protected static Set<DiscoveryNode.Role> MASTER_DATA_ROLES =
            Collections.unmodifiableSet(EnumSet.of(DiscoveryNode.Role.MASTER, DiscoveryNode.Role.DATA));

    protected static DiscoveryNode newNode(String nodeId) {
        return newNode(nodeId, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeName, String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeName, nodeId, buildNewFakeTransportAddress(), attributes, MASTER_DATA_ROLES, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, MASTER_DATA_ROLES, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Set<DiscoveryNode.Role> roles) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT);
    }

    protected static DiscoveryNode newNode(String nodeId, Version version) {
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), emptyMap(), MASTER_DATA_ROLES, version);
    }

    protected  static ClusterState startRandomInitializingShard(ClusterState clusterState, AllocationService strategy) {
        List<ShardRouting> initializingShards = clusterState.getRoutingNodes().shardsWithState(INITIALIZING);
        if (initializingShards.isEmpty()) {
            return clusterState;
        }
        return strategy.applyStartedShards(clusterState,
            arrayAsArrayList(initializingShards.get(randomInt(initializingShards.size() - 1))));
    }

    protected static AllocationDeciders yesAllocationDeciders() {
        return new AllocationDeciders(Settings.EMPTY, Arrays.asList(
            new TestAllocateDecision(Decision.YES),
            new SameShardAllocationDecider(Settings.EMPTY,
                                           new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))));
    }

    protected static AllocationDeciders noAllocationDeciders() {
        return new AllocationDeciders(Settings.EMPTY, Collections.singleton(new TestAllocateDecision(Decision.NO)));
    }

    protected static AllocationDeciders throttleAllocationDeciders() {
        return new AllocationDeciders(Settings.EMPTY, Arrays.asList(
            new TestAllocateDecision(Decision.THROTTLE),
            new SameShardAllocationDecider(Settings.EMPTY,
                                           new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))));
    }

    protected ClusterState applyStartedShardsUntilNoChange(ClusterState clusterState, AllocationService service) {
        ClusterState lastClusterState;
        do {
            lastClusterState = clusterState;
            logger.debug("ClusterState: {}", clusterState.getRoutingNodes());
            clusterState = service.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        } while (lastClusterState.equals(clusterState) == false);
        return clusterState;
    }

    public static class TestAllocateDecision extends AllocationDecider {

        private final Decision decision;

        public TestAllocateDecision(Decision decision) {
            super(Settings.EMPTY);
            this.decision = decision;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            return decision;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
            return decision;
        }

        @Override
        public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
            return decision;
        }
    }

    /** A lock {@link AllocationService} allowing tests to override time */
    protected static class MockAllocationService extends AllocationService {

        private volatile long nanoTimeOverride = -1L;

        public MockAllocationService(Settings settings, AllocationDeciders allocationDeciders, GatewayAllocator gatewayAllocator,
                                     ShardsAllocator shardsAllocator, ClusterInfoService clusterInfoService) {
            super(settings, allocationDeciders, gatewayAllocator, shardsAllocator, clusterInfoService);
        }

        public void setNanoTimeOverride(long nanoTime) {
            this.nanoTimeOverride = nanoTime;
        }

        @Override
        protected long currentNanoTime() {
            return nanoTimeOverride == -1L ? super.currentNanoTime() : nanoTimeOverride;
        }
    }

    /**
     * Mocks behavior in ReplicaShardAllocator to remove delayed shards from list of unassigned shards so they don't get reassigned yet.
     */
    protected static class DelayedShardsMockGatewayAllocator extends GatewayAllocator {

        public DelayedShardsMockGatewayAllocator() {
            super(Settings.EMPTY);
        }

        @Override
        public void applyStartedShards(RoutingAllocation allocation, List<ShardRouting> startedShards) {
            // no-op
        }

        @Override
        public void applyFailedShards(RoutingAllocation allocation, List<FailedShard> failedShards) {
            // no-op
        }

        @Override
        public void allocateUnassigned(RoutingAllocation allocation) {
            final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
            while (unassignedIterator.hasNext()) {
                ShardRouting shard = unassignedIterator.next();
                if (shard.primary() || shard.unassignedInfo().getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
                    continue;
                }
                if (shard.unassignedInfo().isDelayed()) {
                    unassignedIterator.removeAndIgnore(UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION, allocation.changes());
                }
            }
        }
    }
}
