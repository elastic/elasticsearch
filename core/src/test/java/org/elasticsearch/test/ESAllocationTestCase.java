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
package org.elasticsearch.test;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocators;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.ReplicaShardAllocator;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.test.gateway.NoopGatewayAllocator;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.util.CollectionUtils.arrayAsArrayList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

/**
 */
public abstract class ESAllocationTestCase extends ESTestCase {

    public static AllocationService createAllocationService() {
        return createAllocationService(Settings.Builder.EMPTY_SETTINGS);
    }

    public static AllocationService createAllocationService(Settings settings) {
        return createAllocationService(settings, getRandom());
    }

    public static AllocationService createAllocationService(Settings settings, Random random) {
        return createAllocationService(settings, new NodeSettingsService(Settings.Builder.EMPTY_SETTINGS), random);
    }

    public static AllocationService createAllocationService(Settings settings, NodeSettingsService nodeSettingsService, Random random) {
        return new AllocationService(settings,
                randomAllocationDeciders(settings, nodeSettingsService, random),
                new ShardsAllocators(settings, NoopGatewayAllocator.INSTANCE), EmptyClusterInfoService.INSTANCE);
    }

    public static AllocationService createAllocationService(Settings settings, ClusterInfoService clusterInfoService) {
        return new AllocationService(settings,
                randomAllocationDeciders(settings, new NodeSettingsService(Settings.Builder.EMPTY_SETTINGS), getRandom()),
                new ShardsAllocators(settings, NoopGatewayAllocator.INSTANCE), clusterInfoService);
    }

    public static AllocationService createAllocationService(Settings settings, GatewayAllocator allocator) {
        return new AllocationService(settings,
                randomAllocationDeciders(settings, new NodeSettingsService(Settings.Builder.EMPTY_SETTINGS), getRandom()),
                new ShardsAllocators(settings, allocator), EmptyClusterInfoService.INSTANCE);
    }



    public static AllocationDeciders randomAllocationDeciders(Settings settings, NodeSettingsService nodeSettingsService, Random random) {
        final List<Class<? extends AllocationDecider>> defaultAllocationDeciders = ClusterModule.DEFAULT_ALLOCATION_DECIDERS;
        final List<AllocationDecider> list = new ArrayList<>();
        for (Class<? extends AllocationDecider> deciderClass : ClusterModule.DEFAULT_ALLOCATION_DECIDERS) {
            try {
                try {
                    Constructor<? extends AllocationDecider> constructor = deciderClass.getConstructor(Settings.class, NodeSettingsService.class);
                    list.add(constructor.newInstance(settings, nodeSettingsService));
                } catch (NoSuchMethodException e) {
                    Constructor<? extends AllocationDecider> constructor = null;
                    constructor = deciderClass.getConstructor(Settings.class);
                    list.add(constructor.newInstance(settings));
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        assertThat(list.size(), equalTo(defaultAllocationDeciders.size()));
        for (AllocationDecider d : list) {
            assertThat(defaultAllocationDeciders.contains(d.getClass()), is(true));
        }
        Collections.shuffle(list, random);
        return new AllocationDeciders(settings, list.toArray(new AllocationDecider[0]));

    }

    public static DiscoveryNode newNode(String nodeId) {
        return new DiscoveryNode(nodeId, DummyTransportAddress.INSTANCE, Version.CURRENT);
    }

    public static DiscoveryNode newNode(String nodeId, TransportAddress address) {
        return new DiscoveryNode(nodeId, address, Version.CURRENT);
    }

    public static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode("", nodeId, DummyTransportAddress.INSTANCE, attributes, Version.CURRENT);
    }

    public static DiscoveryNode newNode(String nodeName,String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(nodeName, nodeId, DummyTransportAddress.INSTANCE, attributes, Version.CURRENT);
    }

    public static DiscoveryNode newNode(String nodeId, Version version) {
        return new DiscoveryNode(nodeId, DummyTransportAddress.INSTANCE, version);
    }

    public static ClusterState startRandomInitializingShard(ClusterState clusterState, AllocationService strategy) {
        List<ShardRouting> initializingShards = clusterState.getRoutingNodes().shardsWithState(INITIALIZING);
        if (initializingShards.isEmpty()) {
            return clusterState;
        }
        RoutingTable routingTable = strategy.applyStartedShards(clusterState, arrayAsArrayList(initializingShards.get(randomInt(initializingShards.size() - 1)))).routingTable();
        return ClusterState.builder(clusterState).routingTable(routingTable).build();
    }

    public static AllocationDeciders yesAllocationDeciders() {
        return new AllocationDeciders(Settings.EMPTY, new AllocationDecider[] {new TestAllocateDecision(Decision.YES)});
    }

    public static AllocationDeciders noAllocationDeciders() {
        return new AllocationDeciders(Settings.EMPTY, new AllocationDecider[] {new TestAllocateDecision(Decision.NO)});
    }

    public static AllocationDeciders throttleAllocationDeciders() {
        return new AllocationDeciders(Settings.EMPTY, new AllocationDecider[] {new TestAllocateDecision(Decision.THROTTLE)});
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

    /**
     * Mocks behavior in ReplicaShardAllocator to remove delayed shards from list of unassigned shards so they don't get reassigned yet.
     * Also computes delay in UnassignedInfo based on customizable time source.
     */
    protected static class DelayedShardsMockGatewayAllocator extends GatewayAllocator {
        private final ReplicaShardAllocator replicaShardAllocator = new ReplicaShardAllocator(Settings.EMPTY) {
            @Override
            protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetchData(ShardRouting shard, RoutingAllocation allocation) {
                return new AsyncShardFetch.FetchResult<>(shard.shardId(), null, Collections.<String>emptySet(), Collections.<String>emptySet());
            }
        };

        private volatile Function<ShardRouting, Long> timeSource;

        public DelayedShardsMockGatewayAllocator() {
            super(Settings.EMPTY, null, null);
        }

        public void setTimeSource(Function<ShardRouting, Long> timeSource) {
            this.timeSource = timeSource;
        }

        @Override
        public void applyStartedShards(StartedRerouteAllocation allocation) {}

        @Override
        public void applyFailedShards(FailedRerouteAllocation allocation) {}

        @Override
        public boolean allocateUnassigned(RoutingAllocation allocation) {
            final RoutingNodes.UnassignedShards.UnassignedIterator unassignedIterator = allocation.routingNodes().unassigned().iterator();
            boolean changed = false;
            while (unassignedIterator.hasNext()) {
                ShardRouting shard = unassignedIterator.next();
                if (shard.primary() || shard.allocatedPostIndexCreate() == false) {
                    continue;
                }
                changed |= replicaShardAllocator.ignoreUnassignedIfDelayed(timeSource == null ? System.nanoTime() : timeSource.apply(shard),
                        allocation, unassignedIterator, shard);
            }
            return changed;
        }
    }
}
