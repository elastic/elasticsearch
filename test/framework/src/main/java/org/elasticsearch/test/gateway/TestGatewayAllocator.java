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

package org.elasticsearch.test.gateway;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.StartedRerouteAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.PrimaryShardAllocator;
import org.elasticsearch.gateway.PriorityComparator;
import org.elasticsearch.gateway.ReplicaShardAllocator;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An allocator used for tests that doesn't do anything
 */
public class TestGatewayAllocator extends GatewayAllocator {

    Map<String, Map<ShardId,ShardRouting>> knownAllocations = new HashMap<>();
    Map<String, DiscoveryNode> knownNodes = new HashMap<>();

    PrimaryShardAllocator primaryShardAllocator = new PrimaryShardAllocator(Settings.EMPTY) {
        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards>
        fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // for now always return immediately what we know
            final ShardId shardId = shard.shardId();
            final Set<String> ignoreNodes = allocation.getIgnoreNodes(shardId);
            Map<DiscoveryNode, NodeGatewayStartedShards> foundShards = knownAllocations.values().stream()
                .flatMap(shardMap -> shardMap.values().stream())
                .filter(ks -> ks.shardId().equals(shardId))
                .filter(ks -> ignoreNodes.contains(ks.currentNodeId()) == false)
                .filter(ks -> knownNodes.containsKey(ks.currentNodeId()))
                .collect(Collectors.toMap(
                    routing -> knownNodes.get(routing.currentNodeId()),
                    routing ->
                        new NodeGatewayStartedShards(
                            knownNodes.get(routing.currentNodeId()), -1, routing.allocationId().getId(), routing.primary())));

            return new AsyncShardFetch.FetchResult<>(shardId, foundShards, Collections.emptySet(), ignoreNodes);
        }
    };

    ReplicaShardAllocator replicaShardAllocator = new ReplicaShardAllocator(Settings.EMPTY) {
        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetaData> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // for now, just pretend no node has data
            final ShardId shardId = shard.shardId();
            return new AsyncShardFetch.FetchResult<>(shardId, Collections.emptyMap(), Collections.emptySet(),
                allocation.getIgnoreNodes(shardId));
        }
    };

    public TestGatewayAllocator() {
        super(Settings.EMPTY, null, null);
    }

    @Override
    public void applyStartedShards(StartedRerouteAllocation allocation) {
        updateNodes(allocation);
        for (ShardRouting shard: allocation.routingNodes().shards(ShardRouting::active)) {
            addKnownAllocation(shard);
        }
    }

    @Override
    public void applyFailedShards(FailedRerouteAllocation allocation) {
        updateNodes(allocation);
        for (FailedRerouteAllocation.FailedShard failedShard : allocation.failedShards()) {
            final ShardRouting failedRouting = failedShard.routingEntry;
            Map<ShardId, ShardRouting> nodeAllocations = knownAllocations.get(failedRouting.currentNodeId());
            if (nodeAllocations != null) {
                nodeAllocations.remove(failedRouting.shardId());
                if (nodeAllocations.isEmpty()) {
                    knownAllocations.remove(failedRouting.currentNodeId());
                }
            }
        }
    }

    private void updateNodes(RoutingAllocation allocation) {
        knownNodes.clear();
        for (DiscoveryNode node: allocation.nodes()) {
            knownNodes.put(node.getId(), node);
        }
    }

    @Override
    public void allocateUnassigned(RoutingAllocation allocation) {
        updateNodes(allocation);
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        unassigned.sort(PriorityComparator.getAllocationComparator(allocation)); // sort for priority ordering

        primaryShardAllocator.allocateUnassigned(allocation);
        replicaShardAllocator.processExistingRecoveries(allocation);
        replicaShardAllocator.allocateUnassigned(allocation);
    }

    public void addKnownAllocation(ShardRouting shard) {
            knownAllocations.computeIfAbsent(shard.currentNodeId(), id -> new HashMap<>())
                .put(shard.shardId(), shard);
    }
}
