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
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.gateway.PrimaryShardAllocator;
import org.elasticsearch.gateway.ReplicaShardAllocator;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A gateway allocator implementation that keeps an in memory list of started shard allocation
 * that are used as replies to the, normally async, fetch data requests. The in memory list
 * is adapted when shards are started and failed.
 *
 * Nodes leaving and joining the cluster do not change the list of shards the class tracks but
 * rather serves as a filter to what is returned by fetch data. Concretely - fetch data will
 * only return shards that were started on nodes that are currently part of the cluster.
 *
 * For now only primary shard related data is fetched. Replica request always get an empty response.
 *
 *
 * This class is useful to use in unit tests that require the functionality of {@link GatewayAllocator} but do
 * not have all the infrastructure required to use it.
 */
public class TestGatewayAllocator extends GatewayAllocator {

    Map<String /* node id */, Map<ShardId, ShardRouting>> knownAllocations = new HashMap<>();
    DiscoveryNodes currentNodes = DiscoveryNodes.EMPTY_NODES;

    PrimaryShardAllocator primaryShardAllocator = new PrimaryShardAllocator() {
        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // for now always return immediately what we know
            final ShardId shardId = shard.shardId();
            final Set<String> ignoreNodes = allocation.getIgnoreNodes(shardId);
            Map<DiscoveryNode, NodeGatewayStartedShards> foundShards = knownAllocations.values().stream()
                .flatMap(shardMap -> shardMap.values().stream())
                .filter(ks -> ks.shardId().equals(shardId))
                .filter(ks -> ignoreNodes.contains(ks.currentNodeId()) == false)
                .filter(ks -> currentNodes.nodeExists(ks.currentNodeId()))
                .collect(Collectors.toMap(
                    routing -> currentNodes.get(routing.currentNodeId()),
                    routing ->
                        new NodeGatewayStartedShards(
                            currentNodes.get(routing.currentNodeId()), routing.allocationId().getId(), routing.primary())));

            return new AsyncShardFetch.FetchResult<>(shardId, foundShards, ignoreNodes);
        }
    };

    ReplicaShardAllocator replicaShardAllocator = new ReplicaShardAllocator() {
        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetaData> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // for now, just pretend no node has data
            final ShardId shardId = shard.shardId();
            return new AsyncShardFetch.FetchResult<>(shardId, Collections.emptyMap(), allocation.getIgnoreNodes(shardId));
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return true;
        }
    };

    @Override
    public void applyStartedShards(RoutingAllocation allocation, List<ShardRouting> startedShards) {
        currentNodes = allocation.nodes();
        allocation.routingNodes().shards(ShardRouting::active).forEach(this::addKnownAllocation);
    }

    @Override
    public void applyFailedShards(RoutingAllocation allocation, List<FailedShard> failedShards) {
        currentNodes = allocation.nodes();
        for (FailedShard failedShard : failedShards) {
            final ShardRouting failedRouting = failedShard.getRoutingEntry();
            Map<ShardId, ShardRouting> nodeAllocations = knownAllocations.get(failedRouting.currentNodeId());
            if (nodeAllocations != null) {
                nodeAllocations.remove(failedRouting.shardId());
                if (nodeAllocations.isEmpty()) {
                    knownAllocations.remove(failedRouting.currentNodeId());
                }
            }
        }
    }

    @Override
    public void allocateUnassigned(RoutingAllocation allocation) {
        currentNodes = allocation.nodes();
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator);
    }

    /**
     * manually add a specific shard to the allocations the gateway keeps track of
     */
    public void addKnownAllocation(ShardRouting shard) {
            knownAllocations.computeIfAbsent(shard.currentNodeId(), id -> new HashMap<>())
                .put(shard.shardId(), shard);
    }
}
