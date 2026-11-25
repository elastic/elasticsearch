/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardAllocationId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.node.HealthInfoTests;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalRecoveryRetriesHealthTrackerTests extends ESTestCase {

    public void testNoShards() {
        assertThat(determineHealth(Map.of()), equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testShardsWithoutLocalRetriesAreOmitted() {
        var routing = routing(randomShardId(), ShardRoutingState.INITIALIZING);
        assertThat(determineHealth(Map.of(routing, 0)), equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testShardsWithLocalRetriesAreReported() {
        var routing1 = routing(randomShardId(), ShardRoutingState.INITIALIZING);
        var routing2 = routing(randomShardId(), ShardRoutingState.INITIALIZING);
        int retries1 = randomIntBetween(1, 10);
        int retries2 = randomIntBetween(1, 10);

        var health = determineHealth(Map.of(routing1, retries1, routing2, retries2));

        assertThat(
            health.localRetries(),
            equalTo(Map.of(ShardAllocationId.fromRouting(routing1), retries1, ShardAllocationId.fromRouting(routing2), retries2))
        );
    }

    /// Tracker must only report on inactive shards.
    public void testShardsWhichAreActiveAreOmitted() {
        var active = routing(randomShardId(), ShardRoutingState.STARTED);

        var health = determineHealth(Map.of(active, randomIntBetween(1, 10)));

        assertThat(health, equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    /// The local node may not be in the routing table at all (non-data node, or not
    /// yet joined), in which case there's nothing to report rather than a null pointer.
    public void testNoLocalRoutingNodeReportsNothing() {
        var service = mock(IndicesClusterStateService.class);
        var tracker = new LocalRecoveryRetriesHealthTracker(clusterService(null), () -> service);

        assertThat(tracker.determineCurrentHealth(), equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testSetBuilder() {
        var tracker = new LocalRecoveryRetriesHealthTracker(clusterService(List.of()), () -> mock(IndicesClusterStateService.class));
        var builder = mock(UpdateHealthInfoCacheAction.Request.Builder.class);
        var health = new LocalRecoveryRetriesHealthInfo(Map.of(HealthInfoTests.randomShardAllocationId(), randomIntBetween(1, 10)));

        tracker.addToRequestBuilder(builder, health);

        verify(builder).localRecoveryRetriesHealthInfo(health);
    }

    private static LocalRecoveryRetriesHealthInfo determineHealth(Map<ShardRouting, Integer> routingsWithRetries) {
        var service = mock(IndicesClusterStateService.class);
        routingsWithRetries.forEach((routing, retries) -> when(service.getLocalRecoveryRetryCount(routing)).thenReturn(retries));
        var tracker = new LocalRecoveryRetriesHealthTracker(clusterService(List.copyOf(routingsWithRetries.keySet())), () -> service);
        return tracker.determineCurrentHealth();
    }

    /// Creates a [ClusterService] whose [ClusterState#getRoutingNodes] resolves the local node to a [RoutingNode]
    /// iterating over [routings], or to no [RoutingNode] at all when `routings` is null.
    ///
    /// Mocks are used because [ClusterState] and friends are plain data holders that only need to answer a
    /// handful of calls here -- the tracker only reads the local node's shard routings from them.
    private static ClusterService clusterService(List<ShardRouting> routings) {
        String localNodeId = randomAlphaOfLength(8);
        var routingNodes = mock(RoutingNodes.class);
        if (routings == null) {
            when(routingNodes.node(localNodeId)).thenReturn(null);
        } else {
            var routingNode = mock(RoutingNode.class);
            when(routingNode.iterator()).thenAnswer(invocation -> routings.iterator());
            when(routingNodes.node(localNodeId)).thenReturn(routingNode);
        }
        var discoveryNodes = mock(DiscoveryNodes.class);
        when(discoveryNodes.getLocalNodeId()).thenReturn(localNodeId);
        var state = mock(ClusterState.class);
        when(state.getRoutingNodes()).thenReturn(routingNodes);
        when(state.nodes()).thenReturn(discoveryNodes);
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    private static ShardRouting routing(ShardId shardId, ShardRoutingState routingState) {
        return TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(8), true, routingState);
    }

    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(22), randomIntBetween(0, 5));
    }
}
