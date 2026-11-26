/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.shards;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardAllocationId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.node.HealthInfoTests;
import org.elasticsearch.health.node.UpdateHealthInfoCacheAction;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.test.ESTestCase;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LocalRecoveryRetriesHealthTrackerTests extends ESTestCase {

    private static final String LOCAL_NODE_ID = "local-node";
    private static final String MASTER_NODE_ID = "master-node";

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

    /// Only allocations on the local node are reported, even when another node is the elected master.
    public void testOnlyLocalNodeAllocationsAreConsidered() {
        var localRouting = routing(randomShardId(), ShardRoutingState.INITIALIZING);
        var remoteRouting = routing(randomShardId(), "not-local-node", ShardRoutingState.INITIALIZING);

        var service = mock(IndicesClusterStateService.class);
        // Stub a nonzero answer for *any* routing first, so a remote-allocation leak would show up as a false
        // positive rather than coincidentally looking right because the mock only knows about the local one.
        int defaultRetries = randomIntBetween(1, 10);
        when(service.getLocalRecoveryRetryCount(any())).thenReturn(defaultRetries);
        int localRetries = randomValueOtherThan(defaultRetries, () -> randomIntBetween(1, 10));
        when(service.getLocalRecoveryRetryCount(localRouting)).thenReturn(localRetries);

        var tracker = new LocalRecoveryRetriesHealthTracker(clusterService(state(List.of(localRouting, remoteRouting))), () -> service);
        var health = tracker.determineCurrentHealth();

        assertThat(health.localRetries(), equalTo(Map.of(ShardAllocationId.fromRouting(localRouting), localRetries)));
        verify(service, never()).getLocalRecoveryRetryCount(remoteRouting);
    }

    /// The local node may not be in the routing table at all (non-data node, or not yet joined), in which case
    /// there's nothing to report rather than a null pointer. This can't be modeled by the cluster-state-building
    /// helper, so we mock it here.
    public void testNoLocalRoutingNodeReportsNothing() {
        var service = mock(IndicesClusterStateService.class);
        var state = mock(ClusterState.class);
        var nodes = mock(DiscoveryNodes.class);
        when(nodes.getLocalNodeId()).thenReturn(LOCAL_NODE_ID);
        when(state.nodes()).thenReturn(nodes);
        when(state.getRoutingNodes()).thenReturn(mock(RoutingNodes.class));
        var tracker = new LocalRecoveryRetriesHealthTracker(clusterService(state), () -> service);

        assertThat(tracker.determineCurrentHealth(), equalTo(LocalRecoveryRetriesHealthInfo.EMPTY));
    }

    public void testSetBuilder() {
        var tracker = new LocalRecoveryRetriesHealthTracker(clusterService(state(List.of())), () -> mock(IndicesClusterStateService.class));
        var builder = mock(UpdateHealthInfoCacheAction.Request.Builder.class);
        var health = new LocalRecoveryRetriesHealthInfo(Map.of(HealthInfoTests.randomShardAllocationId(), randomIntBetween(1, 10)));

        tracker.addToRequestBuilder(builder, health);

        verify(builder).localRecoveryRetriesHealthInfo(health);
    }

    private static LocalRecoveryRetriesHealthInfo determineHealth(Map<ShardRouting, Integer> routingsWithRetries) {
        var service = mock(IndicesClusterStateService.class);
        routingsWithRetries.forEach((routing, retries) -> when(service.getLocalRecoveryRetryCount(routing)).thenReturn(retries));
        var tracker = new LocalRecoveryRetriesHealthTracker(
            clusterService(state(List.copyOf(routingsWithRetries.keySet()))),
            () -> service
        );
        return tracker.determineCurrentHealth();
    }

    private static ClusterService clusterService(ClusterState state) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    /// Builds a real [ClusterState] with discovery nodes and routing table matching the provided `routings`,
    /// with a dedicated master ([MASTER_NODE_ID]). Always register [LOCAL_NODE_ID], even if it doesn't host anything.
    private static ClusterState state(List<ShardRouting> routings) {
        ProjectMetadata.Builder projectMetadata = ProjectMetadata.builder(Metadata.DEFAULT_PROJECT_ID);
        RoutingTable.Builder routingTable = RoutingTable.builder();
        for (var entry : routings.stream().collect(groupingBy(ShardRouting::index)).entrySet()) {
            Index index = entry.getKey();
            List<ShardRouting> shardsInIndex = entry.getValue();
            IndexMetadata indexMetadata = IndexMetadata.builder(index.getName())
                .settings(indexSettings(IndexVersion.current(), index.getUUID(), shardsInIndex.size(), 0))
                .build();
            IndexRoutingTable.Builder indexRoutingTable = IndexRoutingTable.builder(index);
            shardsInIndex.forEach(indexRoutingTable::addShard);
            projectMetadata.put(indexMetadata, false);
            routingTable.add(indexRoutingTable);
        }

        Set<String> nodeIds = new LinkedHashSet<>(routings.stream().map(ShardRouting::currentNodeId).toList());
        nodeIds.add(LOCAL_NODE_ID);
        nodeIds.add(MASTER_NODE_ID);
        Map<String, DiscoveryNode> nodesById = nodeIds.stream().collect(toMap(id -> id, DiscoveryNodeUtils::create));
        ClusterState stateWithNodes = ClusterStateCreationUtils.state(
            nodesById.get(LOCAL_NODE_ID),
            nodesById.get(MASTER_NODE_ID),
            nodesById.values().toArray(DiscoveryNode[]::new)
        );

        return ClusterState.builder(stateWithNodes)
            .metadata(Metadata.builder().put(projectMetadata))
            .routingTable(GlobalRoutingTable.builder().put(Metadata.DEFAULT_PROJECT_ID, routingTable).build())
            .build();
    }

    private static ShardRouting routing(ShardId shardId, ShardRoutingState routingState) {
        return routing(shardId, LOCAL_NODE_ID, routingState);
    }

    private static ShardRouting routing(ShardId shardId, String nodeId, ShardRoutingState routingState) {
        return TestShardRouting.newShardRouting(shardId, nodeId, true, routingState);
    }

    /// Always shard 0: each call gets its own fresh, independent index, so every group in [state] has exactly
    /// one contiguous shard -- IndexRoutingTable can't have gaps in its shard numbering.
    private static ShardId randomShardId() {
        return new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(22), 0);
    }
}
