/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalance;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardAssignment;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class RecoveryDirectCancellationServiceTests extends ESAllocationTestCase {

    public void testComputeDirectCancellationCandidates() {
        final var indexMetadata = IndexMetadata.builder(randomIndexName()).settings(indexSettings(IndexVersion.current(), 2, 1)).build();
        final var index = indexMetadata.getIndex();
        final var undesiredShardId = new ShardId(index, 0);
        final var desiredShardId = new ShardId(index, 1);
        final var undesiredReplicaAllocationId = AllocationId.newInitializing(randomIdentifier("undesired-"));
        final var desiredReplicaAllocationId = AllocationId.newInitializing(randomIdentifier("desired-"));

        final var indexRoutingTable = IndexRoutingTable.builder(index)
            .addShard(newShardRouting(undesiredShardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(undesiredShardId, "node-1", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(undesiredReplicaAllocationId)
                    .build()
            )
            .addShard(newShardRouting(desiredShardId, "node-0", true, STARTED))
            .addShard(
                TestShardRouting.shardRoutingBuilder(desiredShardId, "node-2", false, ShardRoutingState.INITIALIZING)
                    .withAllocationId(desiredReplicaAllocationId)
                    .build()
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(RoutingTable.builder().add(indexRoutingTable))
            .build();

        final var balance = new DesiredBalance(
            1,
            Map.of(
                undesiredShardId,
                new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0),
                desiredShardId,
                new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)
            )
        );

        final var routingAllocation = createRoutingAllocationFrom(clusterState);
        final var candidates = RecoveryDirectCancellationService.computeDirectCancellationCandidates(balance, routingAllocation)
            .candidates();

        assertThat(candidates, hasSize(1));
        final var candidate = candidates.getFirst();
        assertThat(candidate.node(), equalTo(clusterState.nodes().get("node-1")));
        assertThat(candidate.cancellations(), hasSize(1));
        final var cancellation = candidate.cancellations().getFirst();
        assertThat(cancellation.shardId(), equalTo(undesiredShardId));
        assertThat(cancellation.allocationId(), equalTo(undesiredReplicaAllocationId.getId()));
        assertFalse(cancellation.cancelIfStarted());

        final var forbidRemainOnNode1 = new AllocationDecider() {
            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return shardRouting.shardId().equals(undesiredShardId) && shardRouting.primary() == false ? Decision.NO : Decision.YES;
            }
        };
        final var routingAllocationWithForbidRemain = createRoutingAllocationFrom(clusterState, forbidRemainOnNode1);
        final var escalatedCandidates = RecoveryDirectCancellationService.computeDirectCancellationCandidates(
            balance,
            routingAllocationWithForbidRemain
        ).candidates();

        assertThat(escalatedCandidates, hasSize(1));
        assertTrue(escalatedCandidates.getFirst().cancellations().getFirst().cancelIfStarted());
    }

    public void testDirectCancellationCandidatesIncludesPrimaryRelocation() {
        final var indexMetadata = IndexMetadata.builder("index-1").settings(indexSettings(IndexVersion.current(), 1, 0)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);

        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(
                RoutingTable.builder().add(IndexRoutingTable.builder(index).addShard(newShardRouting(shardId, "node-0", true, STARTED)))
            )
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-2"), 1, 0, 0)));
        final var forbidRemain = new AllocationDecider() {
            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return Decision.NO;
            }
        };

        final var allocation = createRoutingAllocationFrom(clusterState, forbidRemain);
        final var startedPrimary = allocation.routingNodes().node("node-0").getByShardId(shardId);
        allocation.routingNodes()
            .relocateShard(startedPrimary, "node-1", ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, "test-setup", allocation.changes());

        final var candidates = RecoveryDirectCancellationService.computeDirectCancellationCandidates(balance, allocation).candidates();

        assertThat(candidates, hasSize(1));
        assertTrue(candidates.getFirst().cancellations().getFirst().cancelIfStarted());

        final var target = allocation.routingNodes().node("node-1").getByShardId(shardId);
        assertThat(target, notNullValue());
        assertTrue(target.initializing());
        final var source = allocation.routingNodes().node("node-0").getByShardId(shardId);
        assertThat(source, notNullValue());
        assertTrue(source.relocating());
    }

    public void testDirectCancellationCandidatesDoNotEscalateSoleSearchableCopy() {
        final var indexMetadata = IndexMetadata.builder("index-1").settings(indexSettings(IndexVersion.current(), 1, 1)).build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);
        final var searchOnlyAllocationId = AllocationId.newInitializing("search-only-replica");

        final var indexRoutingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(index)
                    .addShard(newShardRouting(shardId, "node-0", true, STARTED))
                    .addShard(
                        TestShardRouting.shardRoutingBuilder(shardId, "node-1", false, ShardRoutingState.INITIALIZING)
                            .withAllocationId(searchOnlyAllocationId)
                            .withRole(ShardRouting.Role.SEARCH_ONLY)
                            .build()
                    )
            );
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes(3))
            .metadata(Metadata.builder().put(indexMetadata, true))
            .routingTable(indexRoutingTable)
            .build();

        final var balance = new DesiredBalance(1, Map.of(shardId, new ShardAssignment(Set.of("node-0", "node-2"), 2, 0, 0)));
        final var forbidRemain = new AllocationDecider() {
            @Override
            public Decision canRemain(
                IndexMetadata indexMetadata,
                ShardRouting shardRouting,
                RoutingNode node,
                RoutingAllocation allocation
            ) {
                return Decision.NO;
            }
        };

        final var allocation = createRoutingAllocationFrom(clusterState, forbidRemain);
        final var candidates = RecoveryDirectCancellationService.computeDirectCancellationCandidates(balance, allocation).candidates();

        assertThat(candidates, hasSize(1));
        assertFalse(candidates.getFirst().cancellations().getFirst().cancelIfStarted());

        final var searchOnlyReplica = allocation.routingNodes().node("node-1").getByShardId(shardId);
        assertThat(searchOnlyReplica, notNullValue());
        assertTrue(searchOnlyReplica.initializing());
        assertThat(searchOnlyReplica.allocationId(), equalTo(searchOnlyAllocationId));
    }

    private static RoutingAllocation createRoutingAllocationFrom(ClusterState clusterState, AllocationDecider... deciders) {
        return TestRoutingAllocationFactory.forClusterState(clusterState).allocationDeciders(deciders).mutable();
    }

    private static DiscoveryNodes discoveryNodes(int nodeCount) {
        final var discoveryNodes = DiscoveryNodes.builder();
        for (var i = 0; i < nodeCount; i++) {
            discoveryNodes.add(newNode("node-" + i, "node-" + i, Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE)));
        }
        discoveryNodes.masterNodeId("node-0").localNodeId("node-0");
        return discoveryNodes.build();
    }
}
