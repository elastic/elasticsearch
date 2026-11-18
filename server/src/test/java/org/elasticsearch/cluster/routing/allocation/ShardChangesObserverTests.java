/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.MoveReason;
import org.elasticsearch.cluster.routing.MoveType;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.test.MockLog.assertThatLogger;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

@TestLogging(value = "org.elasticsearch.cluster.routing.allocation.ShardChangesObserver:TRACE", reason = "verifies debug level logging")
public class ShardChangesObserverTests extends ESAllocationTestCase {

    public void testLogShardStarting() {
        var indexName = randomIdentifier();
        var indexMetadata = IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), 1, 0)).build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(indexMetadata))
            .build();

        assertThatLogger(
            () -> applyStartedShardsUntilNoChange(clusterState, createAllocationService()),
            ShardChangesObserver.class,
            new MockLog.SeenEventExpectation(
                "Should log shard initializing",
                ShardChangesObserver.class.getCanonicalName(),
                Level.TRACE,
                "[" + indexName + "][0][P] initializing from " + RecoverySource.Type.EMPTY_STORE + " on node [node-1]"
            ),
            new MockLog.SeenEventExpectation(
                "Should log shard starting",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] started from " + RecoverySource.Type.EMPTY_STORE + " on node [node-1]"
            )
        );
    }

    public void testLogShardMovement() {
        var allocationId = randomUUID();
        var indexName = randomIdentifier();
        var indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0).put("index.routing.allocation.exclude._id", "node-1"))
            .putInSyncAllocationIds(0, Set.of(allocationId))
            .build();

        ShardRouting shard = shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-1", true, ShardRoutingState.STARTED)
            .withAllocationId(AllocationId.newInitializing(allocationId))
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(shard)))
            .build();

        final var listener = new ShardMovementListenerTests.RecordingListener();
        assertThatLogger(
            () -> applyStartedShardsUntilNoChange(clusterState, createAllocationService(Settings.EMPTY, listener)),
            ShardChangesObserver.class,
            new MockLog.SeenEventExpectation(
                "Should log shard moving",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] is relocating (" + MoveType.CANNOT_REMAIN.reason() + ") from [node-1] to [node-2]"
            ),
            new MockLog.SeenEventExpectation(
                "Should log shard starting",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] started from " + RecoverySource.Type.PEER + " on node [node-2]"
            )
        );

        // the event carries the same reason
        final var relocation = listener.events.stream().filter(e -> e.getFirst().equals("relocationStarted")).findFirst().orElseThrow();
        assertThat(((ShardRouting) relocation.get(1)).currentNodeId(), equalTo("node-1"));
        assertThat(((ShardRouting) relocation.get(2)).currentNodeId(), equalTo("node-2"));
        assertThat(((MoveReason) relocation.get(3)).type(), equalTo(MoveType.CANNOT_REMAIN));
        assertThat(listener.events.getLast().getFirst(), equalTo("relocationCompleted"));
    }

    public void testLogShardFailureAndPromotion() {

        var allocationId1 = randomUUID();
        var allocationId2 = randomUUID();
        var indexName = randomIdentifier();
        var indexMetadata = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putInSyncAllocationIds(0, Set.of(allocationId1, allocationId2))
            .build();

        ShardRouting shard1 = shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-1", true, ShardRoutingState.STARTED)
            .withAllocationId(AllocationId.newInitializing(allocationId1))
            .build();

        ShardRouting shard2 = shardRoutingBuilder(new ShardId(indexMetadata.getIndex(), 0), "node-2", false, ShardRoutingState.STARTED)
            .withAllocationId(AllocationId.newInitializing(allocationId1))
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node-2"))) // node-1 left the cluster
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(RoutingTable.builder().add(IndexRoutingTable.builder(indexMetadata.getIndex()).addShard(shard1).addShard(shard2)))
            .build();

        assertThatLogger(
            () -> createAllocationService().disassociateDeadNodes(clusterState, true, "node-1 left cluster"),
            ShardChangesObserver.class,
            new MockLog.SeenEventExpectation(
                "Should log shard moving",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][R] is promoted to primary on [node-2]"
            ),
            new MockLog.SeenEventExpectation(
                "Should log shard starting",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] has failed on [node-1]: NODE_LEFT"
            )
        );
    }

    public void testEventsForwardedToListener() {
        final var listener = new ShardMovementListenerTests.RecordingListener();
        final var observer = new ShardChangesObserver(listener);

        final var shardId = new ShardId("test-index", "_na_", 0);
        final var primary = randomBoolean();
        final var unassignedShard = shardRoutingBuilder(shardId, null, primary, ShardRoutingState.UNASSIGNED).build();
        final var initializingShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.INITIALIZING).build();
        final var startedShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.STARTED).build();
        final var relocatingShard = shardRoutingBuilder(shardId, "node-1", primary, ShardRoutingState.RELOCATING).withRelocatingNodeId(
            "node-2"
        ).build();
        final var targetShard = shardRoutingBuilder(shardId, "node-2", primary, ShardRoutingState.INITIALIZING).build();
        final var reason = new MoveReason(randomFrom(MoveType.values()), randomBoolean() ? null : randomIdentifier());
        final var unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "boom");

        observer.shardInitialized(unassignedShard, initializingShard);
        observer.shardStarted(initializingShard, startedShard);
        observer.relocationStarted(startedShard, targetShard, reason);
        observer.relocationCompleted(relocatingShard);
        observer.shardFailed(startedShard, unassignedInfo);

        assertThat(
            listener.events,
            contains(
                List.of("shardInitialized", unassignedShard, initializingShard),
                List.of("shardStarted", initializingShard, startedShard),
                List.of("relocationStarted", startedShard, targetShard, reason),
                List.of("relocationCompleted", relocatingShard),
                List.of("shardFailed", startedShard, unassignedInfo)
            )
        );
    }
}
