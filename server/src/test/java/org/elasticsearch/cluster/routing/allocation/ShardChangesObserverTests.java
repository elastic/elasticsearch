/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.elasticsearch.test.MockLog.assertThatLogger;

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

        assertThatLogger(
            () -> applyStartedShardsUntilNoChange(clusterState, createAllocationService()),
            ShardChangesObserver.class,
            new MockLog.SeenEventExpectation(
                "Should log shard moving",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] is relocating (move) from [node-1] to [node-2]"
            ),
            new MockLog.SeenEventExpectation(
                "Should log shard starting",
                ShardChangesObserver.class.getCanonicalName(),
                Level.DEBUG,
                "[" + indexName + "][0][P] started from " + RecoverySource.Type.PEER + " on node [node-2]"
            )
        );
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
}
