/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class StartedShardsRoutingTests extends ESAllocationTestCase {
    public void testStartedShardsMatching() {
        AllocationService allocation = createAllocationService();

        logger.info("--> building initial cluster state");
        AllocationId allocationId = AllocationId.newRelocation(AllocationId.newInitializing());
        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(2)
            .numberOfReplicas(0)
            .putInSyncAllocationIds(1, Collections.singleton(allocationId.getId()))
            .build();
        final Index index = indexMetadata.getIndex();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .metadata(Metadata.builder().put(indexMetadata, false));

        final ShardRouting initShard = TestShardRouting.newShardRouting(
            new ShardId(index, 0),
            "node1",
            true,
            ShardRoutingState.INITIALIZING
        );
        final ShardRouting relocatingShard = shardRoutingBuilder(new ShardId(index, 1), "node1", true, ShardRoutingState.RELOCATING)
            .withRelocatingNodeId("node2")
            .withAllocationId(allocationId)
            .build();
        stateBuilder.routingTable(
            RoutingTable.builder()
                .add(
                    IndexRoutingTable.builder(index)
                        .addIndexShard(IndexShardRoutingTable.builder(initShard.shardId()).addShard(initShard))
                        .addIndexShard(IndexShardRoutingTable.builder(relocatingShard.shardId()).addShard(relocatingShard))
                )
                .build()
        );

        ClusterState state = stateBuilder.build();

        logger.info("--> test starting of shard");

        ClusterState newState = startShardsAndReroute(allocation, state, initShard);
        assertThat("failed to start " + initShard + "\ncurrent routing table:" + newState.routingTable(), newState, not(equalTo(state)));
        assertTrue(
            initShard + "isn't started \ncurrent routing table:" + newState.routingTable(),
            newState.routingTable().index("test").shard(initShard.id()).allShardsStarted()
        );
        state = newState;

        logger.info("--> testing starting of relocating shards");
        newState = startShardsAndReroute(allocation, state, relocatingShard.getTargetRelocatingShard());
        assertThat(
            "failed to start " + relocatingShard + "\ncurrent routing table:" + newState.routingTable(),
            newState,
            not(equalTo(state))
        );
        ShardRouting shardRouting = newState.routingTable().index("test").shard(relocatingShard.id()).shard(0);
        assertThat(shardRouting.state(), equalTo(ShardRoutingState.STARTED));
        assertThat(shardRouting.currentNodeId(), equalTo("node2"));
        assertThat(shardRouting.relocatingNodeId(), nullValue());
    }

    public void testRelocatingPrimariesWithInitializingReplicas() {
        AllocationService allocation = createAllocationService();

        logger.info("--> building initial cluster state");
        AllocationId primaryId = AllocationId.newRelocation(AllocationId.newInitializing());
        AllocationId replicaId = AllocationId.newInitializing();
        boolean relocatingReplica = randomBoolean();
        if (relocatingReplica) {
            replicaId = AllocationId.newRelocation(replicaId);
        }

        final IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putInSyncAllocationIds(
                0,
                relocatingReplica ? Sets.newHashSet(primaryId.getId(), replicaId.getId()) : Sets.newHashSet(primaryId.getId())
            )
            .build();
        final Index index = indexMetadata.getIndex();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4")))
            .metadata(Metadata.builder().put(indexMetadata, false));

        final ShardRouting relocatingPrimary = shardRoutingBuilder(new ShardId(index, 0), "node1", true, ShardRoutingState.RELOCATING)
            .withRelocatingNodeId("node2")
            .withAllocationId(primaryId)
            .build();
        final ShardRouting replica = shardRoutingBuilder(
            new ShardId(index, 0),
            "node3",
            false,
            relocatingReplica ? ShardRoutingState.RELOCATING : ShardRoutingState.INITIALIZING
        ).withRelocatingNodeId(relocatingReplica ? "node4" : null).withAllocationId(replicaId).build();

        stateBuilder.routingTable(
            RoutingTable.builder()
                .add(
                    IndexRoutingTable.builder(index)
                        .addIndexShard(
                            new IndexShardRoutingTable.Builder(relocatingPrimary.shardId()).addShard(relocatingPrimary).addShard(replica)
                        )
                )
                .build()
        );

        ClusterState state = stateBuilder.build();

        logger.info("--> test starting of relocating primary shard with initializing / relocating replica");
        ClusterState newState = startShardsAndReroute(allocation, state, relocatingPrimary.getTargetRelocatingShard());
        assertNotEquals(newState, state);
        assertTrue(newState.routingTable().index("test").allPrimaryShardsActive());
        ShardRouting startedReplica = newState.routingTable().index("test").shard(0).replicaShards().get(0);
        if (relocatingReplica) {
            assertTrue(startedReplica.relocating());
            assertEquals(replica.currentNodeId(), startedReplica.currentNodeId());
            assertEquals(replica.relocatingNodeId(), startedReplica.relocatingNodeId());
            assertEquals(replica.allocationId().getId(), startedReplica.allocationId().getId());
            assertNotEquals(replica.allocationId().getRelocationId(), startedReplica.allocationId().getRelocationId());
        } else {
            assertTrue(startedReplica.initializing());
            assertEquals(replica.currentNodeId(), startedReplica.currentNodeId());
            assertNotEquals(replica.allocationId().getId(), startedReplica.allocationId().getId());
        }

        logger.info("--> test starting of relocating primary shard together with initializing / relocating replica");
        List<ShardRouting> startedShards = new ArrayList<>();
        startedShards.add(relocatingPrimary.getTargetRelocatingShard());
        startedShards.add(relocatingReplica ? replica.getTargetRelocatingShard() : replica);
        Collections.shuffle(startedShards, random());
        newState = startShardsAndReroute(allocation, state, startedShards);
        assertNotEquals(newState, state);
        assertTrue(newState.routingTable().index("test").shard(0).allShardsStarted());
    }
}
