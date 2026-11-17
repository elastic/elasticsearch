/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoveryPriority;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * The decider pins the primary of a parquet index and refuses its replicas, and leaves every other index alone. The local files of a
 * parquet shard do not move with the shard, so every hook that decides where a shard goes must say no.
 */
public class ParquetAllocationDeciderTests extends ESAllocationTestCase {

    private static final DiscoveryNode NODE_A = newNode("node-a", Set.of(DiscoveryNodeRole.DATA_ROLE));
    private static final DiscoveryNode NODE_B = newNode("node-b", Set.of(DiscoveryNodeRole.DATA_ROLE));

    private static final Index PARQUET_INDEX = new Index("parquet-index", "parquet-uuid");
    private static final Index NATIVE_INDEX = new Index("native-index", "native-uuid");

    private final ParquetAllocationDecider decider = new ParquetAllocationDecider();
    private final RoutingAllocation allocation = allocation();

    private static IndexMetadata index(Index index, boolean parquet) {
        final var settings = indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID());
        if (parquet) {
            settings.put("index.storage_format", "parquet");
        }
        return IndexMetadata.builder(index.getName()).settings(settings).build();
    }

    private static RoutingAllocation allocation() {
        final ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(NODE_A).add(NODE_B).build())
            .metadata(Metadata.builder().put(index(PARQUET_INDEX, true), false).put(index(NATIVE_INDEX, false), false))
            .build();
        final RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(state).build();
        allocation.debugDecision(true);
        return allocation;
    }

    private static RoutingNode node(DiscoveryNode node) {
        return RoutingNodesHelper.routingNode(node.getId(), node);
    }

    private static ShardRouting started(Index index, String nodeId, boolean primary) {
        return TestShardRouting.newShardRouting(new ShardId(index, 0), nodeId, primary, ShardRoutingState.STARTED);
    }

    private static ShardRouting unassigned(Index index, boolean primary) {
        return ShardRouting.newUnassigned(
            new ShardId(index, 0),
            primary,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"),
            ShardRouting.Role.DEFAULT,
            primary ? RecoveryPriority.UNASSIGNED_NEW_PRIMARY : RecoveryPriority.UNASSIGNED_EXPECTED
        );
    }

    private static void assertNo(Decision decision, String explanation) {
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.label(), equalTo(ParquetAllocationDecider.NAME));
        assertThat(decision.getExplanation(), containsString(explanation));
    }

    private static void assertYes(Decision decision) {
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.label(), equalTo(ParquetAllocationDecider.NAME));
    }

    public void testAStartedPrimaryCannotMoveToAnotherNode() {
        final ShardRouting primary = started(PARQUET_INDEX, NODE_A.getId(), true);

        assertNo(decider.canAllocate(primary, node(NODE_B), allocation), "cannot move");
        assertYes(decider.canAllocate(primary, node(NODE_A), allocation));
    }

    public void testAnUnassignedPrimaryCanBeAllocatedToAnyNode() {
        // a new index needs a node, and after a restart the gateway allocator picks the node that holds the copy
        final ShardRouting primary = unassigned(PARQUET_INDEX, true);

        assertYes(decider.canAllocate(primary, node(NODE_A), allocation));
        assertYes(decider.canAllocate(primary, node(NODE_B), allocation));
    }

    public void testAReplicaIsRefusedEverywhere() {
        for (ShardRouting replica : new ShardRouting[] {
            unassigned(PARQUET_INDEX, false),
            started(PARQUET_INDEX, NODE_B.getId(), false) }) {
            assertNo(decider.canAllocate(replica, node(NODE_A), allocation), "does not support replicas");
            assertNo(decider.canAllocate(replica, node(NODE_B), allocation), "does not support replicas");
        }
    }

    public void testTheBalancerIsNotAskedToMoveAnything() {
        assertNo(decider.canRebalance(started(PARQUET_INDEX, NODE_A.getId(), true), allocation), "cannot move");
        assertNo(decider.canRebalance(started(PARQUET_INDEX, NODE_B.getId(), false), allocation), "does not support replicas");
    }

    public void testANodeReplacementCannotForceThePrimaryAway() {
        // the default of this hook is yes, and it overrides every no of canAllocate when the node of the shard is being replaced
        final ShardRouting primary = started(PARQUET_INDEX, NODE_A.getId(), true);

        assertNo(decider.canForceAllocateDuringReplace(primary, node(NODE_B), allocation), "cannot move");
    }

    public void testAutoExpandNeverAddsAReplica() {
        assertNo(decider.shouldAutoExpandToNode(allocation.metadata().indexMetadata(PARQUET_INDEX), NODE_B, allocation), "replicas");
    }

    public void testOtherIndicesAreNotAffected() {
        final ShardRouting primary = started(NATIVE_INDEX, NODE_A.getId(), true);
        final ShardRouting replica = unassigned(NATIVE_INDEX, false);
        final IndexMetadata nativeIndex = allocation.metadata().indexMetadata(NATIVE_INDEX);

        for (Decision decision : new Decision[] {
            decider.canAllocate(primary, node(NODE_B), allocation),
            decider.canAllocate(replica, node(NODE_B), allocation),
            decider.canRebalance(primary, allocation),
            decider.canForceAllocateDuringReplace(primary, node(NODE_B), allocation),
            decider.shouldAutoExpandToNode(nativeIndex, NODE_B, allocation) }) {
            assertYes(decision);
        }
    }
}
