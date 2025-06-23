/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexVersion;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class InSyncAllocationIdTests extends ESAllocationTestCase {

    private AllocationService allocation;
    private ShardStateAction.ShardFailedClusterStateTaskExecutor failedClusterStateTaskExecutor;

    @Before
    public void setupAllocationService() {
        allocation = createAllocationService();
        failedClusterStateTaskExecutor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocation, null);
    }

    public void testInSyncAllocationIdsUpdated() {
        logger.info("creating an index with 1 shard, 2 replicas");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(2))
            // add index metadata where we have no routing nodes to check that allocation ids are not removed
            .put(
                IndexMetadata.builder("test-old")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(2)
                    .putInSyncAllocationIds(0, Set.of("x", "y"))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .addAsRecovery(metadata.getProject().index("test-old"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("adding three nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(0));
        assertThat(clusterState.metadata().getProject().index("test-old").inSyncAllocationIds(0), equalTo(Set.of("x", "y")));

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), STARTED).get(0).allocationId().getId(),
            equalTo(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).iterator().next())
        );
        assertThat(clusterState.metadata().getProject().index("test-old").inSyncAllocationIds(0), equalTo(Set.of("x", "y")));

        logger.info("start replica shards");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(3));

        logger.info("remove a node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(3));

        logger.info("remove all remaining nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node2").remove("node3"))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo(6));
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(3));

        // force empty primary
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node1"))).build();
        clusterState = allocation.reroute(
            clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)),
            false,
            false,
            false,
            ActionListener.noop()
        ).clusterState();

        // check that in-sync allocation ids are reset by forcing an empty primary
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(0));

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(1));

        logger.info("fail primary shard");
        ShardRouting startedPrimary = shardsWithState(clusterState.getRoutingNodes(), STARTED).get(0);
        clusterState = allocation.applyFailedShards(clusterState, List.of(new FailedShard(startedPrimary, null, null, true)), List.of());

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(0));
        assertEquals(
            Collections.singleton(startedPrimary.allocationId().getId()),
            clusterState.metadata().getProject().index("test").inSyncAllocationIds(0)
        );
    }

    /**
     * Assume following scenario: indexing request is written to primary, but fails to be replicated to active replica.
     * The primary instructs master to fail replica before acknowledging write to client. In the meanwhile, the node of the replica was
     * removed from the cluster (disassociateDeadNodes). This means that the ShardRouting of the replica was failed, but it's allocation
     * id is still part of the in-sync set. We have to make sure that the failShard request from the primary removes the allocation id
     * from the in-sync set.
     */
    public void testDeadNodesBeforeReplicaFailed() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        logger.info("remove replica node");
        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(2));

        logger.info("fail replica (for which there is no shard routing in the CS anymore)");
        assertNull(clusterState.getRoutingNodes().getByAllocationId(replicaShard.shardId(), replicaShard.allocationId().getId()));
        ShardStateAction.ShardFailedClusterStateTaskExecutor failedClusterStateTaskExecutor =
            new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocation, null);
        long primaryTerm = clusterState.metadata().getProject().index("test").primaryTerm(0);
        clusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterState,
            failedClusterStateTaskExecutor,
            List.of(
                new FailedShardUpdateTask(
                    new FailedShardEntry(
                        shardRoutingTable.shardId(),
                        replicaShard.allocationId().getId(),
                        primaryTerm,
                        "dummy",
                        null,
                        true
                    ),
                    createTestListener()
                )
            )
        );

        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(1));
    }

    /**
     * Assume following scenario: indexing request is written to primary, but fails to be replicated to active replica.
     * The primary instructs master to fail replica before acknowledging write to client. In the meanwhile, primary fails for an unrelated
     * reason. Master now batches both requests to fail primary and replica. We have to make sure that only the allocation id of the primary
     * is kept in the in-sync allocation set before we acknowledge request to client. Otherwise we would acknowledge a write that made it
     * into the primary but not the replica but the replica is still considered non-stale.
     */
    public void testPrimaryFailureBatchedWithReplicaFailure() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting primaryShard = shardRoutingTable.primaryShard();
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);

        long primaryTerm = clusterState.metadata().getProject().index("test").primaryTerm(0);

        List<FailedShardUpdateTask> failureEntries = new ArrayList<>();
        failureEntries.add(
            new FailedShardUpdateTask(
                new FailedShardEntry(shardRoutingTable.shardId(), primaryShard.allocationId().getId(), 0L, "dummy", null, true),
                createTestListener()
            )
        );
        failureEntries.add(
            new FailedShardUpdateTask(
                new FailedShardEntry(shardRoutingTable.shardId(), replicaShard.allocationId().getId(), primaryTerm, "dummy", null, true),
                createTestListener()
            )
        );
        Collections.shuffle(failureEntries, random());
        logger.info("Failing {}", failureEntries);

        clusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterState,
            failedClusterStateTaskExecutor,
            failureEntries
        );

        assertThat(
            clusterState.metadata().getProject().index("test").inSyncAllocationIds(0),
            equalTo(Collections.singleton(primaryShard.allocationId().getId()))
        );

        // resend shard failures to check if they are ignored
        clusterState = ClusterStateTaskExecutorUtils.executeIgnoringFailures(clusterState, failedClusterStateTaskExecutor, failureEntries);

        assertThat(
            clusterState.metadata().getProject().index("test").inSyncAllocationIds(0),
            equalTo(Collections.singleton(primaryShard.allocationId().getId()))
        );
    }

    /**
     * Prevent set of inSyncAllocationIds to grow unboundedly. This can happen for example if we don't write to a primary
     * but repeatedly shut down nodes that have active replicas.
     * We use number_of_replicas + 1 (= possible active shard copies) to bound the inSyncAllocationIds set
     */
    public void testInSyncIdsNotGrowingWithoutBounds() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        Set<String> inSyncSet = clusterState.metadata().getProject().index("test").inSyncAllocationIds(0);
        assertThat(inSyncSet.size(), equalTo(2));

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting primaryShard = shardRoutingTable.primaryShard();
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);

        logger.info("remove a node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metadata().getProject().index("test").inSyncAllocationIds(0));

        // check that inSyncAllocationIds can not grow without bounds
        for (int i = 0; i < 5; i++) {
            logger.info("add back node");
            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(replicaShard.currentNodeId())))
                .build();
            clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

            logger.info("start replica shards");
            clusterState = startInitializingShardsAndReroute(allocation, clusterState);

            logger.info("remove the node");
            clusterState = ClusterState.builder(clusterState)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(replicaShard.currentNodeId()))
                .build();
            clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");
        }

        // in-sync allocation set is bounded
        Set<String> newInSyncSet = clusterState.metadata().getProject().index("test").inSyncAllocationIds(0);
        assertThat(newInSyncSet.size(), equalTo(2));
        // only allocation id of replica was changed
        assertFalse(Sets.haveEmptyIntersection(inSyncSet, newInSyncSet));
        assertThat(newInSyncSet, hasItem(primaryShard.allocationId().getId()));
    }

    /**
     * Only trim set of allocation ids when the set grows
     */
    public void testInSyncIdsNotTrimmedWhenNotGrowing() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        Set<String> inSyncSet = clusterState.metadata().getProject().index("test").inSyncAllocationIds(0);
        assertThat(inSyncSet.size(), equalTo(2));

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting primaryShard = shardRoutingTable.primaryShard();
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);

        logger.info("remove replica node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metadata().getProject().index("test").inSyncAllocationIds(0));

        logger.info("remove primary node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(primaryShard.currentNodeId()))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metadata().getProject().index("test").inSyncAllocationIds(0));

        logger.info("decrease number of replicas to 0");
        clusterState = ClusterState.builder(clusterState)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState.routingTable())
                    .updateNumberOfReplicas(0, new String[] { "test" })
                    .build()
            )
            .metadata(Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(0, new String[] { "test" }))
            .build();

        logger.info("add back node 1");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.routingTable().index("test").shard(0).assignedShards().size(), equalTo(1));
        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metadata().getProject().index("test").inSyncAllocationIds(0));

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metadata().getProject().index("test").inSyncAllocationIds(0));
    }

    /**
     * Don't remove allocation id of failed active primary if there is no replica to promote as primary.
     */
    public void testPrimaryAllocationIdNotRemovedFromInSyncSetWhenNoFailOver() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        Set<String> inSyncSet = clusterState.metadata().getProject().index("test").inSyncAllocationIds(0);
        assertThat(inSyncSet.size(), equalTo(2));

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting primaryShard = shardRoutingTable.primaryShard();
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);

        logger.info("remove replica node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.disassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metadata().getProject().index("test").inSyncAllocationIds(0));

        logger.info("fail primary shard");
        clusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterState,
            failedClusterStateTaskExecutor,
            List.of(
                new FailedShardUpdateTask(
                    new FailedShardEntry(shardRoutingTable.shardId(), primaryShard.allocationId().getId(), 0L, "dummy", null, true),
                    createTestListener()
                )
            )
        );

        assertThat(clusterState.routingTable().index("test").shard(0).assignedShards().size(), equalTo(0));
        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metadata().getProject().index("test").inSyncAllocationIds(0));
    }

    private ClusterState createOnePrimaryOneReplicaClusterState(AllocationService allocation) {
        logger.info("creating an index with 1 shard, 1 replica");
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = allocation.reroute(clusterState, "reroute", ActionListener.noop());

        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(0));

        logger.info("start primary shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);

        assertThat(shardsWithState(clusterState.getRoutingNodes(), STARTED).size(), equalTo(1));
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(1));
        assertThat(
            shardsWithState(clusterState.getRoutingNodes(), STARTED).get(0).allocationId().getId(),
            equalTo(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).iterator().next())
        );

        logger.info("start replica shard");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        assertThat(clusterState.metadata().getProject().index("test").inSyncAllocationIds(0).size(), equalTo(2));
        return clusterState;
    }

    private static <T> ActionListener<T> createTestListener() {
        return ActionTestUtils.assertNoFailureListener(t -> {});
    }
}
