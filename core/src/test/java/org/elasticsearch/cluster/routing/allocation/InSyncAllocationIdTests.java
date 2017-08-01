package org.elasticsearch.cluster.routing.allocation;

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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction.ShardEntry;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
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
        failedClusterStateTaskExecutor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocation, null, logger);
    }

    public void testInSyncAllocationIdsUpdated() {
        logger.info("creating an index with 1 shard, 2 replicas");
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
                // add index metadata where we have no routing nodes to check that allocation ids are not removed
                .put(IndexMetaData.builder("test-old").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2)
                        .putInSyncAllocationIds(0, new HashSet<>(Arrays.asList("x", "y"))))
                .build();
        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("adding three nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(
                newNode("node1")).add(newNode("node2")).add(newNode("node3"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(0));
        assertThat(clusterState.metaData().index("test-old").inSyncAllocationIds(0), equalTo(new HashSet<>(Arrays.asList("x", "y"))));

        logger.info("start primary shard");
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));

        assertThat(clusterState.getRoutingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(1));
        assertThat(clusterState.getRoutingTable().shardsWithState(STARTED).get(0).allocationId().getId(),
                equalTo(clusterState.metaData().index("test").inSyncAllocationIds(0).iterator().next()));
        assertThat(clusterState.metaData().index("test-old").inSyncAllocationIds(0), equalTo(new HashSet<>(Arrays.asList("x", "y"))));

        logger.info("start replica shards");
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));

        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(3));

        logger.info("remove a node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .remove("node1"))
                .build();
        clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(3));

        logger.info("remove all remaining nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .remove("node2").remove("node3"))
                .build();
        clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertThat(clusterState.getRoutingTable().shardsWithState(UNASSIGNED).size(), equalTo(3));
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(3));

        // force empty primary
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .add(newNode("node1")))
            .build();
        clusterState = allocation.reroute(clusterState,
            new AllocationCommands(new AllocateEmptyPrimaryAllocationCommand("test", 0, "node1", true)), false, false)
            .getClusterState();

        // check that in-sync allocation ids are reset by forcing an empty primary
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(0));

        logger.info("start primary shard");
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));

        assertThat(clusterState.getRoutingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(1));

        logger.info("fail primary shard");
        ShardRouting startedPrimary = clusterState.getRoutingNodes().shardsWithState(STARTED).get(0);
        clusterState = allocation.applyFailedShard(clusterState, startedPrimary);

        assertThat(clusterState.getRoutingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertEquals(Collections.singleton(startedPrimary.allocationId().getId()),
            clusterState.metaData().index("test").inSyncAllocationIds(0));
    }

    /**
     * Assume following scenario: indexing request is written to primary, but fails to be replicated to active replica.
     * The primary instructs master to fail replica before acknowledging write to client. In the meanwhile, the node of the replica was
     * removed from the cluster (deassociateDeadNodes). This means that the ShardRouting of the replica was failed, but it's allocation
     * id is still part of the in-sync set. We have to make sure that the failShard request from the primary removes the allocation id
     * from the in-sync set.
     */
    public void testDeadNodesBeforeReplicaFailed() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        logger.info("remove replica node");
        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(2));

        logger.info("fail replica (for which there is no shard routing in the CS anymore)");
        assertNull(clusterState.getRoutingNodes().getByAllocationId(replicaShard.shardId(), replicaShard.allocationId().getId()));
        ShardStateAction.ShardFailedClusterStateTaskExecutor failedClusterStateTaskExecutor =
            new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocation, null, logger);
        long primaryTerm = clusterState.metaData().index("test").primaryTerm(0);
        clusterState = failedClusterStateTaskExecutor.execute(clusterState, Arrays.asList(
                new ShardEntry(shardRoutingTable.shardId(), replicaShard.allocationId().getId(), primaryTerm, "dummy", null))
            ).resultingState;

        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(1));
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

        long primaryTerm = clusterState.metaData().index("test").primaryTerm(0);

        List<ShardEntry> failureEntries = new ArrayList<>();
        failureEntries.add(new ShardEntry(
            shardRoutingTable.shardId(), primaryShard.allocationId().getId(), 0L, "dummy", null));
        failureEntries.add(new ShardEntry(
            shardRoutingTable.shardId(), replicaShard.allocationId().getId(), primaryTerm, "dummy", null));
        Collections.shuffle(failureEntries, random());
        logger.info("Failing {}", failureEntries);

        clusterState = failedClusterStateTaskExecutor.execute(clusterState, failureEntries).resultingState;

        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0),
            equalTo(Collections.singleton(primaryShard.allocationId().getId())));

        // resend shard failures to check if they are ignored
        clusterState = failedClusterStateTaskExecutor.execute(clusterState, failureEntries).resultingState;

        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0),
            equalTo(Collections.singleton(primaryShard.allocationId().getId())));
    }

    /**
     * Prevent set of inSyncAllocationIds to grow unboundedly. This can happen for example if we don't write to a primary
     * but repeatedly shut down nodes that have active replicas.
     * We use number_of_replicas + 1 (= possible active shard copies) to bound the inSyncAllocationIds set
     */
    public void testInSyncIdsNotGrowingWithoutBounds() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        Set<String> inSyncSet = clusterState.metaData().index("test").inSyncAllocationIds(0);
        assertThat(inSyncSet.size(), equalTo(2));

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting primaryShard = shardRoutingTable.primaryShard();
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);

        logger.info("remove a node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metaData().index("test").inSyncAllocationIds(0));

        // check that inSyncAllocationIds can not grow without bounds
        for (int i = 0; i < 5; i++) {
            logger.info("add back node");
            clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode(replicaShard.currentNodeId())))
                .build();
            clusterState = allocation.reroute(clusterState, "reroute");

            logger.info("start replica shards");
            clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));

            logger.info("remove the node");
            clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .remove(replicaShard.currentNodeId()))
                .build();
            clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");
            }

        // in-sync allocation set is bounded
        Set<String> newInSyncSet = clusterState.metaData().index("test").inSyncAllocationIds(0);
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

        Set<String> inSyncSet = clusterState.metaData().index("test").inSyncAllocationIds(0);
        assertThat(inSyncSet.size(), equalTo(2));

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting primaryShard = shardRoutingTable.primaryShard();
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);

        logger.info("remove replica node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metaData().index("test").inSyncAllocationIds(0));

        logger.info("remove primary node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .remove(primaryShard.currentNodeId()))
            .build();
        clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metaData().index("test").inSyncAllocationIds(0));

        logger.info("decrease number of replicas to 0");
        clusterState = ClusterState.builder(clusterState)
            .routingTable(RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(0, "test").build())
            .metaData(MetaData.builder(clusterState.metaData()).updateNumberOfReplicas(0, "test")).build();

        logger.info("add back node 1");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(
            newNode("node1"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().index("test").shard(0).assignedShards().size(), equalTo(1));
        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metaData().index("test").inSyncAllocationIds(0));

        logger.info("start primary shard");
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metaData().index("test").inSyncAllocationIds(0));
    }

    /**
     * Don't remove allocation id of failed active primary if there is no replica to promote as primary.
     */
    public void testPrimaryAllocationIdNotRemovedFromInSyncSetWhenNoFailOver() throws Exception {
        ClusterState clusterState = createOnePrimaryOneReplicaClusterState(allocation);

        Set<String> inSyncSet = clusterState.metaData().index("test").inSyncAllocationIds(0);
        assertThat(inSyncSet.size(), equalTo(2));

        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index("test").shard(0);
        ShardRouting primaryShard = shardRoutingTable.primaryShard();
        ShardRouting replicaShard = shardRoutingTable.replicaShards().get(0);

        logger.info("remove replica node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
            .remove(replicaShard.currentNodeId()))
            .build();
        clusterState = allocation.deassociateDeadNodes(clusterState, true, "reroute");

        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metaData().index("test").inSyncAllocationIds(0));

        logger.info("fail primary shard");
        clusterState = failedClusterStateTaskExecutor.execute(clusterState, Collections.singletonList(new ShardEntry(
            shardRoutingTable.shardId(), primaryShard.allocationId().getId(), 0L, "dummy", null))).resultingState;

        assertThat(clusterState.routingTable().index("test").shard(0).assignedShards().size(), equalTo(0));
        // in-sync allocation ids should not be updated
        assertEquals(inSyncSet, clusterState.metaData().index("test").inSyncAllocationIds(0));
    }

    private ClusterState createOnePrimaryOneReplicaClusterState(AllocationService allocation) {
        logger.info("creating an index with 1 shard, 1 replica");
        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .addAsNew(metaData.index("test"))
            .build();
        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(
            newNode("node1")).add(newNode("node2"))).build();
        clusterState = allocation.reroute(clusterState, "reroute");

        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(0));

        logger.info("start primary shard");
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));

        assertThat(clusterState.getRoutingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(1));
        assertThat(clusterState.getRoutingTable().shardsWithState(STARTED).get(0).allocationId().getId(),
            equalTo(clusterState.metaData().index("test").inSyncAllocationIds(0).iterator().next()));

        logger.info("start replica shard");
        clusterState = allocation.applyStartedShards(clusterState, clusterState.getRoutingNodes().shardsWithState(INITIALIZING));
        assertThat(clusterState.metaData().index("test").inSyncAllocationIds(0).size(), equalTo(2));
        return clusterState;
    }
}
