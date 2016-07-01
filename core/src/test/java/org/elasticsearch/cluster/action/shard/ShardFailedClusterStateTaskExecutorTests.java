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

package org.elasticsearch.cluster.action.shard;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodeService;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;

public class ShardFailedClusterStateTaskExecutorTests extends ESAllocationTestCase {

    private static final String INDEX = "INDEX";
    private AllocationService allocationService;
    private int numberOfReplicas;
    private MetaData metaData;
    private RoutingTable routingTable;
    private ClusterState clusterState;
    private ShardStateAction.ShardFailedClusterStateTaskExecutor executor;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        allocationService = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 8)
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
            .build());
        numberOfReplicas = randomIntBetween(2, 16);
        metaData = MetaData.builder()
            .put(IndexMetaData.builder(INDEX).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(numberOfReplicas))
            .build();
        routingTable = RoutingTable.builder()
            .addAsNew(metaData.index(INDEX))
            .build();
        clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();
        executor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocationService, null, logger);
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        List<ShardStateAction.ShardRoutingEntry> tasks = Collections.emptyList();
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result =
            executor.execute(clusterState, tasks);
        assertTasksSuccessful(tasks, result, clusterState, false);
    }

    public void testDuplicateFailuresAreOkay() throws Exception {
        String reason = "test duplicate failures are okay";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<ShardStateAction.ShardRoutingEntry> tasks = createExistingShards(currentState, reason);
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result = executor.execute(currentState, tasks);
        assertTasksSuccessful(tasks, result, clusterState, true);
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        String reason = "test non existent shards are marked as successful";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<ShardStateAction.ShardRoutingEntry> tasks = createNonExistentShards(currentState, reason);
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result = executor.execute(clusterState, tasks);
        assertTasksSuccessful(tasks, result, clusterState, false);
    }

    public void testTriviallySuccessfulTasksBatchedWithFailingTasks() throws Exception {
        String reason = "test trivially successful tasks batched with failing tasks";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<ShardStateAction.ShardRoutingEntry> failingTasks = createExistingShards(currentState, reason);
        List<ShardStateAction.ShardRoutingEntry> nonExistentTasks = createNonExistentShards(currentState, reason);
        ShardStateAction.ShardFailedClusterStateTaskExecutor failingExecutor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocationService, null, logger) {
            @Override
            RoutingAllocation.Result applyFailedShards(ClusterState currentState, List<FailedRerouteAllocation.FailedShard> failedShards) {
                throw new RuntimeException("simulated applyFailedShards failure");
            }
        };
        List<ShardStateAction.ShardRoutingEntry> tasks = new ArrayList<>();
        tasks.addAll(failingTasks);
        tasks.addAll(nonExistentTasks);
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result = failingExecutor.execute(currentState, tasks);
        Map<ShardStateAction.ShardRoutingEntry, ClusterStateTaskExecutor.TaskResult> taskResultMap =
            failingTasks.stream().collect(Collectors.toMap(Function.identity(), task -> ClusterStateTaskExecutor.TaskResult.failure(new RuntimeException("simulated applyFailedShards failure"))));
        taskResultMap.putAll(nonExistentTasks.stream().collect(Collectors.toMap(Function.identity(), task -> ClusterStateTaskExecutor.TaskResult.success())));
        assertTaskResults(taskResultMap, result, currentState, false);
    }

    public void testIllegalShardFailureRequests() throws Exception {
        String reason = "test illegal shard failure requests";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<ShardStateAction.ShardRoutingEntry> failingTasks = createExistingShards(currentState, reason);
        List<ShardStateAction.ShardRoutingEntry> tasks = new ArrayList<>();
        for (ShardStateAction.ShardRoutingEntry failingTask : failingTasks) {
            tasks.add(new ShardStateAction.ShardRoutingEntry(failingTask.getShardRouting(), randomInvalidSourceShard(currentState, failingTask.getShardRouting()), failingTask.message, failingTask.failure));
        }
        Map<ShardStateAction.ShardRoutingEntry, ClusterStateTaskExecutor.TaskResult> taskResultMap =
            tasks.stream().collect(Collectors.toMap(
                Function.identity(),
                task -> ClusterStateTaskExecutor.TaskResult.failure(new ShardStateAction.NoLongerPrimaryShardException(task.getShardRouting().shardId(), "source shard [" + task.sourceShardRouting + "] is neither the local allocation nor the primary allocation"))));
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result = executor.execute(currentState, tasks);
        assertTaskResults(taskResultMap, result, currentState, false);
    }

    private ClusterState createClusterStateWithStartedShards(String reason) {
        int numberOfNodes = 1 + numberOfReplicas;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        IntStream.rangeClosed(1, numberOfNodes).mapToObj(node -> newNode("node" + node)).forEach(nodes::put);
        ClusterState stateAfterAddingNode =
            ClusterState.builder(clusterState).nodes(nodes).build();
        RoutingTable afterReroute =
            allocationService.reroute(stateAfterAddingNode, reason).routingTable();
        ClusterState stateAfterReroute = ClusterState.builder(stateAfterAddingNode).routingTable(afterReroute).build();
        RoutingNodes routingNodes = stateAfterReroute.getRoutingNodes();
        RoutingTable afterStart =
            allocationService.applyStartedShards(stateAfterReroute, routingNodes.shardsWithState(ShardRoutingState.INITIALIZING)).routingTable();
        return ClusterState.builder(stateAfterReroute).routingTable(afterStart).build();
    }

    private List<ShardStateAction.ShardRoutingEntry> createExistingShards(ClusterState currentState, String reason) {
        List<ShardRouting> shards = new ArrayList<>();
        GroupShardsIterator shardGroups =
            currentState.routingTable().allAssignedShardsGrouped(new String[] { INDEX }, true);
        for (ShardIterator shardIt : shardGroups) {
            for (ShardRouting shard : shardIt.asUnordered()) {
                shards.add(shard);
            }
        }
        List<ShardRouting> failures = randomSubsetOf(randomIntBetween(1, 1 + shards.size() / 4), shards.toArray(new ShardRouting[0]));
        String indexUUID = metaData.index(INDEX).getIndexUUID();
        int numberOfTasks = randomIntBetween(failures.size(), 2 * failures.size());
        List<ShardRouting> shardsToFail = new ArrayList<>(numberOfTasks);
        for (int i = 0; i < numberOfTasks; i++) {
            shardsToFail.add(randomFrom(failures));
        }
        return toTasks(currentState, shardsToFail, indexUUID, reason);
    }

    private List<ShardStateAction.ShardRoutingEntry> createNonExistentShards(ClusterState currentState, String reason) {
        // add shards from a non-existent index
        String nonExistentIndexUUID = "non-existent";
        Index index = new Index("non-existent", nonExistentIndexUUID);
        List<String> nodeIds = new ArrayList<>();
        for (ObjectCursor<String> nodeId : currentState.nodes().getNodes().keys()) {
            nodeIds.add(nodeId.toString());
        }
        List<ShardRouting> nonExistentShards = new ArrayList<>();
        nonExistentShards.add(nonExistentShardRouting(index, nodeIds, true));
        for (int i = 0; i < numberOfReplicas; i++) {
            nonExistentShards.add(nonExistentShardRouting(index, nodeIds, false));
        }

        List<ShardStateAction.ShardRoutingEntry> existingShards = createExistingShards(currentState, reason);
        List<ShardStateAction.ShardRoutingEntry> shardsWithMismatchedAllocationIds = new ArrayList<>();
        for (ShardStateAction.ShardRoutingEntry existingShard : existingShards) {
            ShardRouting sr = existingShard.getShardRouting();
            ShardRouting nonExistentShardRouting =
                TestShardRouting.newShardRouting(sr.shardId(), sr.currentNodeId(), sr.relocatingNodeId(), sr.restoreSource(), sr.primary(), sr.state());
            shardsWithMismatchedAllocationIds.add(new ShardStateAction.ShardRoutingEntry(nonExistentShardRouting, nonExistentShardRouting, existingShard.message, existingShard.failure));
        }

        List<ShardStateAction.ShardRoutingEntry> tasks = new ArrayList<>();
        nonExistentShards.forEach(shard -> tasks.add(new ShardStateAction.ShardRoutingEntry(shard, shard, reason, new CorruptIndexException("simulated", nonExistentIndexUUID))));
        tasks.addAll(shardsWithMismatchedAllocationIds);
        return tasks;
    }

    private ShardRouting nonExistentShardRouting(Index index, List<String> nodeIds, boolean primary) {
        return TestShardRouting.newShardRouting(new ShardId(index, 0), randomFrom(nodeIds), primary, randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING, ShardRoutingState.STARTED));
    }

    private static void assertTasksSuccessful(
        List<ShardStateAction.ShardRoutingEntry> tasks,
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result,
        ClusterState clusterState,
        boolean clusterStateChanged
    ) {
        Map<ShardStateAction.ShardRoutingEntry, ClusterStateTaskExecutor.TaskResult> taskResultMap =
            tasks.stream().collect(Collectors.toMap(Function.identity(), task -> ClusterStateTaskExecutor.TaskResult.success()));
        assertTaskResults(taskResultMap, result, clusterState, clusterStateChanged);
    }

    private static void assertTaskResults(
        Map<ShardStateAction.ShardRoutingEntry, ClusterStateTaskExecutor.TaskResult> taskResultMap,
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result,
        ClusterState clusterState,
        boolean clusterStateChanged
    ) {
        // there should be as many task results as tasks
        assertEquals(taskResultMap.size(), result.executionResults.size());

        for (Map.Entry<ShardStateAction.ShardRoutingEntry, ClusterStateTaskExecutor.TaskResult> entry : taskResultMap.entrySet()) {
            // every task should have a corresponding task result
            assertTrue(result.executionResults.containsKey(entry.getKey()));

            // the task results are as expected
            assertEquals(entry.getValue().isSuccess(), result.executionResults.get(entry.getKey()).isSuccess());
        }

        List<ShardRouting> shards = clusterState.getRoutingTable().allShards();
        for (Map.Entry<ShardStateAction.ShardRoutingEntry, ClusterStateTaskExecutor.TaskResult> entry : taskResultMap.entrySet()) {
            if (entry.getValue().isSuccess()) {
                // the shard was successfully failed and so should not
                // be in the routing table
                for (ShardRouting shard : shards) {
                    if (entry.getKey().getShardRouting().allocationId() != null) {
                        assertThat(shard.allocationId(), not(equalTo(entry.getKey().getShardRouting().allocationId())));
                    }
                }
            } else {
                // check we saw the expected failure
                ClusterStateTaskExecutor.TaskResult actualResult = result.executionResults.get(entry.getKey());
                assertThat(actualResult.getFailure(), instanceOf(entry.getValue().getFailure().getClass()));
                assertThat(actualResult.getFailure().getMessage(), equalTo(entry.getValue().getFailure().getMessage()));
            }
        }

        if (clusterStateChanged) {
            assertNotSame(clusterState, result.resultingState);
        } else {
            assertSame(clusterState, result.resultingState);
        }
    }

    private static List<ShardStateAction.ShardRoutingEntry> toTasks(ClusterState currentState, List<ShardRouting> shards, String indexUUID, String message) {
        return shards
            .stream()
            .map(shard -> new ShardStateAction.ShardRoutingEntry(shard, randomValidSourceShard(currentState, shard), message, new CorruptIndexException("simulated", indexUUID)))
            .collect(Collectors.toList());
    }

    private static ShardRouting randomValidSourceShard(ClusterState currentState, ShardRouting shardRouting) {
        // for the request node ID to be valid, either the request is
        // from the node the shard is assigned to, or the request is
        // from the node holding the primary shard
        if (randomBoolean()) {
            // request from local node
            return shardRouting;
        } else {
            // request from primary node unless in the case of
            // non-existent shards there is not one and we fallback to
            // the local node
            ShardRouting primaryNodeId = primaryShard(currentState, shardRouting);
            return primaryNodeId != null ? primaryNodeId : shardRouting;
        }
    }

    private static ShardRouting randomInvalidSourceShard(ClusterState currentState, ShardRouting shardRouting) {
        ShardRouting primaryShard = primaryShard(currentState, shardRouting);
        Set<ShardRouting> shards =
            currentState
                .routingTable()
                .allShards()
                .stream()
                .filter(shard -> !shard.isSameAllocation(shardRouting))
                .filter(shard -> !shard.isSameAllocation(primaryShard))
                .collect(Collectors.toSet());
        if (!shards.isEmpty()) {
            return randomSubsetOf(1, shards.toArray(new ShardRouting[0])).get(0);
        } else {
            return
                    TestShardRouting.newShardRouting(shardRouting.shardId(), DiscoveryNodeService.generateNodeId(Settings.EMPTY), randomBoolean(), randomFrom(ShardRoutingState.values()));
        }
    }

    private static ShardRouting primaryShard(ClusterState currentState, ShardRouting shardRouting) {
        IndexShardRoutingTable indexShard = currentState.getRoutingTable().shardRoutingTableOrNull(shardRouting.shardId());
        return indexShard == null ? null : indexShard.primaryShard();
    }
}
