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

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
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
import org.elasticsearch.test.ESAllocationTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
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
        allocationService = createAllocationService(settingsBuilder()
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
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metaData(metaData).routingTable(routingTable).build();
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
        Map<ShardStateAction.ShardRoutingEntry, Boolean> taskResultMap =
            failingTasks.stream().collect(Collectors.toMap(Function.identity(), task -> false));
        taskResultMap.putAll(nonExistentTasks.stream().collect(Collectors.toMap(Function.identity(), task -> true)));
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
        return toTasks(shardsToFail, indexUUID, reason);
    }

    private List<ShardStateAction.ShardRoutingEntry> createNonExistentShards(ClusterState currentState, String reason) {
        // add shards from a non-existent index
        MetaData nonExistentMetaData =
            MetaData.builder()
                .put(IndexMetaData.builder("non-existent").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(numberOfReplicas))
                .build();
        RoutingTable routingTable = RoutingTable.builder().addAsNew(nonExistentMetaData.index("non-existent")).build();
        String nonExistentIndexUUID = nonExistentMetaData.index("non-existent").getIndexUUID();

        List<ShardStateAction.ShardRoutingEntry> existingShards = createExistingShards(currentState, reason);
        List<ShardStateAction.ShardRoutingEntry> shardsWithMismatchedAllocationIds = new ArrayList<>();
        for (ShardStateAction.ShardRoutingEntry existingShard : existingShards) {
            ShardRouting sr = existingShard.getShardRouting();
            ShardRouting nonExistentShardRouting =
                TestShardRouting.newShardRouting(sr.index(), sr.id(), sr.currentNodeId(), sr.relocatingNodeId(), sr.restoreSource(), sr.primary(), sr.state(), sr.version());
            shardsWithMismatchedAllocationIds.add(new ShardStateAction.ShardRoutingEntry(nonExistentShardRouting, existingShard.indexUUID, existingShard.message, existingShard.failure));
        }

        List<ShardStateAction.ShardRoutingEntry> tasks = new ArrayList<>();
        tasks.addAll(toTasks(routingTable.allShards(), nonExistentIndexUUID, reason));
        tasks.addAll(shardsWithMismatchedAllocationIds);
        return tasks;
    }

    private static void assertTasksSuccessful(
        List<ShardStateAction.ShardRoutingEntry> tasks,
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result,
        ClusterState clusterState,
        boolean clusterStateChanged
    ) {
        Map<ShardStateAction.ShardRoutingEntry, Boolean> taskResultMap =
            tasks.stream().collect(Collectors.toMap(Function.identity(), task -> true));
        assertTaskResults(taskResultMap, result, clusterState, clusterStateChanged);
    }

    private static void assertTaskResults(
        Map<ShardStateAction.ShardRoutingEntry, Boolean> taskResultMap,
        ClusterStateTaskExecutor.BatchResult<ShardStateAction.ShardRoutingEntry> result,
        ClusterState clusterState,
        boolean clusterStateChanged
    ) {
        // there should be as many task results as tasks
        assertEquals(taskResultMap.size(), result.executionResults.size());

        for (Map.Entry<ShardStateAction.ShardRoutingEntry, Boolean> entry : taskResultMap.entrySet()) {
            // every task should have a corresponding task result
            assertTrue(result.executionResults.containsKey(entry.getKey()));

            // the task results are as expected
            assertEquals(entry.getValue(), result.executionResults.get(entry.getKey()).isSuccess());
        }

        // every shard that we requested to be successfully failed is
        // gone
        List<ShardRouting> shards = clusterState.getRoutingTable().allShards();
        for (Map.Entry<ShardStateAction.ShardRoutingEntry, Boolean> entry : taskResultMap.entrySet()) {
            if (entry.getValue()) {
                for (ShardRouting shard : shards) {
                    if (entry.getKey().getShardRouting().allocationId() != null) {
                        assertThat(shard.allocationId(), not(equalTo(entry.getKey().getShardRouting().allocationId())));
                    }
                }
            }
        }

        if (clusterStateChanged) {
            assertNotSame(clusterState, result.resultingState);
        } else {
            assertSame(clusterState, result.resultingState);
        }
    }

    private static List<ShardStateAction.ShardRoutingEntry> toTasks(List<ShardRouting> shards, String indexUUID, String message) {
        return shards
            .stream()
            .map(shard -> new ShardStateAction.ShardRoutingEntry(shard, indexUUID, message, new CorruptIndexException("simulated", indexUUID)))
            .collect(Collectors.toList());
    }

}
