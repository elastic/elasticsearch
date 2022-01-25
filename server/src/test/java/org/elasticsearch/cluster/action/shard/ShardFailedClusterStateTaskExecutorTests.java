/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.action.shard;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.StaleShard;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.contains;

public class ShardFailedClusterStateTaskExecutorTests extends ESAllocationTestCase {

    private static final String INDEX = "INDEX";
    private AllocationService allocationService;
    private int numberOfReplicas;
    private Metadata metadata;
    private RoutingTable routingTable;
    private ClusterState clusterState;
    private ShardStateAction.ShardFailedClusterStateTaskExecutor executor;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocationService = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 8)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .build()
        );
        numberOfReplicas = randomIntBetween(2, 16);
        metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(INDEX)
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(numberOfReplicas)
                    .primaryTerm(0, randomIntBetween(2, 10))
            )
            .build();
        routingTable = RoutingTable.builder().addAsNew(metadata.index(INDEX)).build();
        clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        executor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocationService, null);
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        List<FailedShardUpdateTask> tasks = Collections.emptyList();
        ClusterStateTaskExecutor.ClusterTasksResult<FailedShardUpdateTask> result = executor.execute(clusterState, tasks);
        assertTasksSuccessful(tasks, result, clusterState, false);
    }

    public void testDuplicateFailuresAreOkay() throws Exception {
        String reason = "test duplicate failures are okay";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<FailedShardUpdateTask> tasks = createExistingShards(currentState, reason);
        ClusterStateTaskExecutor.ClusterTasksResult<FailedShardUpdateTask> result = executor.execute(currentState, tasks);
        assertTasksSuccessful(tasks, result, clusterState, true);
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        String reason = "test non existent shards are marked as successful";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<FailedShardUpdateTask> tasks = createNonExistentShards(currentState, reason);
        ClusterStateTaskExecutor.ClusterTasksResult<FailedShardUpdateTask> result = executor.execute(clusterState, tasks);
        assertTasksSuccessful(tasks, result, clusterState, false);
    }

    public void testTriviallySuccessfulTasksBatchedWithFailingTasks() throws Exception {
        String reason = "test trivially successful tasks batched with failing tasks";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<FailedShardUpdateTask> failingTasks = createExistingShards(currentState, reason);
        List<FailedShardUpdateTask> nonExistentTasks = createNonExistentShards(currentState, reason);
        ShardStateAction.ShardFailedClusterStateTaskExecutor failingExecutor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(
            allocationService,
            null
        ) {
            @Override
            ClusterState applyFailedShards(ClusterState currentState, List<FailedShard> failedShards, List<StaleShard> staleShards) {
                throw new RuntimeException("simulated applyFailedShards failure");
            }
        };
        List<FailedShardUpdateTask> tasks = new ArrayList<>();
        tasks.addAll(failingTasks);
        tasks.addAll(nonExistentTasks);
        ClusterStateTaskExecutor.ClusterTasksResult<FailedShardUpdateTask> result = failingExecutor.execute(currentState, tasks);
        List<Tuple<FailedShardUpdateTask, ClusterStateTaskExecutor.TaskResult>> taskResultList = new ArrayList<>();
        for (FailedShardUpdateTask failingTask : failingTasks) {
            taskResultList.add(
                Tuple.tuple(
                    failingTask,
                    ClusterStateTaskExecutor.TaskResult.failure(new RuntimeException("simulated applyFailedShards failure"))
                )
            );
        }
        for (FailedShardUpdateTask nonExistentTask : nonExistentTasks) {
            taskResultList.add(Tuple.tuple(nonExistentTask, ClusterStateTaskExecutor.TaskResult.success()));
        }
        assertTaskResults(taskResultList, result, currentState, false);
    }

    public void testIllegalShardFailureRequests() throws Exception {
        String reason = "test illegal shard failure requests";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<FailedShardUpdateTask> failingTasks = createExistingShards(currentState, reason);
        List<FailedShardUpdateTask> tasks = new ArrayList<>();
        for (FailedShardUpdateTask failingTask : failingTasks) {
            FailedShardEntry entry = failingTask.entry();
            long primaryTerm = currentState.metadata().index(entry.getShardId().getIndex()).primaryTerm(entry.getShardId().id());
            tasks.add(
                new FailedShardUpdateTask(
                    new FailedShardEntry(
                        entry.getShardId(),
                        entry.getAllocationId(),
                        randomIntBetween(1, (int) primaryTerm - 1),
                        entry.message,
                        entry.failure,
                        randomBoolean()
                    ),
                    createTestListener()
                )
            );
        }
        List<Tuple<FailedShardUpdateTask, ClusterStateTaskExecutor.TaskResult>> taskResultList = tasks.stream()
            .map(
                task -> Tuple.tuple(
                    task,
                    ClusterStateTaskExecutor.TaskResult.failure(
                        new ShardStateAction.NoLongerPrimaryShardException(
                            task.entry().getShardId(),
                            "primary term ["
                                + task.entry().primaryTerm
                                + "] did not match current primary term ["
                                + currentState.metadata()
                                    .index(task.entry().getShardId().getIndex())
                                    .primaryTerm(task.entry().getShardId().id())
                                + "]"
                        )
                    )
                )
            )
            .collect(Collectors.toList());
        ClusterStateTaskExecutor.ClusterTasksResult<FailedShardUpdateTask> result = executor.execute(currentState, tasks);
        assertTaskResults(taskResultList, result, currentState, false);
    }

    public void testMarkAsStaleWhenFailingShard() throws Exception {
        final MockAllocationService allocation = createAllocationService();
        ClusterState clusterState = createClusterStateWithStartedShards("test markAsStale");
        clusterState = startInitializingShardsAndReroute(allocation, clusterState);
        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable().index(INDEX).shard(0);
        long primaryTerm = clusterState.metadata().index(INDEX).primaryTerm(0);
        final Set<String> oldInSync = clusterState.metadata().index(INDEX).inSyncAllocationIds(0);
        {
            FailedShardUpdateTask failShardOnly = new FailedShardUpdateTask(
                new FailedShardEntry(shardRoutingTable.shardId(), randomFrom(oldInSync), primaryTerm, "dummy", null, false),
                createTestListener()
            );
            ClusterState appliedState = executor.execute(clusterState, List.of(failShardOnly)).resultingState();
            Set<String> newInSync = appliedState.metadata().index(INDEX).inSyncAllocationIds(0);
            assertThat(newInSync, equalTo(oldInSync));
        }
        {
            final String failedAllocationId = randomFrom(oldInSync);
            FailedShardUpdateTask failAndMarkAsStale = new FailedShardUpdateTask(
                new FailedShardEntry(shardRoutingTable.shardId(), failedAllocationId, primaryTerm, "dummy", null, true),
                createTestListener()
            );
            ClusterState appliedState = executor.execute(clusterState, List.of(failAndMarkAsStale)).resultingState();
            Set<String> newInSync = appliedState.metadata().index(INDEX).inSyncAllocationIds(0);
            assertThat(Sets.difference(oldInSync, newInSync), contains(failedAllocationId));
        }
    }

    private ClusterState createClusterStateWithStartedShards(String reason) {
        int numberOfNodes = 1 + numberOfReplicas;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        IntStream.rangeClosed(1, numberOfNodes).mapToObj(node -> newNode("node" + node)).forEach(nodes::add);
        ClusterState stateAfterAddingNode = ClusterState.builder(clusterState).nodes(nodes).build();
        RoutingTable afterReroute = allocationService.reroute(stateAfterAddingNode, reason).routingTable();
        ClusterState stateAfterReroute = ClusterState.builder(stateAfterAddingNode).routingTable(afterReroute).build();
        return ESAllocationTestCase.startInitializingShardsAndReroute(allocationService, stateAfterReroute);
    }

    private List<FailedShardUpdateTask> createExistingShards(ClusterState currentState, String reason) {
        List<ShardRouting> shards = new ArrayList<>();
        GroupShardsIterator<ShardIterator> shardGroups = currentState.routingTable().allAssignedShardsGrouped(new String[] { INDEX }, true);
        for (ShardIterator shardIt : shardGroups) {
            for (ShardRouting shard : shardIt) {
                shards.add(shard);
            }
        }
        List<ShardRouting> failures = randomSubsetOf(randomIntBetween(1, 1 + shards.size() / 4), shards.toArray(new ShardRouting[0]));
        String indexUUID = metadata.index(INDEX).getIndexUUID();
        int numberOfTasks = randomIntBetween(failures.size(), 2 * failures.size());
        List<ShardRouting> shardsToFail = new ArrayList<>(numberOfTasks);
        for (int i = 0; i < numberOfTasks; i++) {
            shardsToFail.add(randomFrom(failures));
        }
        return toTasks(currentState, shardsToFail, indexUUID, reason);
    }

    private List<FailedShardUpdateTask> createNonExistentShards(ClusterState currentState, String reason) {
        // add shards from a non-existent index
        String nonExistentIndexUUID = "non-existent";
        Index index = new Index("non-existent", nonExistentIndexUUID);
        List<String> nodeIds = new ArrayList<>(currentState.nodes().getNodes().keySet());
        List<ShardRouting> nonExistentShards = new ArrayList<>();
        nonExistentShards.add(nonExistentShardRouting(index, nodeIds, true));
        for (int i = 0; i < numberOfReplicas; i++) {
            nonExistentShards.add(nonExistentShardRouting(index, nodeIds, false));
        }

        List<FailedShardUpdateTask> existingShards = createExistingShards(currentState, reason);
        List<FailedShardUpdateTask> shardsWithMismatchedAllocationIds = new ArrayList<>();
        for (FailedShardUpdateTask existingShard : existingShards) {
            FailedShardEntry entry = existingShard.entry();
            shardsWithMismatchedAllocationIds.add(
                new FailedShardUpdateTask(
                    new FailedShardEntry(entry.getShardId(), UUIDs.randomBase64UUID(), 0L, entry.message, entry.failure, randomBoolean()),
                    createTestListener()
                )
            );
        }

        List<FailedShardUpdateTask> tasks = new ArrayList<>();
        nonExistentShards.forEach(
            shard -> tasks.add(
                new FailedShardUpdateTask(
                    new FailedShardEntry(
                        shard.shardId(),
                        shard.allocationId().getId(),
                        0L,
                        reason,
                        new CorruptIndexException("simulated", nonExistentIndexUUID),
                        randomBoolean()
                    ),
                    createTestListener()
                )
            )
        );
        tasks.addAll(shardsWithMismatchedAllocationIds);
        return tasks;
    }

    private ShardRouting nonExistentShardRouting(Index index, List<String> nodeIds, boolean primary) {
        ShardRoutingState state = randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING, ShardRoutingState.STARTED);
        return TestShardRouting.newShardRouting(
            new ShardId(index, 0),
            randomFrom(nodeIds),
            state == ShardRoutingState.RELOCATING ? randomFrom(nodeIds) : null,
            primary,
            state
        );
    }

    private static void assertTasksSuccessful(
        List<FailedShardUpdateTask> tasks,
        ClusterStateTaskExecutor.ClusterTasksResult<FailedShardUpdateTask> result,
        ClusterState clusterState,
        boolean clusterStateChanged
    ) {
        List<Tuple<FailedShardUpdateTask, ClusterStateTaskExecutor.TaskResult>> taskResultList = tasks.stream()
            .map(t -> Tuple.tuple(t, ClusterStateTaskExecutor.TaskResult.success()))
            .collect(Collectors.toList());
        assertTaskResults(taskResultList, result, clusterState, clusterStateChanged);
    }

    private static void assertTaskResults(
        List<Tuple<FailedShardUpdateTask, ClusterStateTaskExecutor.TaskResult>> taskResultList,
        ClusterStateTaskExecutor.ClusterTasksResult<FailedShardUpdateTask> result,
        ClusterState clusterState,
        boolean clusterStateChanged
    ) {
        // there should be as many task results as tasks
        assertEquals(taskResultList.size(), result.executionResults().size());

        for (Tuple<FailedShardUpdateTask, ClusterStateTaskExecutor.TaskResult> entry : taskResultList) {
            // every task should have a corresponding task result
            assertTrue(result.executionResults().containsKey(entry.v1()));

            // the task results are as expected
            assertEquals(entry.v1().toString(), entry.v2().isSuccess(), result.executionResults().get(entry.v1()).isSuccess());
        }

        List<ShardRouting> shards = clusterState.getRoutingTable().allShards();
        for (Tuple<FailedShardUpdateTask, ClusterStateTaskExecutor.TaskResult> entry : taskResultList) {
            if (entry.v2().isSuccess()) {
                // the shard was successfully failed and so should not be in the routing table
                for (ShardRouting shard : shards) {
                    if (shard.assignedToNode()) {
                        assertFalse(
                            "entry key " + entry.v1() + ", shard routing " + shard,
                            entry.v1().entry().getShardId().equals(shard.shardId())
                                && entry.v1().entry().getAllocationId().equals(shard.allocationId().getId())
                        );
                    }
                }
            } else {
                // check we saw the expected failure
                ClusterStateTaskExecutor.TaskResult actualResult = result.executionResults().get(entry.v1());
                assertThat(actualResult.getFailure(), instanceOf(entry.v2().getFailure().getClass()));
                assertThat(actualResult.getFailure().getMessage(), equalTo(entry.v2().getFailure().getMessage()));
            }
        }

        if (clusterStateChanged) {
            assertNotSame(clusterState, result.resultingState());
        } else {
            assertSame(clusterState, result.resultingState());
        }
    }

    private static List<FailedShardUpdateTask> toTasks(
        ClusterState currentState,
        List<ShardRouting> shards,
        String indexUUID,
        String message
    ) {
        return shards.stream()
            .map(
                shard -> new FailedShardUpdateTask(
                    new FailedShardEntry(
                        shard.shardId(),
                        shard.allocationId().getId(),
                        randomBoolean() ? 0L : currentState.metadata().getIndexSafe(shard.index()).primaryTerm(shard.id()),
                        message,
                        new CorruptIndexException("simulated", indexUUID),
                        randomBoolean()
                    ),
                    createTestListener()
                )
            )
            .collect(Collectors.toList());
    }

    private static <T> ActionListener<T> createTestListener() {
        return ActionListener.wrap(() -> { throw new AssertionError("task should not complete"); });
    }
}
