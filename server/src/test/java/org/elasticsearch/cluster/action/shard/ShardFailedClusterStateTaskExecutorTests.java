/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.action.shard;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardEntry;
import org.elasticsearch.cluster.action.shard.ShardStateAction.FailedShardUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.StaleShard;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;

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
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(1)
                    .numberOfReplicas(numberOfReplicas)
                    .primaryTerm(0, randomIntBetween(2, 10))
            )
            .build();
        routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(metadata.index(INDEX)).build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
        executor = new ShardStateAction.ShardFailedClusterStateTaskExecutor(allocationService, null);
    }

    public void testEmptyTaskListProducesSameClusterState() throws Exception {
        String reason = "test no-op";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        executeAndAssertSuccessful(currentState, List.of(), false);
    }

    public void testDuplicateFailuresAreOkay() throws Exception {
        String reason = "test duplicate failures are okay";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<FailedShardUpdateTask> tasks = createExistingShards(currentState, reason);
        executeAndAssertSuccessful(currentState, tasks, true);
    }

    public void testNonExistentShardsAreMarkedAsSuccessful() throws Exception {
        String reason = "test non existent shards are marked as successful";
        ClusterState currentState = createClusterStateWithStartedShards(reason);
        List<FailedShardUpdateTask> tasks = createNonExistentShards(currentState, reason);
        executeAndAssertSuccessful(currentState, tasks, false);
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

        final var resultingState = ClusterStateTaskExecutorUtils.executeHandlingResults(
            currentState,
            failingExecutor,
            tasks,
            task -> assertThat(nonExistentTasks, hasItem(task)),
            (task, e) -> {
                assertThat(failingTasks, hasItem(task));
                assertThat(e, Matchers.instanceOf(RuntimeException.class));
                assertThat(e.getMessage(), equalTo("simulated applyFailedShards failure"));
            }
        );
        assertSame(currentState, resultingState);
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
        final var resultingState = ClusterStateTaskExecutorUtils.executeHandlingResults(
            currentState,
            executor,
            tasks,
            task -> fail("unexpectedly succeeded: " + task),
            (task, e) -> {
                if (e instanceof ShardStateAction.NoLongerPrimaryShardException noLongerPrimaryShardException) {
                    assertThat(noLongerPrimaryShardException.getShardId(), equalTo(task.entry().getShardId()));
                    assertThat(
                        noLongerPrimaryShardException.getMessage(),
                        equalTo(
                            "primary term ["
                                + task.entry().primaryTerm
                                + "] did not match current primary term ["
                                + currentState.metadata()
                                    .index(task.entry().getShardId().getIndex())
                                    .primaryTerm(task.entry().getShardId().id())
                                + "]"
                        )
                    );
                } else {
                    assert false : e;
                }
            }
        );
        assertSame(currentState, resultingState);
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
            ClusterState appliedState = executeAndAssertSuccessful(clusterState, List.of(failShardOnly), true);
            Set<String> newInSync = appliedState.metadata().index(INDEX).inSyncAllocationIds(0);
            assertThat(newInSync, equalTo(oldInSync));
        }
        {
            final String failedAllocationId = randomFrom(oldInSync);
            FailedShardUpdateTask failAndMarkAsStale = new FailedShardUpdateTask(
                new FailedShardEntry(shardRoutingTable.shardId(), failedAllocationId, primaryTerm, "dummy", null, true),
                createTestListener()
            );
            ClusterState appliedState = executeAndAssertSuccessful(clusterState, List.of(failAndMarkAsStale), true);
            Set<String> newInSync = appliedState.metadata().index(INDEX).inSyncAllocationIds(0);
            assertThat(Sets.difference(oldInSync, newInSync), contains(failedAllocationId));
        }
    }

    private ClusterState createClusterStateWithStartedShards(String reason) {
        int numberOfNodes = 1 + numberOfReplicas;
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        IntStream.rangeClosed(1, numberOfNodes).mapToObj(node -> newNode("node" + node)).forEach(nodes::add);
        ClusterState stateAfterAddingNode = ClusterState.builder(clusterState).nodes(nodes).build();
        ClusterState stateWithInitializingPrimary = allocationService.reroute(stateAfterAddingNode, reason, ActionListener.noop());
        ClusterState stateWithStartedPrimary = startInitializingShardsAndReroute(allocationService, stateWithInitializingPrimary);
        final boolean secondReroute = randomBoolean();
        ClusterState resultingState = secondReroute
            ? startInitializingShardsAndReroute(allocationService, stateWithStartedPrimary)
            : stateWithStartedPrimary;
        final var indexShardRoutingTable = resultingState.routingTable().shardRoutingTable(INDEX, 0);
        assertTrue(indexShardRoutingTable.primaryShard().started());
        assertTrue(
            RoutingNodesHelper.asStream(indexShardRoutingTable)
                .anyMatch(sr -> sr.primary() == false && sr.unassigned() == false && (sr.started() || secondReroute == false))
        );
        return resultingState;
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

    private ClusterState executeAndAssertSuccessful(ClusterState clusterState, List<FailedShardUpdateTask> tasks, boolean expectChange)
        throws Exception {

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(clusterState, executor, tasks);
        if (expectChange) {
            assertNotSame(clusterState, resultingState);
        } else {
            assertSame(clusterState, resultingState);
        }

        for (ShardRouting shard : resultingState.getRoutingTable().allShardsIterator()) {
            if (shard.assignedToNode()) {
                for (final var task : tasks) {
                    assertFalse(
                        "task " + task + ", shard routing " + shard,
                        task.entry().getShardId().equals(shard.shardId())
                            && task.entry().getAllocationId().equals(shard.allocationId().getId())
                    );
                }
            }
        }

        return resultingState;
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
            .toList();
    }

    private static <T> ActionListener<T> createTestListener() {
        return ActionListener.running(() -> { throw new AssertionError("task should not complete"); });
    }
}
