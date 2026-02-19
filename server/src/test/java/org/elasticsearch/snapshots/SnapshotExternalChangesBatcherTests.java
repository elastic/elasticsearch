/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class SnapshotExternalChangesBatcherTests extends ESTestCase {
    private static final DiscoveryNode MASTER_NODE = DiscoveryNodeUtils.builder("node1")
        .roles(new HashSet<>(DiscoveryNodeRole.roles()))
        .build();

    public void testExternalChangesSubmissionAndExecution() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var executedChanges = new ArrayList<Boolean>();

        final AtomicReference<SnapshotExternalChangesBatcher> batcher = new AtomicReference<>();
        final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> executor = batchContext -> {
            assertThat("at most one task in the queue at any time", batchContext.taskContexts().size(), is(1));
            final var taskContext = batchContext.taskContexts().getFirst();
            executedChanges.add(batcher.get().acquireChangesToExecute());
            taskContext.success(batcher.get()::onTaskCompletion);
            return batchContext.initialState();
        };

        try (var masterService = createMasterService(threadPool)) {
            final var queue = masterService.createTaskQueue("snapshots-service-external-change", Priority.NORMAL, executor);
            batcher.set(new SnapshotExternalChangesBatcher(queue, s -> false, (e, m) -> {}, s -> {}, (e, v) -> {}));

            // Randomized interleaving of external tasks execution and submission.
            final var expectedChanges = new ArrayList<Boolean>();
            boolean batchNodes = false;
            boolean batchShards = false;
            for (int i = 0; i < 30; i++) {
                if (randomBoolean()) {
                    boolean nodes = randomBoolean();
                    boolean shards = randomBoolean();
                    if (nodes || shards) {
                        batchNodes |= nodes;
                        batchShards |= shards;
                        batcher.get().processExternalChanges(nodes, shards);
                    }
                } else {
                    if (batchNodes || batchShards) {
                        assertThat(masterService.numberOfPendingTasks(), is(1));
                        expectedChanges.add(batchNodes);
                        deterministicTaskQueue.runRandomTask();
                        batchNodes = false;
                        batchShards = false;
                    } else {
                        assertThat(masterService.numberOfPendingTasks(), is(0));
                    }
                }
            }
            if (batchNodes || batchShards) {
                expectedChanges.add(batchNodes);
                deterministicTaskQueue.runAllRunnableTasks();
                assertThat(masterService.numberOfPendingTasks(), is(0));
            }
            assertThat("executed nodesChanged flags should match expected batches", executedChanges, is(expectedChanges));
        }
    }

    public void testExternalChangesTaskResubmissionOnCompletion() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var pendingChanges = new AtomicBoolean(false);

        final AtomicReference<SnapshotExternalChangesBatcher> batcher = new AtomicReference<>();
        final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> executor = batchContext -> {
            pendingChanges.set(false);
            assertThat("at most one task in the queue at any time", batchContext.taskContexts().size(), is(1));
            final var taskContext = batchContext.taskContexts().getFirst();
            batcher.get().acquireChangesToExecute();
            if (randomBoolean()) {
                boolean nodes = randomBoolean();
                boolean shards = randomBoolean();
                batcher.get().processExternalChanges(nodes, shards);
                pendingChanges.set(nodes || shards);
            }
            taskContext.success(batcher.get()::onTaskCompletion);
            return batchContext.initialState();
        };

        try (var masterService = createMasterService(threadPool)) {
            final var queue = masterService.createTaskQueue("snapshots-service-external-change", Priority.NORMAL, executor);
            batcher.set(new SnapshotExternalChangesBatcher(queue, s -> false, (e, m) -> {}, s -> {}, (e, v) -> {}));

            batcher.get().processExternalChanges(true, true);
            for (int i = 0; i < 30; i++) {
                deterministicTaskQueue.runRandomTask();
                if (pendingChanges.get()) {
                    assertThat(masterService.numberOfPendingTasks(), is(1));
                } else {
                    assertThat(masterService.numberOfPendingTasks(), is(0));
                }
                batcher.get().processExternalChanges(true, false);
            }
        }
    }

    public void testExternalChangesTaskAbortsOnMasterFailure() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var hasFailed = new AtomicBoolean(false);
        final var lastSuccessfulExecutedChange = new AtomicBoolean(false);

        final AtomicReference<SnapshotExternalChangesBatcher> batcher = new AtomicReference<>();
        final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> executor = batchContext -> {
            hasFailed.set(false);
            assertThat("at most one task in the queue at any time", batchContext.taskContexts().size(), is(1));
            final var taskContext = batchContext.taskContexts().getFirst();
            if (randomBoolean()) {
                hasFailed.set(true);
                if (randomBoolean()) {
                    // Can fail before or after acquiring changes. Both cases should result in the same end state
                    batcher.get().acquireChangesToExecute();
                }
                if (randomBoolean()) {
                    // This change should get dropped because of the failure.
                    batcher.get().processExternalChanges(randomBoolean(), randomBoolean());
                }
                throw randomFrom(
                    new NotMasterException("simulated no longer master"),
                    new FailedToCommitClusterStateException("simulated failed to commit failure")
                );
            } else {
                lastSuccessfulExecutedChange.set(batcher.get().acquireChangesToExecute());
                taskContext.success(batcher.get()::onTaskCompletion);
                return batchContext.initialState();
            }
        };

        try (var masterService = createMasterService(threadPool)) {
            final var queue = masterService.createTaskQueue("snapshots-service-external-change", Priority.NORMAL, executor);
            batcher.set(new SnapshotExternalChangesBatcher(queue, s -> false, (e, m) -> {}, s -> {}, (e, v) -> {}));

            boolean nodesChanged = randomBoolean();
            boolean shardChanged = true;
            batcher.get().processExternalChanges(nodesChanged, shardChanged);
            for (int i = 0; i < 10; i++) {
                deterministicTaskQueue.runRandomTask();
                assertThat(masterService.numberOfPendingTasks(), is(0));
                if (!hasFailed.get()) {
                    // Verify the last failure wiped historical changes
                    assertThat(lastSuccessfulExecutedChange.get(), is(nodesChanged));
                }
                nodesChanged = randomBoolean();
                batcher.get().processExternalChanges(nodesChanged, shardChanged);
            }
        }
    }

    public void testCloneSnapshotClusterStateUpdate() throws Exception {
        final var repoClone = "repo-clone";
        final var repoCloneWithShards = "repo-clone-with-shards";
        final var isInitializingClone = randomBoolean();

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var masterService = createMasterService(new DeterministicTaskQueue().getThreadPool())) {
            final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> noopExecutor =
                ClusterStateTaskExecutor.BatchExecutionContext::initialState;
            final var batcher = new SnapshotExternalChangesBatcher(
                masterService.createTaskQueue("test", Priority.NORMAL, noopExecutor),
                s -> isInitializingClone,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            // Clone with empty shardSnapshotStatusByRepoShardId -> retained only if isInitializingClone is true.
            final var cloneIndexName = randomIndexName();
            final var emptyClone = SnapshotsInProgress.startClone(
                snapshot(repoClone, "empty-clone"),
                new SnapshotId("empty-clone", uuid()),
                Map.of(cloneIndexName, new IndexId(cloneIndexName, uuid())),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            );

            // Clone with non-empty shardSnapshotStatusByRepoShardId -> retained unchanged, no known failures.
            final var cloneWithShardIndexName = randomIndexName();
            final var cloneWithShardsIndexId = new IndexId(cloneWithShardIndexName, uuid());
            final var cloneWithShards = SnapshotsInProgress.startClone(
                snapshot(repoCloneWithShards, "clone-with-shards"),
                new SnapshotId("clone-with-shards", uuid()),
                Map.of(cloneWithShardIndexName, cloneWithShardsIndexId),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            )
                .withClones(
                    Map.of(new RepositoryShardId(cloneWithShardsIndexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED)
                );

            var clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(discoveryNodes(MASTER_NODE.getId()))
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(emptyClone).withAddedEntry(cloneWithShards));

            batcher.processExternalChanges(randomBoolean(), true);
            final var updatedState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterStateBuilder.build(),
                batcher.new Executor(),
                List.of(batcher.new Task())
            );
            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(updatedState);

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            assertThat("snapshotFinalizer should not fire for clone snapshots", finalized, empty());
            assertThat(
                "empty-shards clone should be retained only when isInitializingClone is true",
                updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoClone),
                isInitializingClone ? equalTo(List.of(emptyClone)) : empty()
            );
            assertThat(
                "non-empty-shards clone should always be retained unchanged when there are no known failures",
                updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoCloneWithShards),
                equalTo(List.of(cloneWithShards))
            );
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    public void testStartedWaitingSnapshotClusterStateUpdate() throws Exception {
        final var repoWaiting = "repo-waiting-snapshot";
        final var anotherNodeId = uuid();
        final var includeRoutingTable = randomBoolean();

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var masterService = createMasterService(new DeterministicTaskQueue().getThreadPool())) {
            final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> noopExecutor =
                ClusterStateTaskExecutor.BatchExecutionContext::initialState;
            final var batcher = new SnapshotExternalChangesBatcher(
                masterService.createTaskQueue("test", Priority.NORMAL, noopExecutor),
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var index = new Index(indexName, uuid());
            final var shardId = new ShardId(index, 0);
            // STARTED snapshot with a WAITING shard.
            // With routing table: shard promoted WAITING -> INIT, snapshot stays STARTED.
            // Without routing table: shard has no primary to assign to, fails with FAILED, snapshot completes.
            final var waitingEntry = snapshotEntry(
                snapshot(repoWaiting, "waiting-snapshot"),
                Map.of(indexName, new IndexId(indexName, uuid())),
                Map.of(
                    shardId,
                    new SnapshotsInProgress.ShardSnapshotStatus(
                        MASTER_NODE.getId(),
                        SnapshotsInProgress.ShardState.WAITING,
                        ShardGeneration.newGeneration(random())
                    )
                )
            );
            var clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(discoveryNodes(MASTER_NODE.getId()))
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(waitingEntry));
            if (includeRoutingTable) {
                clusterStateBuilder = clusterStateBuilder.putRoutingTable(
                    ProjectId.DEFAULT,
                    RoutingTable.builder()
                        .add(
                            IndexRoutingTable.builder(index)
                                .addIndexShard(
                                    IndexShardRoutingTable.builder(shardId)
                                        .addShard(TestShardRouting.newShardRouting(shardId, anotherNodeId, true, ShardRoutingState.STARTED))
                                )
                        )
                        .build()
                );
            }

            batcher.processExternalChanges(randomBoolean(), true);
            final var updatedState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterStateBuilder.build(),
                batcher.new Executor(),
                List.of(batcher.new Task())
            );
            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(updatedState);
            final var updatedWaitingSnapshots = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoWaiting);

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            assertThat("waiting snapshot should remain in cluster state", updatedWaitingSnapshots, hasSize(1));

            final var updatedSnapshot = updatedWaitingSnapshots.getFirst();
            if (includeRoutingTable) {
                assertThat(
                    "snapshot should stay STARTED while shard in INIT",
                    updatedSnapshot.state(),
                    is(SnapshotsInProgress.State.STARTED)
                );
                assertThat(
                    "WAITING shard should be promoted to INIT",
                    updatedSnapshot.shards().get(shardId).state(),
                    is(SnapshotsInProgress.ShardState.INIT)
                );
                assertThat(
                    "promoted shard should be assigned to the routing primary",
                    updatedSnapshot.shards().get(shardId).nodeId(),
                    is(anotherNodeId)
                );
                assertThat("snapshotFinalizer should not fire while the snapshot is still STARTED", finalized, empty());
            } else {
                assertTrue("snapshot should be in a terminal state after its shard was failed", updatedSnapshot.state().completed());
                assertThat(
                    "WAITING shard should be FAILED when no routing entry exists",
                    updatedSnapshot.shards().get(shardId).state(),
                    is(SnapshotsInProgress.ShardState.FAILED)
                );
                assertThat("snapshotFinalizer should fire for the completed snapshot", finalized, hasSize(1));
                assertTrue("finalised snapshot should be in a terminal state", finalized.get(0).state().completed());
            }
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    public void testCompletedSnapshotClusterStateUpdate() throws Exception {
        final var repoCompleted = "repo-completed-snapshot";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var masterService = createMasterService(new DeterministicTaskQueue().getThreadPool())) {
            final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> noopExecutor = ctx -> ctx.initialState();
            final var batcher = new SnapshotExternalChangesBatcher(
                masterService.createTaskQueue("test", Priority.NORMAL, noopExecutor),
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var index = new Index(indexName, uuid());
            final var shardId = new ShardId(index, 0);
            // A snapshot whose shard is all already in a terminal state.
            final var terminalShardState = randomBoolean() ? SnapshotsInProgress.ShardState.SUCCESS : SnapshotsInProgress.ShardState.FAILED;
            final var terminalShardStatus = terminalShardState == SnapshotsInProgress.ShardState.SUCCESS
                ? SnapshotsInProgress.ShardSnapshotStatus.success(
                    MASTER_NODE.getId(),
                    new ShardSnapshotResult(ShardGeneration.newGeneration(random()), ByteSizeValue.ZERO, 0)
                )
                : new SnapshotsInProgress.ShardSnapshotStatus(
                    MASTER_NODE.getId(),
                    SnapshotsInProgress.ShardState.FAILED,
                    ShardGeneration.newGeneration(random()),
                    "simulated failure"
                );
            final var completedEntry = snapshotEntry(
                snapshot(repoCompleted, "completed-snapshot"),
                Map.of(indexName, new IndexId(indexName, uuid())),
                Map.of(shardId, terminalShardStatus)
            );
            assertThat("test precondition: entry should already be in a completed state", completedEntry.state().completed(), is(true));

            // A deletion in the same repo skips snapshotFinalizer -> finalization done by deletion completion instead.
            final var includeSameRepoDeletion = randomBoolean();
            final var deletionEntry = SnapshotDeletionsInProgress.of(
                List.of(
                    new SnapshotDeletionsInProgress.Entry(
                        ProjectId.DEFAULT,
                        repoCompleted,
                        List.of(new SnapshotId("snap-to-delete", uuid())),
                        System.currentTimeMillis(),
                        1L,
                        SnapshotDeletionsInProgress.State.STARTED
                    )
                )
            );
            var clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(discoveryNodes(MASTER_NODE.getId()))
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(completedEntry));
            if (includeSameRepoDeletion) {
                clusterStateBuilder = clusterStateBuilder.putCustom(SnapshotDeletionsInProgress.TYPE, deletionEntry);
            }

            batcher.processExternalChanges(randomBoolean(), true);
            final var updatedState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterStateBuilder.build(),
                batcher.new Executor(),
                List.of(batcher.new Task())
            );
            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(updatedState);
            final var updatedCompletedSnapshots = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoCompleted);

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            // Completed snapshots are left untouched in cluster
            assertThat("completed snapshot should remain in cluster state", updatedCompletedSnapshots, hasSize(1));
            assertThat(
                "completed snapshot state should be unchanged",
                updatedCompletedSnapshots.getFirst().state(),
                is(completedEntry.state())
            );
            assertThat(
                "completed snapshot shard state should be unchanged",
                updatedCompletedSnapshots.getFirst().shards().get(shardId).state(),
                is(terminalShardState)
            );
            if (includeSameRepoDeletion) {
                // snapshotFinalizer is skipped: the deletion completion will trigger finalization.
                assertThat("snapshotFinalizer should not fire when a deletion exists in the same repo", finalized, empty());
                assertThat("deletionStarter should fire for the deletion", deleted.entrySet(), hasSize(1));
                assertNotNull(deleted.get(deletionEntry.getEntries().getFirst()));
            } else {
                assertThat("snapshotFinalizer should fire for the completed snapshot", finalized, hasSize(1));
                assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
            }
        }
    }

    public void testAbortedSnapshotClusterStateUpdateBasedOnNodeChanges() throws Exception {
        final var repoAborted = "repo-aborted-snapshot";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();
        final var isInitializingClone = randomBoolean();

        try (var masterService = createMasterService(new DeterministicTaskQueue().getThreadPool())) {
            final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> noopExecutor = ctx -> ctx.initialState();
            final var batcher = new SnapshotExternalChangesBatcher(
                masterService.createTaskQueue("test", Priority.NORMAL, noopExecutor),
                s -> isInitializingClone,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var index = new Index(indexName, uuid());
            final var shardId = new ShardId(index, 0);
            final var abortedEntry = snapshotEntry(
                snapshot(repoAborted, "aborted-snapshot"),
                Map.of(indexName, new IndexId(indexName, uuid())),
                Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(uuid(), ShardGeneration.newGeneration(random())))
            ).abort();

            assertNotNull("aborted entry should not be null", abortedEntry);
            assertThat("entry should be ABORTED", abortedEntry.state(), is(SnapshotsInProgress.State.ABORTED));
            assertThat(
                "shard should be in ABORTED state",
                abortedEntry.shards().get(shardId).state(),
                is(SnapshotsInProgress.ShardState.ABORTED)
            );

            final var clusterState = ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(discoveryNodes(MASTER_NODE.getId()))
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(abortedEntry))
                .build();

            final var nodesChanged = randomBoolean();
            batcher.processExternalChanges(nodesChanged, true);
            final var updatedState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterState,
                batcher.new Executor(),
                List.of(batcher.new Task())
            );
            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(updatedState);
            final var updatedAbortedSnapshots = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoAborted);

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));

            if (nodesChanged) {
                // ABORTED snapshots are included in statesToUpdate.
                // The ABORTED shard on the departed node is failed → the snapshot becomes completed → snapshotFinalizer fires.
                assertThat("snapshotFinalizer should fire for the completed aborted snapshot when nodes changed", finalized, hasSize(1));
                assertTrue("completed aborted snapshot should be in a terminal state", finalized.get(0).state().completed());
                assertThat(
                    "shard should be FAILED after its node departed the cluster",
                    finalized.get(0).shards().get(shardId).state(),
                    is(SnapshotsInProgress.ShardState.FAILED)
                );
            } else {
                // ABORTED snapshots are excluded from statesToUpdate.
                // No shard-state changes are applied: the snapshot remains ABORTED and the finalizer is not called.
                assertThat("snapshotFinalizer should not fire for an ABORTED snapshot when only shard routing changed", finalized, empty());
                assertThat("aborted snapshot should remain in cluster state", updatedAbortedSnapshots, hasSize(1));
                assertThat(
                    "aborted snapshot should still be ABORTED",
                    updatedAbortedSnapshots.getFirst().state(),
                    is(SnapshotsInProgress.State.ABORTED)
                );
                assertThat(
                    "shard should remain ABORTED when nodes have not changed",
                    updatedAbortedSnapshots.getFirst().shards().get(shardId).state(),
                    is(SnapshotsInProgress.ShardState.ABORTED)
                );
            }
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    private static MasterService createMasterService(ThreadPool threadPool) {
        final var settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "test")
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .build();
        final var masterService = new MasterService(
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            new TaskManager(settings, threadPool, emptySet()),
            MeterRegistry.NOOP
        ) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                return new StoppableExecutorServiceWrapper(threadPool.generic());
            }
        };
        final var initialState = ClusterState.builder(new ClusterName("external-tasks-test"))
            .nodes(DiscoveryNodes.builder().add(MASTER_NODE).localNodeId(MASTER_NODE.getId()).masterNodeId(MASTER_NODE.getId()))
            .putCompatibilityVersions(MASTER_NODE.getId(), CompatibilityVersionsUtils.staticCurrent())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        final var clusterStateRef = new AtomicReference<>(initialState);
        masterService.setClusterStatePublisher((e, pl, al) -> {
            ClusterServiceUtils.setAllElapsedMillis(e);
            clusterStateRef.set(e.getNewState());
            threadPool.generic().execute(() -> pl.onResponse(null));
        });
        masterService.setClusterStateSupplier(clusterStateRef::get);
        masterService.start();
        threadPool.getThreadContext().markAsSystemContext();
        return masterService;
    }

    private static String uuid() {
        return UUIDs.randomBase64UUID(random());
    }

    private static Snapshot snapshot(String repoName, String name) {
        return new Snapshot(ProjectId.DEFAULT, repoName, new SnapshotId(name, uuid()));
    }

    private static SnapshotsInProgress.Entry snapshotEntry(
        Snapshot snapshot,
        Map<String, IndexId> indexIds,
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards
    ) {
        return SnapshotsInProgress.startedEntry(
            snapshot,
            random().nextBoolean(),
            random().nextBoolean(),
            indexIds,
            Collections.emptyList(),
            1L,
            randomNonNegativeLong(),
            shards,
            Collections.emptyMap(),
            IndexVersion.current(),
            Collections.emptyList()
        );
    }

    private static DiscoveryNodes discoveryNodes(String localNodeId) {
        return DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder(localNodeId).roles(new HashSet<>(DiscoveryNodeRole.roles())).build())
            .localNodeId(localNodeId)
            .build();
    }
}
