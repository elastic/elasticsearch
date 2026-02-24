/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
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
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
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
import java.util.function.UnaryOperator;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class SnapshotExternalChangesBatcherTests extends ESTestCase {
    private static final DiscoveryNode MASTER_NODE = DiscoveryNodeUtils.builder("node1")
        .roles(new HashSet<>(DiscoveryNodeRole.roles()))
        .build();

    public void testProcessExternalChangesWithNoChanges() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final AtomicReference<SnapshotExternalChangesBatcher> batcher = new AtomicReference<>();
        final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> executor = batchContext -> {
            fail("executor should never be called when no changes are submitted");
            return batchContext.initialState();
        };

        try (var masterService = createMasterService(threadPool)) {
            final var queue = masterService.createTaskQueue("snapshots-service-external-change", Priority.NORMAL, executor);
            batcher.set(new SnapshotExternalChangesBatcher(queue, s -> false, (e, m) -> {}, s -> {}, (e, v) -> {}));

            batcher.get().processExternalChanges(false, false);

            assertThat("no task should be pending", masterService.numberOfPendingTasks(), is(0));
            assertFalse("no runnable tasks should exist", deterministicTaskQueue.hasRunnableTasks());
        }
    }

    public void testExternalChangesSubmissionAndExecution() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var executedChanges = new ArrayList<Boolean>();

        final AtomicReference<SnapshotExternalChangesBatcher> batcher = new AtomicReference<>();
        final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> executor = batchContext -> {
            assertThat("at most one task in the queue at any time", batchContext.taskContexts().size(), is(1));
            final var taskContext = batchContext.taskContexts().getFirst();
            executedChanges.add(taskContext.getTask().nodeChanges);
            taskContext.success(batcher.get()::onTaskCompletion);
            return batchContext.initialState();
        };

        try (var masterService = createMasterService(threadPool)) {
            final var queue = masterService.createTaskQueue("snapshots-service-external-change", Priority.NORMAL, executor);
            batcher.set(new SnapshotExternalChangesBatcher(queue, s -> false, (e, m) -> {}, s -> {}, (e, v) -> {}));

            final var expectedExecutedChanges = new ArrayList<Boolean>();
            final var firstQueued = randomBoolean();
            batcher.get().processExternalChanges(firstQueued, true);
            expectedExecutedChanges.add(firstQueued);

            // Randomized interleaving of external tasks execution and submission.
            boolean pendingNodeChanges = false;
            boolean pendingShardChanges = false;
            for (int i = 0; i < 30; i++) {
                if (randomBoolean()) {
                    boolean nodes = randomBoolean();
                    boolean shards = randomBoolean();
                    batcher.get().processExternalChanges(nodes, shards);
                    if (nodes || shards) {
                        pendingNodeChanges |= nodes;
                        pendingShardChanges |= shards;
                    }
                } else {
                    if (pendingNodeChanges || pendingShardChanges) {
                        assertThat(masterService.numberOfPendingTasks(), is(1));
                        expectedExecutedChanges.add(pendingNodeChanges);
                        deterministicTaskQueue.runRandomTask();
                        pendingNodeChanges = false;
                        pendingShardChanges = false;
                    } else {
                        deterministicTaskQueue.runRandomTask();
                        assertThat(masterService.numberOfPendingTasks(), is(0));
                        final var restartQueue = randomBoolean();
                        batcher.get().processExternalChanges(restartQueue, true);
                        expectedExecutedChanges.add(restartQueue);
                    }
                }
            }
            if (pendingNodeChanges || pendingShardChanges) {
                expectedExecutedChanges.add(pendingNodeChanges);
            }
            deterministicTaskQueue.runAllRunnableTasks();
            assertThat(masterService.numberOfPendingTasks(), is(0));
            assertThat("executed changes should match expected", executedChanges, is(expectedExecutedChanges));
        }
    }

    public void testExternalChangesTaskResubmissionOnSuccessAndFailure() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var concurrentChange = new AtomicBoolean(false);

        final AtomicReference<SnapshotExternalChangesBatcher> batcher = new AtomicReference<>();
        final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> executor = batchContext -> {
            assertThat("at most one task in the queue at any time", batchContext.taskContexts().size(), is(1));
            final var taskContext = batchContext.taskContexts().getFirst();
            if (concurrentChange.get()) {
                batcher.get().processExternalChanges(randomBoolean(), true);
            }
            if (randomBoolean()) {
                throw randomFrom(
                    new NotMasterException("simulated no longer master"),
                    new FailedToCommitClusterStateException("simulated failed to commit failure")
                );
            }
            taskContext.success(batcher.get()::onTaskCompletion);
            return batchContext.initialState();
        };

        try (var masterService = createMasterService(threadPool)) {
            final var queue = masterService.createTaskQueue("snapshots-service-external-change", Priority.NORMAL, executor);
            batcher.set(new SnapshotExternalChangesBatcher(queue, s -> false, (e, m) -> {}, s -> {}, (e, v) -> {}));

            for (int i = 0; i < 10; i++) {
                batcher.get().processExternalChanges(randomBoolean(), true);
                concurrentChange.set(randomBoolean());
                deterministicTaskQueue.runRandomTask();
                if (concurrentChange.get()) {
                    assertThat(masterService.numberOfPendingTasks(), is(1));
                    concurrentChange.set(false);
                    deterministicTaskQueue.runRandomTask();
                }
                assertThat(masterService.numberOfPendingTasks(), is(0));
            }
        }
    }

    public void testStartedWaitingSnapshotClusterStateUpdate() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repoWaiting = "repo-waiting-snapshot";
        final var anotherNodeId = uuid();
        final var includeRoutingTable = randomBoolean();

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
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

            updateClusterState(clusterService, deterministicTaskQueue, currentState -> {
                var updateBuilder = ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(waitingEntry));
                if (includeRoutingTable) {
                    updateBuilder = updateBuilder.putRoutingTable(
                        ProjectId.DEFAULT,
                        RoutingTable.builder()
                            .add(
                                IndexRoutingTable.builder(index)
                                    .addIndexShard(
                                        IndexShardRoutingTable.builder(shardId)
                                            .addShard(
                                                TestShardRouting.newShardRouting(shardId, anotherNodeId, true, ShardRoutingState.STARTED)
                                            )
                                    )
                            )
                            .build()
                    );
                }
                return updateBuilder.build();
            });

            batcher.processExternalChanges(randomBoolean(), true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
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
                assertTrue("finalised snapshot should be in a terminal state", finalized.getFirst().state().completed());
            }
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    public void testCompletedSnapshotClusterStateUpdate() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repoCompleted = "repo-completed-snapshot";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var index = new Index(indexName, uuid());
            final var shardId = new ShardId(index, 0);
            // A snapshot whose shard is already in a terminal state.
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
                    "failure"
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

            updateClusterState(clusterService, deterministicTaskQueue, currentState -> {
                var updateBuilder = ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(completedEntry));
                if (includeSameRepoDeletion) {
                    updateBuilder = updateBuilder.putCustom(SnapshotDeletionsInProgress.TYPE, deletionEntry);
                }
                return updateBuilder.build();
            });

            batcher.processExternalChanges(randomBoolean(), true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            final var updatedCompletedSnapshots = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoCompleted);

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            assertThat("completed snapshot should remain in cluster state", updatedCompletedSnapshots, hasSize(1));
            assertThat("completed snapshot should be untouched", updatedCompletedSnapshots.getFirst(), sameInstance(completedEntry));
            if (includeSameRepoDeletion) {
                // snapshotFinalizer is skipped: the deletion completion will trigger finalization.
                assertThat("snapshotFinalizer should not fire", finalized, empty());
                assertThat("deletionStarter should fire for the deletion", deleted.entrySet(), hasSize(1));
                assertNotNull(deleted.get(deletionEntry.getEntries().getFirst()));
            } else {
                assertThat("snapshotFinalizer should fire for the completed snapshot", finalized, hasSize(1));
                assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
            }
        }
    }

    public void testAbortedSnapshotClusterStateUpdateBasedOnNodeChanges() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repoAborted = "repo-aborted-snapshot";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();
        final var isInitializingClone = randomBoolean();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> isInitializingClone,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var shardId = new ShardId(new Index(indexName, uuid()), 0);
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

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(abortedEntry))
                    .build()
            );

            final var nodesChanged = randomBoolean();
            batcher.processExternalChanges(nodesChanged, true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            final var updatedAbortedSnapshots = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoAborted);

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));

            if (nodesChanged) {
                // ABORTED snapshots are included in update.
                // The ABORTED shard on the departed node has failed -> the snapshot becomes completed -> snapshotFinalizer fires.
                assertThat("snapshotFinalizer should fire for the completed aborted snapshot", finalized, hasSize(1));
                assertTrue("completed aborted snapshot should be in a terminal state", finalized.getFirst().state().completed());
                assertThat(
                    "shard should be FAILED",
                    finalized.getFirst().shards().get(shardId).state(),
                    is(SnapshotsInProgress.ShardState.FAILED)
                );
            } else {
                // ABORTED snapshots are excluded from update.
                // No shard-state changes are applied: the snapshot remains ABORTED and the finalizer is not called.
                assertThat("snapshotFinalizer should not be called for an ABORTED snapshot when only shard changed", finalized, empty());
                assertThat("aborted snapshot should remain in cluster state", updatedAbortedSnapshots, hasSize(1));
                assertThat("aborted snapshot should be untouched", updatedAbortedSnapshots.getFirst(), sameInstance(abortedEntry));
            }
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    public void testBwcUnknownRepoGenEntryCleanup() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repo = "repo-bwc";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            // Older versions could write entries with UNKNOWN_REPO_GEN in INIT or ABORTED state that did not write anything
            // to the repository. To simulate this, create an ABORTED entry with UNKNOWN_REPO_GEN and nodesChanged=false
            final var bwcEntry = SnapshotsInProgress.startedEntry(
                snapshot(repo, "bwc-snapshot"),
                random().nextBoolean(),
                random().nextBoolean(),
                Map.of(indexName, new IndexId(indexName, uuid())),
                Collections.emptyList(),
                1L,
                RepositoryData.UNKNOWN_REPO_GEN,
                Map.of(
                    new ShardId(new Index(indexName, uuid()), 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(MASTER_NODE.getId(), ShardGeneration.newGeneration(random()))
                ),
                Collections.emptyMap(),
                IndexVersion.current(),
                Collections.emptyList()
            ).abort();
            assertNotNull("aborted bwc entry should not be null", bwcEntry);
            assertThat("bwc entry should be ABORTED", bwcEntry.state(), is(SnapshotsInProgress.State.ABORTED));
            assertThat("bwc entry should have UNKNOWN_REPO_GEN", bwcEntry.repositoryStateId(), is(RepositoryData.UNKNOWN_REPO_GEN));

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(bwcEntry))
                    .build()
            );

            batcher.processExternalChanges(false, true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            assertThat("bwc entry should be removed", updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repo), empty());
            assertThat("snapshotFinalizer should not fire for bwc cleanup", finalized, empty());
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    public void testWaitingDeletionDoesNotSuppressFinalization() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repo = "repo-waiting-deletion";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var completedEntry = snapshotEntry(
                snapshot(repo, "completed-snapshot"),
                Map.of(indexName, new IndexId(indexName, uuid())),
                Map.of(
                    new ShardId(new Index(indexName, uuid()), 0),
                    SnapshotsInProgress.ShardSnapshotStatus.success(
                        MASTER_NODE.getId(),
                        new ShardSnapshotResult(ShardGeneration.newGeneration(random()), ByteSizeValue.ZERO, 0)
                    )
                )
            );
            assertThat("snapshot should already be completed", completedEntry.state().completed(), is(true));

            // A WAITING deletion in the same repo.
            final var waitingDeletion = new SnapshotDeletionsInProgress.Entry(
                ProjectId.DEFAULT,
                repo,
                List.of(new SnapshotId("snap-to-delete", uuid())),
                System.currentTimeMillis(),
                1L,
                SnapshotDeletionsInProgress.State.WAITING
            );

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(completedEntry))
                    .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(waitingDeletion)))
                    .build()
            );

            batcher.processExternalChanges(randomBoolean(), true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            assertThat(
                "snapshotFinalizer should be called on completed snapshot: WAITING deletion does not suppress finalization",
                finalized,
                hasSize(1)
            );
            assertThat(finalized.getFirst().snapshot().getSnapshotId().getName(), is("completed-snapshot"));
            assertThat("finalized entry should be completed", finalized.getFirst().state(), is(SnapshotsInProgress.State.SUCCESS));
            assertThat("deletionStarter should not be called: WAITING deletion not yet started", deleted.entrySet(), empty());

            final var updatedEntries = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repo);
            assertThat("snapshot entry should be retained in cluster state", updatedEntries, hasSize(1));
            assertThat("snapshot should be unchanged", updatedEntries.getFirst(), sameInstance(completedEntry));
        }
    }

    public void testCloneSnapshotWithOrWithoutShardSnapshotStatus() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repoClone = "repo-clone";
        final var repoCloneWithShards = "repo-clone-with-shards";
        final var isInitializingClone = randomBoolean();

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
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

            // Clone with non-empty shardSnapshotStatusByRepoShardId -> retained unchanged
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

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(
                        SnapshotsInProgress.TYPE,
                        SnapshotsInProgress.EMPTY.withAddedEntry(emptyClone).withAddedEntry(cloneWithShards)
                    )
                    .build()
            );

            batcher.processExternalChanges(randomBoolean(), true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            assertThat("snapshotFinalizer should not fire for clone snapshots", finalized, empty());
            final var updatedEmptyClones = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoClone);
            if (isInitializingClone) {
                assertThat("empty-shards clone should be retained when isInitializingClone is true", updatedEmptyClones, hasSize(1));
                assertThat("empty-shards clone should be untouched", updatedEmptyClones.getFirst(), sameInstance(emptyClone));
            } else {
                assertThat("empty-shards clone should be removed when isInitializingClone is false", updatedEmptyClones, empty());
            }
            final var updatedClonesWithShards = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoCloneWithShards);
            assertThat("non-empty-shards clone should always be retained", updatedClonesWithShards, hasSize(1));
            assertThat("non-empty-shards clone should be untouched", updatedClonesWithShards.getFirst(), sameInstance(cloneWithShards));
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    public void testCloneSnapshotWhenExecutingDeletion() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repo = "repo-clone-with-deletion";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var indexId = new IndexId(indexName, uuid());
            final var cloneEntry = SnapshotsInProgress.startClone(
                snapshot(repo, "clone"),
                new SnapshotId("source", uuid()),
                Map.of(indexName, indexId),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            ).withClones(Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

            // STARTED deletion in the same repo prevents clone shard reassignment.
            final var deletionEntry = new SnapshotDeletionsInProgress.Entry(
                ProjectId.DEFAULT,
                repo,
                List.of(new SnapshotId("snap-to-delete", uuid())),
                System.currentTimeMillis(),
                1L,
                SnapshotDeletionsInProgress.State.STARTED
            );

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(cloneEntry))
                    .putCustom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.of(List.of(deletionEntry)))
                    .build()
            );

            batcher.processExternalChanges(randomBoolean(), true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            final var updatedClones = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repo);
            assertThat("clone should be retained when deletion in progress in the same repo", updatedClones, hasSize(1));
            assertThat("clone should be untouched", updatedClones.getFirst(), sameInstance(cloneEntry));
            assertThat("snapshotFinalizer should not fire for clone snapshots", finalized, empty());
            assertThat("deletionStarter should fire for the deletion", deleted.entrySet(), hasSize(1));
        }
    }

    public void testCloneShardsReassignedFromKnownFailures() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repo = "repo-clone-failures";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var indexId = new IndexId(indexName, uuid());
            final var departedNodeId = uuid();
            final var shardGeneration = ShardGeneration.newGeneration(random());

            // Snapshot with shard in INIT on a departed node will fail -> knownFailures.
            final var nonCloneEntry = snapshotEntry(
                snapshot(repo, "non-clone"),
                Map.of(indexName, indexId),
                Map.of(
                    new ShardId(new Index(indexName, uuid()), 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(departedNodeId, shardGeneration)
                )
            );

            // Clone with UNASSIGNED_QUEUED shard in the failed RepositoryShardId
            // -> reassigned using the generation from the known failure.
            final var cloneEntry = SnapshotsInProgress.startClone(
                snapshot(repo, "clone"),
                new SnapshotId("source", uuid()),
                Map.of(indexName, indexId),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            ).withClones(Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    // failing snapshot first, then clone
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(nonCloneEntry).withAddedEntry(cloneEntry))
                    .build()
            );

            batcher.processExternalChanges(true, true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));

            final var updatedEntries = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repo);
            assertThat("both entries should be retained", updatedEntries, hasSize(2));

            final var updatedNonClone = updatedEntries.get(0);
            assertThat(updatedNonClone.snapshot().getSnapshotId().getName(), is("non-clone"));
            assertThat(
                "non-clone should be completed because its only shard failed on the departed node",
                updatedNonClone.state(),
                is(SnapshotsInProgress.State.SUCCESS)
            );

            final var updatedClone = updatedEntries.get(1);
            assertThat(updatedClone.snapshot().getSnapshotId().getName(), is("clone"));
            assertThat("clone should still be in progress", updatedClone.state(), is(SnapshotsInProgress.State.STARTED));
            final var cloneShardStatus = updatedClone.shardSnapshotStatusByRepoShardId().get(new RepositoryShardId(indexId, 0));
            assertThat(
                "clone shard should be reassigned to local node using the failure's generation",
                cloneShardStatus.nodeId(),
                is(MASTER_NODE.getId())
            );
            assertThat("clone shard should use the generation from the known failure", cloneShardStatus.generation(), is(shardGeneration));
        }
    }

    public void testCloneShardSkippedWhenAlreadyActiveInAnotherEntry() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repo = "repo-active-shard";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var indexId = new IndexId(indexName, uuid());
            final var departedNodeId = uuid();
            final var shardGeneration = ShardGeneration.newGeneration(random());

            // non-clone with a shard on a departed node -> fails, populates knownFailures.
            final var failingEntry = snapshotEntry(
                snapshot(repo, "failing"),
                Map.of(indexName, indexId),
                Map.of(
                    new ShardId(new Index(indexName, uuid()), 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(departedNodeId, shardGeneration)
                )
            );

            // first clone with UNASSIGNED_QUEUED for the same RepositoryShardId -> shard promoted.
            final var firstClone = SnapshotsInProgress.startClone(
                snapshot(repo, "clone-1"),
                new SnapshotId("source-1", uuid()),
                Map.of(indexName, indexId),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            ).withClones(Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

            // second clone with UNASSIGNED_QUEUED for the same RepositoryShardId.
            // knownFailures has a match, but isActive() is now true -> shard is skipped.
            final var secondClone = SnapshotsInProgress.startClone(
                snapshot(repo, "clone-2"),
                new SnapshotId("source-2", uuid()),
                Map.of(indexName, indexId),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            ).withClones(Map.of(new RepositoryShardId(indexId, 0), SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED));

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(
                        SnapshotsInProgress.TYPE,
                        SnapshotsInProgress.EMPTY.withAddedEntry(failingEntry).withAddedEntry(firstClone).withAddedEntry(secondClone)
                    )
                    .build()
            );

            batcher.processExternalChanges(true, true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            final var updatedEntries = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repo);
            assertThat("all three entries should be retained", updatedEntries, hasSize(3));

            final var updatedFailingEntry = updatedEntries.get(0);
            assertThat(updatedFailingEntry.snapshot().getSnapshotId().getName(), is("failing"));
            assertThat("failing entry should be completed", updatedFailingEntry.state(), is(SnapshotsInProgress.State.SUCCESS));

            final var updatedFirstClone = updatedEntries.get(1);
            assertThat(updatedFirstClone.snapshot().getSnapshotId().getName(), is("clone-1"));
            final var firstCloneStatus = updatedFirstClone.shardSnapshotStatusByRepoShardId().get(new RepositoryShardId(indexId, 0));
            assertThat("first clone shard should be reassigned to local node", firstCloneStatus.nodeId(), is(MASTER_NODE.getId()));

            assertThat(
                "second clone should be untouched because the shard is already active in the first clone",
                updatedEntries.get(2),
                sameInstance(secondClone)
            );

            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
        }
    }

    public void testNonCloneEntryRetainedUnchangedWhenNoShardChanges() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repo = "repo-no-changes";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var index = new Index(indexName, uuid());
            final var shardId = new ShardId(index, 0);
            // STARTED snapshot with a shard in INIT state on a node that still exists (MASTER_NODE).
            final var entry = snapshotEntry(
                snapshot(repo, "snapshot"),
                Map.of(indexName, new IndexId(indexName, uuid())),
                Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(MASTER_NODE.getId(), ShardGeneration.newGeneration(random())))
            );

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(entry))
                    .build()
            );

            batcher.processExternalChanges(randomBoolean(), true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            final var updatedEntries = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repo);
            assertThat("snapshot should be retained in cluster state", updatedEntries, hasSize(1));
            assertThat("snapshot should be untouched", updatedEntries.getFirst(), sameInstance(entry));
            assertThat("snapshotFinalizer should not fire for a non-completed snapshot", finalized, empty());
            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
            assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
        }
    }

    public void testCloneMultipleShardsReassignedFromKnownFailures() {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var repo = "repo-multi-clone";

        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();

        try (var clusterService = createClusterService(deterministicTaskQueue)) {
            final var batcher = new SnapshotExternalChangesBatcher(
                clusterService,
                s -> false,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            final var indexName = randomIndexName();
            final var index = new Index(indexName, uuid());
            final var indexId = new IndexId(indexName, index.getUUID());
            final var departedNodeId = uuid();
            final var gen0 = ShardGeneration.newGeneration(random());
            final var gen1 = ShardGeneration.newGeneration(random());
            final var gen2 = ShardGeneration.newGeneration(random());

            // Non-clone entry with 3 shards on a departed node
            // all three will fail and get added to knownFailures.
            final var nonCloneEntry = snapshotEntry(
                snapshot(repo, "non-clone"),
                Map.of(indexName, indexId),
                Map.of(
                    new ShardId(index, 0),
                    new SnapshotsInProgress.ShardSnapshotStatus(departedNodeId, gen0),
                    new ShardId(index, 1),
                    new SnapshotsInProgress.ShardSnapshotStatus(departedNodeId, gen1),
                    new ShardId(index, 2),
                    new SnapshotsInProgress.ShardSnapshotStatus(departedNodeId, gen2)
                )
            );

            // Clone entry with shards 0 and 1 as UNASSIGNED_QUEUED, but no shard 2.
            // - Shard 0: UNASSIGNED_QUEUED -> initializes inFlightShardSnapshotStates and clones builder
            // - Shard 1: UNASSIGNED_QUEUED -> hits the "already initialized" branches
            // - Shard 2: not present in clone
            final var cloneEntry = SnapshotsInProgress.startClone(
                snapshot(repo, "clone"),
                new SnapshotId("source", uuid()),
                Map.of(indexName, indexId),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            )
                .withClones(
                    Map.of(
                        new RepositoryShardId(indexId, 0),
                        SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED,
                        new RepositoryShardId(indexId, 1),
                        SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED
                    )
                );

            updateClusterState(
                clusterService,
                deterministicTaskQueue,
                currentState -> ClusterState.builder(currentState)
                    .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY.withAddedEntry(nonCloneEntry).withAddedEntry(cloneEntry))
                    .build()
            );

            batcher.processExternalChanges(true, true);
            deterministicTaskQueue.runAllRunnableTasks();

            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
            final var updatedEntries = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repo);
            assertThat("both entries should be retained", updatedEntries, hasSize(2));

            final var updatedNonClone = updatedEntries.getFirst();
            assertThat(updatedNonClone.snapshot().getSnapshotId().getName(), is("non-clone"));
            assertTrue("non-clone should be completed because all shards failed on the departed node", updatedNonClone.state().completed());
            for (int shard = 0; shard < 3; shard++) {
                assertThat(
                    "shard " + shard + " should be FAILED",
                    updatedNonClone.shards().get(new ShardId(index, shard)).state(),
                    is(SnapshotsInProgress.ShardState.FAILED)
                );
            }

            final var updatedClone = updatedEntries.get(1);
            assertThat(updatedClone.snapshot().getSnapshotId().getName(), is("clone"));
            assertThat("clone should still be in progress", updatedClone.state(), is(SnapshotsInProgress.State.STARTED));

            final var shard0Status = updatedClone.shardSnapshotStatusByRepoShardId().get(new RepositoryShardId(indexId, 0));
            assertThat("clone shard 0 should be reassigned to local node", shard0Status.nodeId(), is(MASTER_NODE.getId()));
            assertThat("clone shard 0 should use the generation from the known failure", shard0Status.generation(), is(gen0));

            final var shard1Status = updatedClone.shardSnapshotStatusByRepoShardId().get(new RepositoryShardId(indexId, 1));
            assertThat("clone shard 1 should be reassigned to local node", shard1Status.nodeId(), is(MASTER_NODE.getId()));
            assertThat("clone shard 1 should use the generation from the known failure", shard1Status.generation(), is(gen1));

            assertThat("snapshotFinalizer should fire for the completed non-clone", finalized, hasSize(1));
            assertTrue("finalized snapshot should be completed", finalized.getFirst().state().completed());
            assertThat("clonesStarter always called with updated state", cloned.get(), equalTo(updatedSnapshotsInProgress));
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

    private static ClusterService createClusterService(DeterministicTaskQueue deterministicTaskQueue) {
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var settings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "test")
            .put(Node.NODE_NAME_SETTING.getKey(), "test_node")
            .put(ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.ZERO)
            .build();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        final var masterService = new MasterService(
            settings,
            clusterSettings,
            threadPool,
            new TaskManager(settings, threadPool, emptySet()),
            MeterRegistry.NOOP
        ) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };

        final var clusterApplierService = new ClusterApplierService("test_node", settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };

        final var initialState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(MASTER_NODE).localNodeId(MASTER_NODE.getId()).masterNodeId(MASTER_NODE.getId()))
            .putCompatibilityVersions(MASTER_NODE.getId(), CompatibilityVersionsUtils.staticCurrent())
            .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
            .build();
        clusterApplierService.setInitialState(initialState);
        clusterApplierService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());

        masterService.setClusterStateSupplier(clusterApplierService::state);
        masterService.setClusterStatePublisher((event, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(event);
            ackListener.onCommit(TimeValue.ZERO);
            clusterApplierService.onNewClusterState(
                "mock_publish[" + event.getSummary() + "]",
                event::getNewState,
                ActionTestUtils.assertNoFailureListener(ignored -> {
                    ackListener.onNodeAck(MASTER_NODE, null);
                    publishListener.onResponse(null);
                })
            );
        });

        final var clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);
        clusterService.start();
        threadPool.getThreadContext().markAsSystemContext();
        return clusterService;
    }

    private static void updateClusterState(
        ClusterService clusterService,
        DeterministicTaskQueue deterministicTaskQueue,
        UnaryOperator<ClusterState> updater
    ) {
        final var completed = new AtomicBoolean();
        // noinspection deprecation
        clusterService.submitUnbatchedStateUpdateTask("test-setup", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return updater.apply(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("test setup should not fail", e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                assertTrue(completed.compareAndSet(false, true));
            }
        });
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(completed.get());
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
}
