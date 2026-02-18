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
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
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
import java.util.Objects;
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
            taskContext.success(batcher.get()::onTaskSuccess);
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
            taskContext.success(batcher.get()::onTaskSuccess);
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
                taskContext.success(batcher.get()::onTaskSuccess);
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

    public void testExternalChangeClusterStateUpdate() throws Exception {
        final var deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var repoFail = "repo-failing-snapshot";
        final var repoPromoted = "repo-promoted-snapshot";
        final var repoClone = "repo-clone";
        final var repoStartDeletion = "repo-started-deletion";

        final var anotherNodeId = uuid();
        assert !Objects.equals(MASTER_NODE, anotherNodeId);

        // Capture callbacks
        final var finalized = new ArrayList<SnapshotsInProgress.Entry>();
        final var cloned = new AtomicReference<SnapshotsInProgress>();
        final var deleted = new HashMap<SnapshotDeletionsInProgress.Entry, IndexVersion>();
        final var isInitializingClone = randomBoolean();

        try (var masterService = createMasterService(threadPool)) {
            final ClusterStateTaskExecutor<SnapshotExternalChangesBatcher.Task> noopExecutor = ctx -> ctx.initialState();
            final var queue = masterService.createTaskQueue("test", Priority.NORMAL, noopExecutor);
            final var batcher = new SnapshotExternalChangesBatcher(
                queue,
                s -> isInitializingClone,
                (entry, _metadata) -> finalized.add(entry),
                cloned::set,
                deleted::put
            );

            var snapshotsInProgress = SnapshotsInProgress.EMPTY;
            var clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(discoveryNodes(MASTER_NODE.getId()))
                .putCustom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);

            // STARTED snapshot with a single WAITING shard and no routing entry -> snapshotFinalizer
            final var includeCompletingSnapshot = randomBoolean();
            ShardId completingSnapshotShardId = null;
            if (includeCompletingSnapshot) {
                final var indexNameCompletingSnapshot = randomIndexName();
                final var indexCompletingSnapshot = new Index(indexNameCompletingSnapshot, uuid());
                completingSnapshotShardId = new ShardId(indexCompletingSnapshot, 0);
                snapshotsInProgress = snapshotsInProgress.withAddedEntry(
                    snapshotEntry(
                        snapshot(repoFail, "completing-snapshot"),
                        Map.of(indexNameCompletingSnapshot, new IndexId(indexNameCompletingSnapshot, uuid())),
                        Map.of(
                            completingSnapshotShardId,
                            new SnapshotsInProgress.ShardSnapshotStatus(
                                MASTER_NODE.getId(),
                                SnapshotsInProgress.ShardState.WAITING,
                                ShardGeneration.newGeneration(random())
                            )
                        )
                    )
                );
            }
            // STARTED snapshot with a WAITING shard whose routing primary has just started -> promoted to INIT.
            final var includePromotedSnapshot = randomBoolean();
            final var indexNamePromotedSnapshot = randomIndexName();
            final var indexPromotedSnapshot = new Index(indexNamePromotedSnapshot, uuid());
            final var routedShard = new ShardId(indexPromotedSnapshot, 0);
            if (includePromotedSnapshot) {
                // Routing table entry required so the WAITING shard in scenario B can be promoted.
                clusterStateBuilder = clusterStateBuilder.putRoutingTable(
                    ProjectId.DEFAULT,
                    RoutingTable.builder()
                        .add(
                            IndexRoutingTable.builder(indexPromotedSnapshot)
                                .addIndexShard(
                                    IndexShardRoutingTable.builder(routedShard)
                                        .addShard(
                                            TestShardRouting.newShardRouting(routedShard, anotherNodeId, true, ShardRoutingState.STARTED)
                                        )
                                )
                        )
                        .build()
                );
                snapshotsInProgress = snapshotsInProgress.withAddedEntry(
                    snapshotEntry(
                        snapshot(repoPromoted, "will-promote-snapshot"),
                        Map.of(indexNamePromotedSnapshot, new IndexId(indexNamePromotedSnapshot, uuid())),
                        Map.of(
                            routedShard,
                            new SnapshotsInProgress.ShardSnapshotStatus(
                                MASTER_NODE.getId(),
                                SnapshotsInProgress.ShardState.WAITING,
                                ShardGeneration.newGeneration(random())
                            )
                        )
                    )
                );
            }

            // Clone entry. If snapshot not tracked in isInitializingClone → dropped from cluster state.
            final var indexNameInitClone = randomIndexName();
            final var cloneSnapshot = SnapshotsInProgress.startClone(
                snapshot(repoClone, "clone"),
                new SnapshotId("source-clone", uuid()),
                Map.of(indexNameInitClone, new IndexId(indexNameInitClone, uuid())),
                1L,
                System.currentTimeMillis(),
                IndexVersion.current()
            );
            snapshotsInProgress = snapshotsInProgress.withAddedEntry(cloneSnapshot);

            // STARTED deletion → deletionStarter.
            final var includeDeletingSnapshot = randomBoolean();
            final var deletionEntry = SnapshotDeletionsInProgress.of(
                List.of(
                    new SnapshotDeletionsInProgress.Entry(
                        ProjectId.DEFAULT,
                        repoStartDeletion,
                        List.of(new SnapshotId("snap-to-delete", uuid())),
                        System.currentTimeMillis(),
                        1L,
                        SnapshotDeletionsInProgress.State.STARTED
                    )
                )
            );
            if (includeDeletingSnapshot) {
                clusterStateBuilder = clusterStateBuilder.putCustom(SnapshotDeletionsInProgress.TYPE, deletionEntry);
            }
            clusterStateBuilder = clusterStateBuilder.putCustom(SnapshotsInProgress.TYPE, snapshotsInProgress);

            // Execute external changes
            final var nodesChanged = randomBoolean();
            batcher.processExternalChanges(nodesChanged, true);
            final var updatedState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterStateBuilder.build(),
                batcher.new Executor(),
                List.of(batcher.new Task())
            );
            final var updatedSnapshotsInProgress = SnapshotsInProgress.get(updatedState);

            // Verify results
            assertNotNull("clonesStarter always invoked", cloned.get());

            final var updatedStateCompletingSnapshot = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoFail);
            if (includeCompletingSnapshot) {
                assertThat("snapshotFinalizer should fire for the completed snapshot", finalized, hasSize(1));
                assertTrue("completed snapshot should be in a terminal state", finalized.get(0).state().completed());
                assertThat("completed snapshot should remain in cluster state until finalized", updatedStateCompletingSnapshot, hasSize(1));
            } else {
                assertThat("snapshotFinalizer should not fire when no completing snapshot was included", finalized, empty());
                assertThat("unexpected snapshot in repoFail", updatedStateCompletingSnapshot, empty());
            }

            final var updatedStatePromotedSnapshot = updatedSnapshotsInProgress.forRepo(ProjectId.DEFAULT, repoPromoted);
            if (includePromotedSnapshot) {
                assertThat("promoted snapshot should be in cluster state", updatedStatePromotedSnapshot, hasSize(1));
                final var entry = updatedStatePromotedSnapshot.getFirst();
                assertThat(
                    "snapshot should still be STARTED while the shard is in INIT (not yet complete)",
                    entry.state(),
                    is(SnapshotsInProgress.State.STARTED)
                );
                assertThat(
                    "WAITING shard should be promoted to INIT in the result state",
                    entry.shards().get(routedShard).state(),
                    is(SnapshotsInProgress.ShardState.INIT)
                );
                assertThat(
                    "promoted shard should be assigned to the routing primary",
                    entry.shards().get(routedShard).nodeId(),
                    is(anotherNodeId)
                );
            } else {
                assertThat("unexpected snapshot in repoPromoted", updatedStatePromotedSnapshot, empty());
            }

            assertThat(
                "clone should be dropped from cluster state only if isInitializingClone is false",
                SnapshotsInProgress.get(updatedState).forRepo(ProjectId.DEFAULT, repoClone),
                isInitializingClone ? equalTo(List.of(cloneSnapshot)) : empty()
            );

            final var updatedStateDeletingSnapshot = SnapshotDeletionsInProgress.get(updatedState).getEntries();
            if (includeDeletingSnapshot) {
                assertThat("deletionStarter should fire for a STARTED deletion in cluster state", deleted.entrySet(), hasSize(1));
                assertNotNull(deleted.get(deletionEntry.getEntries().getFirst()));
                assertThat("deletion should remain in result state", updatedStateDeletingSnapshot, hasSize(1));
                assertThat(
                    "deletion should still be STARTED",
                    updatedStateDeletingSnapshot.getFirst().state(),
                    is(SnapshotDeletionsInProgress.State.STARTED)
                );
            } else {
                assertThat("deletionStarter should not fire when no deletion in cluster state", deleted.entrySet(), empty());
                assertThat("unexpected deletion in updated state", updatedStateDeletingSnapshot, empty());
            }
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
