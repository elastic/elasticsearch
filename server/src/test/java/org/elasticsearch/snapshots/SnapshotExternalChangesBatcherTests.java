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
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.is;

public class SnapshotExternalChangesBatcherTests extends ESTestCase {

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

    public void testExternalChangesTaskResubmissionOnSuccess() {
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

    private static MasterService createMasterService(ThreadPool threadPool) {
        final var localNode = DiscoveryNodeUtils.builder("node1").roles(new HashSet<>(DiscoveryNodeRole.roles())).build();
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
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .putCompatibilityVersions(localNode.getId(), CompatibilityVersionsUtils.staticCurrent())
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
}
