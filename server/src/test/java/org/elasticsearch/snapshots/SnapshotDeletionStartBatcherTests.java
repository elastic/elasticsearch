/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.ProjectScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.RepositoriesStats;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;

import static org.elasticsearch.cluster.SnapshotDeletionsInProgress.State.STARTED;
import static org.elasticsearch.cluster.SnapshotDeletionsInProgress.State.WAITING;
import static org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus.UNASSIGNED_QUEUED;
import static org.elasticsearch.cluster.SnapshotsInProgress.State.ABORTED;
import static org.elasticsearch.cluster.SnapshotsInProgress.State.SUCCESS;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotDeletionStartBatcherTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private ClusterService clusterService;
    private ClusterStatePublisher defaultClusterStatePublisher;
    private RepositoriesService repositoriesService;
    private SnapshotDeletionStartBatcher batcher;
    private String repoName;

    private Set<SnapshotId> snapshotAbortNotifications;
    private Set<SnapshotId> snapshotEndNotifications;
    private Map<String, List<ActionListener<Void>>> completionHandlers;
    private List<SnapshotDeletionsInProgress.Entry> startedDeletions;
    private DiscoveryNode localNode;

    @Before
    public void createServices() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        final var threadPool = deterministicTaskQueue.getThreadPool();

        final var settings = Settings.builder()
            // disable infinitely-retrying watchdog so we can completely drain the task queue
            .put(ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.ZERO)
            .build();
        final var clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var projectScopedSettings = new ProjectScopedSettings(settings, Set.of());

        localNode = DiscoveryNodeUtils.create(randomIdentifier("node-id-"));
        final var initialState = ClusterState.builder(new ClusterName(randomIdentifier("cluster-")))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()).build())
            .build();

        final var masterService = new MasterService(
            settings,
            clusterSettings,
            threadPool,
            new TaskManager(settings, threadPool, Set.of()),
            MeterRegistry.NOOP
        ) {
            @Override
            protected ExecutorService createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };

        final var clusterApplierService = new ClusterApplierService(randomIdentifier("node-"), settings, clusterSettings, threadPool) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };
        clusterApplierService.setInitialState(initialState);
        clusterApplierService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());

        defaultClusterStatePublisher = (clusterStatePublicationEvent, publishListener, ackListener) -> {
            ClusterServiceUtils.setAllElapsedMillis(clusterStatePublicationEvent);
            ackListener.onCommit(TimeValue.ZERO);
            clusterApplierService.onNewClusterState(
                "mock_publish_to_self[" + clusterStatePublicationEvent.getSummary() + "]",
                clusterStatePublicationEvent::getNewState,
                ActionTestUtils.assertNoFailureListener(ignored -> {
                    logger.info("--> ack and complete [{}]", clusterStatePublicationEvent.getNewState().version());
                    ackListener.onNodeAck(localNode, null);
                    publishListener.onResponse(null);
                })
            );
        };

        masterService.setClusterStateSupplier(clusterApplierService::state);
        masterService.setClusterStatePublisher(defaultClusterStatePublisher);

        clusterService = new ClusterService(settings, clusterSettings, projectScopedSettings, masterService, clusterApplierService);
        clusterService.start();

        final var repoType = randomIdentifier("repo-type-");
        repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            Map.of(repoType, TestRepository::new),
            Map.of(),
            threadPool,
            new NodeClient(settings, threadPool, DefaultProjectResolver.INSTANCE),
            List.of(),
            SnapshotMetrics.NOOP
        );
        repositoriesService.start();

        threadPool.getThreadContext().markAsSystemContext();

        repoName = randomIdentifier("repo-");
        safeAwait(l -> {
            repositoriesService.registerRepository(
                ProjectId.DEFAULT,
                new PutRepositoryRequest(
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    repoName
                ).type(repoType).verify(false),
                l.map(ignored -> null)
            );
            deterministicTaskQueue.runAllTasksInTimeOrder();
        });

        snapshotAbortNotifications = new HashSet<>();
        snapshotEndNotifications = new HashSet<>();
        completionHandlers = new HashMap<>();
        startedDeletions = new ArrayList<>();

        batcher = new SnapshotDeletionStartBatcher(
            repositoriesService,
            clusterService,
            threadPool,
            ProjectId.DEFAULT,
            repoName,
            snapshot -> {
                assertEquals(ProjectId.DEFAULT, snapshot.getProjectId());
                assertEquals(repoName, snapshot.getRepository());
                snapshotAbortNotifications.add(snapshot.getSnapshotId());
            },
            (entry, ignoredMetadata, ignoredRepositoryData) -> {
                assertEquals(ProjectId.DEFAULT, entry.projectId());
                assertEquals(repoName, entry.repository());
                snapshotEndNotifications.add(entry.snapshot().getSnapshotId());
            },
            (deletionUuid, itemListener) -> {
                if (deletionUuid == null) {
                    itemListener.onResponse(null);
                } else {
                    completionHandlers.computeIfAbsent(deletionUuid, ignored -> new ArrayList<>()).add(itemListener);
                }
            },
            (projectId, repositoryName, deleteEntry, ignoredRepositoryData, ignoredMinNodeVersion) -> {
                assertEquals(ProjectId.DEFAULT, projectId);
                assertEquals(repoName, repositoryName);
                startedDeletions.add(deleteEntry);
            }
        );

        logger.info("----> services all created");
    }

    private static class TestRepository extends AbstractLifecycleComponent implements Repository {
        private RepositoryMetadata metadata;

        SubscribableListener<RepositoryData> repositoryDataListener = SubscribableListener.newSucceeded(RepositoryData.EMPTY);

        TestRepository(ProjectId projectId, RepositoryMetadata metadata) {
            assertEquals(ProjectId.DEFAULT, projectId);
            this.metadata = metadata;
        }

        @Override
        public ProjectId getProjectId() {
            return ProjectId.DEFAULT;
        }

        @Override
        public RepositoryMetadata getMetadata() {
            return metadata;
        }

        @Override
        public void getSnapshotInfo(
            Collection<SnapshotId> snapshotIds,
            boolean abortOnFailure,
            BooleanSupplier isCancelled,
            CheckedConsumer<SnapshotInfo, Exception> consumer,
            ActionListener<Void> listener
        ) {
            throw new AssertionError("should not be called");
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId, boolean fromProjectMetadata) {
            throw new AssertionError("should not be called");
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
            repositoryDataListener.addListener(listener);
        }

        @Override
        public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void deleteSnapshots(
            Collection<SnapshotId> snapshotIds,
            long repositoryDataGeneration,
            IndexVersion minimumNodeVersion,
            ActionListener<RepositoryData> repositoryDataUpdateListener,
            Runnable onCompletion
        ) {
            throw new AssertionError("should not be called");
        }

        @Override
        public String startVerification() {
            throw new AssertionError("should not be called");
        }

        @Override
        public void endVerification(String verificationToken) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {
            throw new AssertionError("should not be called");
        }

        @Override
        public boolean isReadOnly() {
            throw new AssertionError("should not be called");
        }

        @Override
        public void snapshotShard(SnapshotShardContext snapshotShardContext) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void restoreShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId snapshotShardId,
            RecoveryState recoveryState,
            ActionListener<Void> listener
        ) {
            throw new AssertionError("should not be called");
        }

        @Override
        public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void updateState(ClusterState state) {
            metadata = Objects.requireNonNull(
                RepositoriesMetadata.get(state.projectState(ProjectId.DEFAULT).metadata()).repository(metadata.name())
            );
        }

        @Override
        public void cloneShardSnapshot(
            SnapshotId source,
            SnapshotId target,
            RepositoryShardId shardId,
            ShardGeneration shardGeneration,
            ActionListener<ShardSnapshotResult> listener
        ) {
            throw new AssertionError("should not be called");
        }

        @Override
        public void awaitIdle() {
            throw new AssertionError("should not be called");
        }

        @Override
        public LongWithAttributes getShardSnapshotsInProgress() {
            throw new AssertionError("should not be called");
        }

        @Override
        public RepositoriesStats.SnapshotStats getSnapshotStats() {
            throw new AssertionError("should not be called");
        }

        @Override
        public void doStart() {}

        @Override
        public void doStop() {}

        @Override
        public void doClose() {}
    }

    private Snapshot randomSnapshot() {
        return randomSnapshot("snapshot-");
    }

    private Snapshot randomSnapshot(String prefix) {
        return new Snapshot(ProjectId.DEFAULT, repoName, new SnapshotId(randomIdentifier(prefix), randomUUID()));
    }

    private void submitClusterStateUpdateTask(String source, ClusterStateUpdateTask clusterStateUpdateTask) {
        // noinspection deprecation
        clusterService.submitUnbatchedStateUpdateTask(source, clusterStateUpdateTask);
    }

    private void updateClusterState(UnaryOperator<ClusterState> clusterStateOperator) {
        final var completed = new AtomicBoolean();
        submitClusterStateUpdateTask("test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return clusterStateOperator.apply(currentState);
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                assertTrue(completed.compareAndSet(false, true));
            }
        });
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(completed.get());
    }

    private void setSnapshotsInProgress(SnapshotsInProgress.Entry... entries) {
        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    SnapshotsInProgress.TYPE,
                    SnapshotsInProgress.EMPTY.createCopyWithUpdatedEntriesForRepo(ProjectId.DEFAULT, repoName, List.of(entries))
                )
                .build()
        );
    }

    private void addCompleteSnapshot(Snapshot snapshot) {
        final var repository = asInstanceOf(
            TestRepository.class,
            repositoriesService.repository(snapshot.getProjectId(), snapshot.getRepository())
        );
        final var repositoryDataListener = new SubscribableListener<RepositoryData>();
        repository.getRepositoryData(EsExecutors.DIRECT_EXECUTOR_SERVICE, repositoryDataListener);
        assertTrue(repositoryDataListener.isDone());
        repository.repositoryDataListener = SubscribableListener.newSucceeded(
            safeAwait(repositoryDataListener).addSnapshot(
                snapshot.getSnapshotId(),
                RepositoryData.SnapshotDetails.EMPTY,
                FinalizeSnapshotContext.UpdatedShardGenerations.EMPTY,
                Map.of(),
                Map.of()
            )
        );
    }

    private SubscribableListener<Void> startDeletion(String... snapshotNames) {
        return SubscribableListener.newForked(
            l -> batcher.startDeletion(snapshotNames, true, MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT, l)
        );
    }

    private SubscribableListener<Void> startDeletionNoWait(String... snapshotNames) {
        return SubscribableListener.newForked(
            l -> batcher.startDeletion(snapshotNames, false, MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT, l)
        );
    }

    public void testUnknownRepository() {
        final var unregisterFuture = SubscribableListener.<AcknowledgedResponse>newForked(
            l -> repositoriesService.unregisterRepository(
                ProjectId.DEFAULT,
                new DeleteRepositoryRequest(
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                    repoName
                ),
                l
            )
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(unregisterFuture.isDone());
        safeAwait(unregisterFuture);

        final var deletionFuture = startDeletion(randomSnapshotName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertThat(
            asInstanceOf(RepositoryMissingException.class, safeAwaitFailure(deletionFuture)).getMessage(),
            equalTo(Strings.format("[%s] missing", repoName))
        );
    }

    public void testDeleteMissingSnapshots() {
        final var snapshotName = randomSnapshotName();
        final var deletionFuture = startDeletion(snapshotName);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertThat(
            asInstanceOf(SnapshotMissingException.class, safeAwaitFailure(deletionFuture)).getMessage(),
            allOf(containsString(repoName), containsString(snapshotName))
        );
    }

    public void testTimeoutWhileWaiting() {
        final var keepGoing = new AtomicBoolean(true);
        class LoopingTask extends ClusterStateUpdateTask {
            LoopingTask() {
                super(Priority.HIGH);
            }

            @Override
            public ClusterState execute(ClusterState currentState) {
                if (deterministicTaskQueue.hasDeferredTasks()) {
                    deterministicTaskQueue.advanceTime();
                }
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                maybeEnqueueTask();
            }

            void maybeEnqueueTask() {
                if (keepGoing.get()) {
                    logger.info("--> submit looping task");
                    // noinspection deprecation
                    clusterService.submitUnbatchedStateUpdateTask("looping task", LoopingTask.this);
                }
            }
        }
        new LoopingTask().maybeEnqueueTask();

        final var snapshotName = randomSnapshotName();
        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(new String[] { snapshotName }, true, TimeValue.THIRTY_SECONDS, l)
        );
        deletionFuture.addListener(ActionTestUtils.assertNoSuccessListener(e -> {
            assertThat(
                asInstanceOf(ElasticsearchTimeoutException.class, e).getMessage(),
                allOf(containsString(repoName), containsString("did not start snapshot deletion within [30s]"))
            );
            keepGoing.set(false);
        }));
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertFalse(keepGoing.get()); // implies we checked the exception type & message
    }

    public void testDeleteNotStartedSnapshot() {
        final var snapshot = randomSnapshot();
        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var shardId = new ShardId(new Index(repoIndex.getName(), randomUUID()), 0);

        setSnapshotsInProgress(
            SnapshotsInProgress.startedEntry(
                snapshot,
                randomBoolean(),
                randomBoolean(),
                Map.of(repoIndex.getName(), repoIndex),
                List.of(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(shardId, UNASSIGNED_QUEUED),
                Map.of(),
                IndexVersionUtils.randomVersion(),
                List.of()
            )
        );

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);

        assertTrue(startedDeletions.isEmpty());
        assertTrue(snapshotEndNotifications.isEmpty());
        assertThat(snapshotAbortNotifications, equalTo(Set.of(snapshot.getSnapshotId())));
        assertTrue(completionHandlers.isEmpty());

        assertEquals(0, SnapshotsInProgress.get(clusterService.state()).count());
        assertThat(SnapshotDeletionsInProgress.get(clusterService.state()).getEntries(), hasSize(0));
    }

    public void testDeleteInProgressSnapshot() {
        final var snapshot = randomSnapshot();
        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var shardId = new ShardId(new Index(repoIndex.getName(), randomUUID()), 0);
        final var shardGeneration = ShardGeneration.newGeneration();
        setSnapshotsInProgress(
            SnapshotsInProgress.startedEntry(
                snapshot,
                randomBoolean(),
                randomBoolean(),
                Map.of(repoIndex.getName(), repoIndex),
                List.of(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(localNode.getId(), shardGeneration)),
                Map.of(),
                IndexVersionUtils.randomVersion(),
                List.of()
            )
        );

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletionFuture.isDone());

        assertTrue(startedDeletions.isEmpty());
        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
        assertEquals(1, snapshotsInProgress.count());
        final var snapshotEntry = Objects.requireNonNull(snapshotsInProgress.snapshot(snapshot));
        assertEquals(ABORTED, snapshotEntry.state());
        assertEquals(
            Map.of(
                shardId,
                new SnapshotsInProgress.ShardSnapshotStatus(
                    localNode.getId(),
                    SnapshotsInProgress.ShardState.ABORTED,
                    shardGeneration,
                    "aborted by snapshot deletion"
                )
            ),
            snapshotEntry.shards()
        );

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(WAITING, deletionEntry.state());

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid()));
        assertThat(listeners, hasSize(1));
        listeners.getFirst().onResponse(null);

        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);
    }

    public void testDeleteCompletesInProgressSnapshot() {
        final var snapshot = randomSnapshot();
        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var index = new Index(repoIndex.getName(), randomUUID());

        final var shardId0 = new ShardId(index, 0);
        final var shard0Status = randomBoolean()
            ? SnapshotsInProgress.ShardSnapshotStatus.success(
                localNode.getId(),
                new ShardSnapshotResult(ShardGeneration.newGeneration(), ByteSizeValue.ZERO, 1)
            )
            : new SnapshotsInProgress.ShardSnapshotStatus(
                localNode.getId(),
                SnapshotsInProgress.ShardState.FAILED,
                ShardGeneration.newGeneration(),
                "test"
            );

        final var shardId1 = new ShardId(index, 1);

        setSnapshotsInProgress(
            SnapshotsInProgress.startedEntry(
                snapshot,
                randomBoolean(),
                randomBoolean(),
                Map.of(repoIndex.getName(), repoIndex),
                List.of(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(shardId0, shard0Status, shardId1, UNASSIGNED_QUEUED),
                Map.of(),
                IndexVersionUtils.randomVersion(),
                List.of()
            )
        );

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletionFuture.isDone());

        assertTrue(startedDeletions.isEmpty());
        assertThat(snapshotEndNotifications, equalTo(Set.of(snapshot.getSnapshotId())));
        assertTrue(snapshotAbortNotifications.isEmpty());

        final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
        assertEquals(1, snapshotsInProgress.count());
        final var snapshotEntry = Objects.requireNonNull(snapshotsInProgress.snapshot(snapshot));
        assertEquals(SUCCESS, snapshotEntry.state());
        assertEquals(
            Map.of(
                shardId0,
                shard0Status,
                shardId1,
                new SnapshotsInProgress.ShardSnapshotStatus(
                    null,
                    SnapshotsInProgress.ShardState.FAILED,
                    null,
                    "aborted by snapshot deletion"
                )
            ),
            snapshotEntry.shards()
        );

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(WAITING, deletionEntry.state());

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid()));
        assertThat(listeners, hasSize(1));
        listeners.getFirst().onResponse(null);

        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);
    }

    public void testDeleteCompletedSnapshot() {
        final var snapshot = randomSnapshot();
        addCompleteSnapshot(snapshot);

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletionFuture.isDone());

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        assertThat(startedDeletions, hasSize(1));
        assertThat(startedDeletions.getFirst().snapshots(), contains(snapshot.getSnapshotId()));

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(STARTED, deletionEntry.state());
        assertThat(deletionEntry.snapshots(), contains(snapshot.getSnapshotId()));

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid()));
        assertThat(listeners, hasSize(1));
        assertFalse(deletionFuture.isDone());
        listeners.getFirst().onResponse(null);

        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);
    }

    public void testDeleteCompletedSnapshotWithoutWaitingForCompletion() {
        final var snapshot = randomSnapshot();
        addCompleteSnapshot(snapshot);

        final var deletionFuture = startDeletionNoWait(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isSuccess());

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());
        assertTrue(completionHandlers.isEmpty());

        assertThat(startedDeletions, hasSize(1));
        assertThat(startedDeletions.getFirst().snapshots(), contains(snapshot.getSnapshotId()));

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(STARTED, deletionEntry.state());
        assertThat(deletionEntry.snapshots(), contains(snapshot.getSnapshotId()));
    }

    public void testNotifyOnPublishFailure() {
        final var snapshot = randomSnapshot();
        addCompleteSnapshot(snapshot);

        clusterService.getMasterService()
            .setClusterStatePublisher(
                (ignoredEvent, listener, ignoredAckListener) -> listener.onFailure(new NotMasterException("simulated"))
            );

        final var deletionFuture = SubscribableListener.<Void>newForked(
            l -> batcher.startDeletion(
                new String[] { snapshot.getSnapshotId().getName() },
                randomBoolean() /* doesn't matter if waiting for completion or not */,
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                l
            )
        );
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertThat(asInstanceOf(NotMasterException.class, safeAwaitFailure(deletionFuture)).getMessage(), equalTo("simulated"));
    }

    public void testBatching() {
        final var snapshots = new Snapshot[3];
        for (int i = 0; i < 3; i++) {
            snapshots[i] = randomSnapshot();
            addCompleteSnapshot(snapshots[i]);
        }

        final var futures = new ArrayList<SubscribableListener<Void>>();
        for (final Snapshot snapshot : snapshots) {
            futures.add(startDeletion(snapshot.getSnapshotId().getName()));
        }
        deterministicTaskQueue.runAllTasksInTimeOrder();
        futures.forEach(l -> assertFalse(l.isDone()));

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        assertThat(startedDeletions, hasSize(1));

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(2));
        final var startedDeletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(STARTED, startedDeletionEntry.state());
        final var waitingDeletionEntry = deletionsInProgress.getEntries().getLast();
        assertEquals(WAITING, waitingDeletionEntry.state());
        assertThat(startedDeletionEntry.snapshots(), contains(snapshots[0].getSnapshotId()));
        assertThat(waitingDeletionEntry.snapshots(), containsInAnyOrder(snapshots[1].getSnapshotId(), snapshots[2].getSnapshotId()));

        final var deletionUuids = Set.of(startedDeletionEntry.uuid(), waitingDeletionEntry.uuid());
        assertThat(completionHandlers.keySet(), equalTo(deletionUuids));

        final var listeners = deletionUuids.stream()
            .map(u -> Objects.requireNonNull(completionHandlers.get(u)))
            .flatMap(Collection::stream)
            .toList();
        assertThat(listeners, hasSize(3));
        listeners.forEach(l -> l.onResponse(null));
        futures.forEach(l -> assertTrue(l.isDone()));
        futures.forEach(ESTestCase::safeAwait);
    }

    public void testRepositoryDataFailure() {
        final var repository = asInstanceOf(TestRepository.class, repositoriesService.repository(ProjectId.DEFAULT, repoName));
        final var message = randomIdentifier("failure-message-");
        repository.repositoryDataListener = SubscribableListener.newFailed(new RuntimeException(message));

        final var deletionFuture = startDeletion(randomSnapshotName());
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertTrue(deletionFuture.isDone());
        assertThat(safeAwaitFailure(deletionFuture).getMessage(), equalTo(message));

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());
        assertTrue(startedDeletions.isEmpty());
        assertTrue(completionHandlers.isEmpty());
    }

    public void testContinuesWithNextBatchAfterFailure() {
        final var snapshots = new Snapshot[3];
        for (int i = 0; i < 3; i++) {
            snapshots[i] = randomSnapshot();
            addCompleteSnapshot(snapshots[i]);
        }

        final var message = randomIdentifier("failure-message-");

        clusterService.getMasterService().setClusterStatePublisher(new ClusterStatePublisher() {
            private boolean failedFirstBatch;

            @Override
            public void publish(
                ClusterStatePublicationEvent clusterStatePublicationEvent,
                ActionListener<Void> publishListener,
                AckListener ackListener
            ) {
                if (failedFirstBatch) {
                    defaultClusterStatePublisher.publish(clusterStatePublicationEvent, publishListener, ackListener);
                } else {
                    failedFirstBatch = true;
                    assertThat(
                        clusterStatePublicationEvent.getSummary().toString(),
                        allOf(
                            startsWith("snapshot-deletion-start[default/"),
                            containsString(repoName),
                            containsString("[1]"),
                            containsString(snapshots[0].getSnapshotId().getName())
                        )
                    );
                    publishListener.onFailure(new NotMasterException(message));
                }
            }
        });

        final var futures = new ArrayList<SubscribableListener<Void>>();
        for (final Snapshot snapshot : snapshots) {
            futures.add(startDeletion(snapshot.getSnapshotId().getName()));
        }
        deterministicTaskQueue.runAllTasksInTimeOrder();
        final var firstFuture = futures.removeFirst();
        assertTrue(firstFuture.isDone());
        assertThat(asInstanceOf(NotMasterException.class, safeAwaitFailure(firstFuture)).getMessage(), equalTo(message));
        futures.forEach(l -> assertFalse(l.isDone()));

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        assertThat(startedDeletions, hasSize(1));

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(STARTED, deletionEntry.state());
        assertThat(deletionEntry.snapshots(), containsInAnyOrder(snapshots[1].getSnapshotId(), snapshots[2].getSnapshotId()));

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = completionHandlers.get(deletionEntry.uuid());
        assertThat(listeners, hasSize(2));
        listeners.forEach(l -> l.onResponse(null));
        futures.forEach(l -> assertTrue(l.isDone()));
        futures.forEach(ESTestCase::safeAwait);
    }

    public void testRejectsDeletionOfCloneSource() {
        final var snapshot = randomSnapshot();
        addCompleteSnapshot(snapshot);

        final var clone = randomSnapshot("clone-");
        final var indexName = randomIndexName();

        setSnapshotsInProgress(
            SnapshotsInProgress.startClone(
                clone,
                snapshot.getSnapshotId(),
                Map.of(indexName, new IndexId(indexName, randomUUID())),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                IndexVersionUtils.randomVersion()
            )
        );

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertThat(
            safeAwaitFailure(deletionFuture).getMessage(),
            allOf(containsString(snapshot.toString()), containsString("cannot delete snapshot while it is being cloned"))
        );

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());
        assertTrue(startedDeletions.isEmpty());
        assertTrue(completionHandlers.isEmpty());
    }

    public void testRejectsDeletionOfRestoreSource() {
        final var snapshot = randomSnapshot();
        addCompleteSnapshot(snapshot);

        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    RestoreInProgress.TYPE,
                    new RestoreInProgress.Builder().add(
                        new RestoreInProgress.Entry(
                            randomUUID(),
                            snapshot,
                            RestoreInProgress.State.INIT,
                            randomBoolean(),
                            List.of(),
                            Map.of()
                        )
                    ).build()
                )
                .build()
        );

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(deletionFuture.isDone());
        assertThat(
            safeAwaitFailure(deletionFuture).getMessage(),
            allOf(containsString(snapshot.toString()), containsString("cannot delete snapshot during a restore in progress"))
        );

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());
        assertTrue(startedDeletions.isEmpty());
        assertTrue(completionHandlers.isEmpty());
    }

    public void testIgnoresRestoreSourceInOtherRepositories() {
        final var snapshot = randomSnapshot();
        addCompleteSnapshot(snapshot);

        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    RestoreInProgress.TYPE,
                    new RestoreInProgress.Builder()
                        // same repo name, different project
                        .add(
                            new RestoreInProgress.Entry(
                                randomUUID(),
                                new Snapshot(ProjectId.fromId(randomUUID()), repoName, snapshot.getSnapshotId()),
                                RestoreInProgress.State.INIT,
                                randomBoolean(),
                                List.of(),
                                Map.of()
                            )
                        )
                        // same project, different repo name
                        .add(
                            new RestoreInProgress.Entry(
                                randomUUID(),
                                new Snapshot(
                                    ProjectId.DEFAULT,
                                    randomValueOtherThan(repoName, ESTestCase::randomRepoName),
                                    snapshot.getSnapshotId()
                                ),
                                RestoreInProgress.State.INIT,
                                randomBoolean(),
                                List.of(),
                                Map.of()
                            )
                        )

                        .build()
                )
                .build()
        );

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletionFuture.isDone());

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        assertThat(startedDeletions, hasSize(1));
        assertThat(startedDeletions.getFirst().snapshots(), contains(snapshot.getSnapshotId()));

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(STARTED, deletionEntry.state());
        assertThat(deletionEntry.snapshots(), contains(snapshot.getSnapshotId()));

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid()));
        assertThat(listeners, hasSize(1));
        assertFalse(deletionFuture.isDone());
        listeners.getFirst().onResponse(null);

        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);
    }

    public void testResolvesWildcards() {
        final var finishedSnapshot = randomSnapshot("finished-");
        addCompleteSnapshot(finishedSnapshot);

        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var shardId = new ShardId(new Index(repoIndex.getName(), randomUUID()), 0);
        final var shardGeneration = ShardGeneration.newGeneration();

        final var runningSnapshot = randomSnapshot("running-");
        setSnapshotsInProgress(
            SnapshotsInProgress.startedEntry(
                runningSnapshot,
                randomBoolean(),
                randomBoolean(),
                Map.of(repoIndex.getName(), repoIndex),
                List.of(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(localNode.getId(), shardGeneration)),
                Map.of(),
                IndexVersionUtils.randomVersion(),
                List.of()
            )
        );

        final var deleteFinishedFuture = startDeletion("finished-*");
        final var deleteRunningFuture = startDeletion("running-*");
        final var deleteAllFuture = startDeletion("*-*");
        final var deleteNoneFuture = startDeletion("unused-prefix-*");

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deleteFinishedFuture.isDone());
        assertFalse(deleteRunningFuture.isDone());
        assertFalse(deleteAllFuture.isDone());
        assertTrue(deleteNoneFuture.isSuccess());

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());
        assertTrue(startedDeletions.isEmpty());

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(WAITING, deletionEntry.state());
        assertThat(deletionEntry.snapshots(), containsInAnyOrder(finishedSnapshot.getSnapshotId(), runningSnapshot.getSnapshotId()));

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        for (var listener : completionHandlers.get(deletionEntry.uuid())) {
            listener.onResponse(null);
        }

        assertTrue(deleteFinishedFuture.isSuccess());
        assertTrue(deleteRunningFuture.isSuccess());
        assertTrue(deleteAllFuture.isSuccess());
    }

    public void testPermitsNoOpDeleteOnReadOnlyRepository() {
        updateClusterState(currentState -> {
            final var projectMetadata = currentState.projectState(ProjectId.DEFAULT).metadata();
            final var oldRepoMetadata = RepositoriesMetadata.get(projectMetadata).repository(repoName);
            final var newRepoMetadata = oldRepoMetadata.withSettings(
                Settings.builder().put(oldRepoMetadata.settings()).put(BlobStoreRepository.READONLY_SETTING_KEY, true).build()
            );
            return ClusterState.builder(currentState)
                .putProjectMetadata(
                    ProjectMetadata.builder(projectMetadata)
                        .putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(List.of(newRepoMetadata)))
                )
                .build();
        });

        final var deleteFuture = startDeletion("*");

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertTrue(deleteFuture.isSuccess());
        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());
        assertTrue(startedDeletions.isEmpty());
        assertTrue(completionHandlers.isEmpty());
    }

    public void testPermitsNoOpDeleteWhileCleanupInProgress() {
        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    RepositoryCleanupInProgress.TYPE,
                    new RepositoryCleanupInProgress(
                        List.of(new RepositoryCleanupInProgress.Entry(ProjectId.DEFAULT, repoName, randomNonNegativeLong()))
                    )
                )
                .build()
        );

        final var deleteFuture = startDeletion("*");

        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertTrue(deleteFuture.isSuccess());
        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());
        assertTrue(startedDeletions.isEmpty());
        assertTrue(completionHandlers.isEmpty());
    }

    public void testSubscribesToRunningDeletionEvenIfBatchCreatesWaitingDeletion() {
        // Slightly tricky case where a batch of items has to create a new WAITING deletion behind a STARTED one, but some of its items
        // can just subscribe to the STARTED one directly.
        doSubscriptionTest(false);
    }

    public void testDuplicatesRunningDeletionIfNotAllSnapshotDeletionsRunning() {
        // Slightly tricky case where a batch of items has to create a new WAITING deletion behind a STARTED one, but some of its items
        // overlap the deletions in the STARTED one - in this case we must add those snapshots to the WAITING deletion too, so that we only
        // report success after deleting all the requested snapshots, even if the STARTED one fails but the WAITING one goes on to succeed.
        doSubscriptionTest(true);
    }

    private void doSubscriptionTest(boolean deleteBothRunningAndWaiting) {
        final Snapshot[] snapshots = new Snapshot[2];
        for (int i = 0; i < snapshots.length; i++) {
            snapshots[i] = randomSnapshot();
            addCompleteSnapshot(snapshots[i]);
        }

        final var snapshot0Deletion1 = startDeletion(snapshots[0].getSnapshotId().getName()); // singleton batch
        final var snapshot0Deletion2 = startDeletion(snapshots[0].getSnapshotId().getName());
        final var snapshot0Deletion3NoWait = startDeletionNoWait(snapshots[0].getSnapshotId().getName());
        final var snapshot1Deletion1 = deleteBothRunningAndWaiting
            ? startDeletion(snapshots[0].getSnapshotId().getName(), snapshots[1].getSnapshotId().getName())
            : startDeletion(snapshots[1].getSnapshotId().getName()); // batched with snapshot0Deletion2
        final var completedAssertions = new AtomicBoolean(false);

        // the first singleton batch is already enqueued with the master service, but the next batch is not; check the intermediate state:
        submitClusterStateUpdateTask("assert-singleton-batch", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
                assertThat(deletionsInProgress.getEntries(), hasSize(1));
                final var deletionEntry = deletionsInProgress.getEntries().getFirst();
                assertEquals(STARTED, deletionEntry.state());
                assertThat(deletionEntry.snapshots(), contains(snapshots[0].getSnapshotId()));
                assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
                assertThat(completionHandlers.get(deletionEntry.uuid()), hasSize(1));
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                final var finalCheckTask = new ClusterStateUpdateTask() {
                    private static final ElasticsearchException NOT_READY_EXCEPTION = new ElasticsearchException("not ready yet, retry");

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        if (newState == currentState) {
                            throw NOT_READY_EXCEPTION;
                        }

                        // verifying that the second batch is applied atomically, there are no more intermediate states

                        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
                        assertThat(deletionsInProgress.getEntries(), hasSize(2));

                        final var startedDeletionEntry = deletionsInProgress.getEntries().getFirst();
                        assertEquals(STARTED, startedDeletionEntry.state());
                        assertThat(startedDeletionEntry.snapshots(), contains(snapshots[0].getSnapshotId()));

                        final var waitingDeletionEntry = deletionsInProgress.getEntries().getLast();
                        assertEquals(WAITING, waitingDeletionEntry.state());
                        assertThat(
                            waitingDeletionEntry.snapshots(),
                            deleteBothRunningAndWaiting
                                ? containsInAnyOrder(snapshots[0].getSnapshotId(), snapshots[1].getSnapshotId())
                                : contains(snapshots[1].getSnapshotId())
                        );

                        assertThat(completionHandlers.keySet(), equalTo(Set.of(startedDeletionEntry.uuid(), waitingDeletionEntry.uuid())));
                        assertThat(completionHandlers.get(startedDeletionEntry.uuid()), hasSize(2));
                        assertThat(completionHandlers.get(waitingDeletionEntry.uuid()), hasSize(1));
                        return currentState;
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (e == NOT_READY_EXCEPTION) {
                            submitClusterStateUpdateTask("assert-second-batch", this);
                        } else {
                            fail(e);
                        }
                    }

                    @Override
                    public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                        assertTrue(completedAssertions.compareAndSet(false, true));
                    }
                };
                submitClusterStateUpdateTask("assert-second-batch", finalCheckTask);
            }
        });

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertTrue(completedAssertions.get());
        assertTrue(snapshot0Deletion3NoWait.isSuccess());

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(2));

        final var startedDeletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(STARTED, startedDeletionEntry.state());
        assertThat(startedDeletionEntry.snapshots(), contains(snapshots[0].getSnapshotId()));

        final var waitingDeletionEntry = deletionsInProgress.getEntries().getLast();
        assertEquals(WAITING, waitingDeletionEntry.state());
        assertThat(
            waitingDeletionEntry.snapshots(),
            deleteBothRunningAndWaiting
                ? containsInAnyOrder(snapshots[0].getSnapshotId(), snapshots[1].getSnapshotId())
                : contains(snapshots[1].getSnapshotId())
        );

        assertThat(completionHandlers.keySet(), equalTo(Set.of(startedDeletionEntry.uuid(), waitingDeletionEntry.uuid())));
        assertThat(completionHandlers.get(startedDeletionEntry.uuid()), hasSize(2));
        assertThat(completionHandlers.get(waitingDeletionEntry.uuid()), hasSize(1));

        for (var startedDeletionListener : completionHandlers.get(startedDeletionEntry.uuid())) {
            startedDeletionListener.onResponse(null);
        }
        assertTrue(snapshot0Deletion1.isSuccess());
        assertTrue(snapshot0Deletion2.isSuccess());
        assertFalse(snapshot1Deletion1.isDone());

        for (var waitingDeletionListener : completionHandlers.get(waitingDeletionEntry.uuid())) {
            waitingDeletionListener.onResponse(null);
        }
        assertTrue(snapshot1Deletion1.isSuccess());
    }

    public void testSubscribesToAlreadyDeletingSnapshots() {
        final var repoIndex = new IndexId(randomIndexName(), randomUUID());
        final var shardId = new ShardId(new Index(repoIndex.getName(), randomUUID()), 0);
        final var runningSnapshot = randomSnapshot("running-");
        final var completedSnapshot = randomSnapshot("completed-");

        addCompleteSnapshot(completedSnapshot);

        setSnapshotsInProgress(
            SnapshotsInProgress.startedEntry(
                runningSnapshot,
                randomBoolean(),
                randomBoolean(),
                Map.of(repoIndex.getName(), repoIndex),
                List.of(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(shardId, new SnapshotsInProgress.ShardSnapshotStatus(randomUUID(), ShardGeneration.newGeneration())),
                Map.of(),
                IndexVersionUtils.randomVersion(),
                List.of()
            )
        );

        updateClusterState(
            currentState -> ClusterState.builder(currentState)
                .putCustom(
                    SnapshotDeletionsInProgress.TYPE,
                    SnapshotDeletionsInProgress.of(
                        List.of(
                            new SnapshotDeletionsInProgress.Entry(
                                randomUniqueProjectId(),
                                repoName,
                                List.of(randomSnapshot("other-project-").getSnapshotId()),
                                randomNonNegativeLong(),
                                randomNonNegativeLong(),
                                WAITING
                            ),
                            new SnapshotDeletionsInProgress.Entry(
                                ProjectId.DEFAULT,
                                randomValueOtherThan(repoName, ESTestCase::randomRepoName),
                                List.of(randomSnapshot("other-repo-").getSnapshotId()),
                                randomNonNegativeLong(),
                                randomNonNegativeLong(),
                                WAITING
                            )
                        )
                    )
                )
                .build()
        );

        final var deletion1Future = startDeletion(runningSnapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletion1Future.isDone());

        final var snapshotsInProgress = SnapshotsInProgress.get(clusterService.state());
        assertEquals(1, snapshotsInProgress.count());
        final var snapshotEntry = Objects.requireNonNull(snapshotsInProgress.snapshot(runningSnapshot));
        assertEquals(ABORTED, snapshotEntry.state());

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(3));
        final var deletionEntry = deletionsInProgress.getEntries().getLast();
        assertEquals(ProjectId.DEFAULT, deletionEntry.projectId());
        assertEquals(repoName, deletionEntry.repoName());
        assertEquals(WAITING, deletionEntry.state());

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        assertThat(Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid())), hasSize(1));

        final var deletion2Future = startDeletion(runningSnapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletion2Future.isDone());

        assertSame(snapshotsInProgress, SnapshotsInProgress.get(clusterService.state()));
        assertSame(deletionsInProgress, SnapshotDeletionsInProgress.get(clusterService.state()));
        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        assertThat(Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid())), hasSize(2));

        final var deletion3Future = startDeletion(completedSnapshot.getSnapshotId().getName());
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletion3Future.isDone());

        assertSame(snapshotsInProgress, SnapshotsInProgress.get(clusterService.state()));
        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        assertThat(Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid())), hasSize(3));

        final var deletionsInProgress2 = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress2.getEntries(), hasSize(3));
        final var deletionEntry2 = deletionsInProgress2.getEntries().getLast();
        assertEquals(ProjectId.DEFAULT, deletionEntry2.projectId());
        assertEquals(repoName, deletionEntry2.repoName());
        assertEquals(WAITING, deletionEntry2.state());
        assertEquals(deletionEntry.uuid(), deletionEntry2.uuid());

        assertTrue(startedDeletions.isEmpty());
        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        ActionListener.onResponse(completionHandlers.get(deletionEntry.uuid()), null);
        assertTrue(deletion1Future.isSuccess());
        assertTrue(deletion2Future.isSuccess());
        assertTrue(deletion3Future.isSuccess());
    }

    public void testRetryOnStaleRepositoryData() {
        final var snapshot = randomSnapshot();

        submitClusterStateUpdateTask("update repository metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final var projectMetadata = currentState.projectState(ProjectId.DEFAULT).metadata();
                final var repoMetadata = RepositoriesMetadata.get(projectMetadata).repository(repoName);
                return ClusterState.builder(currentState)
                    .putProjectMetadata(
                        ProjectMetadata.builder(projectMetadata)
                            .putCustom(
                                RepositoriesMetadata.TYPE,
                                RepositoriesMetadata.get(projectMetadata)
                                    .withUpdatedGeneration(repoName, repoMetadata.generation() + 1, repoMetadata.pendingGeneration() + 1)
                            )
                    )
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            public void clusterStateProcessed(ClusterState initialState, ClusterState newState) {
                addCompleteSnapshot(snapshot);
            }
        });

        final var deletionFuture = startDeletion(snapshot.getSnapshotId().getName());

        submitClusterStateUpdateTask("ensure first attempt was no-op", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                assertFalse(SnapshotDeletionsInProgress.get(currentState).hasDeletionsInProgress());
                return currentState;
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        assertEquals(
            clusterService.getMasterService().pendingTasks().stream().map(t -> t.source().toString()).toList(),
            List.of("update repository metadata", "snapshot-deletion-start[default/" + repoName + "][1]", "ensure first attempt was no-op")
        );

        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertFalse(deletionFuture.isDone());

        assertTrue(snapshotEndNotifications.isEmpty());
        assertTrue(snapshotAbortNotifications.isEmpty());

        assertThat(startedDeletions, hasSize(1));
        assertThat(startedDeletions.getFirst().snapshots(), contains(snapshot.getSnapshotId()));

        final var deletionsInProgress = SnapshotDeletionsInProgress.get(clusterService.state());
        assertThat(deletionsInProgress.getEntries(), hasSize(1));
        final var deletionEntry = deletionsInProgress.getEntries().getFirst();
        assertEquals(STARTED, deletionEntry.state());
        assertThat(deletionEntry.snapshots(), contains(snapshot.getSnapshotId()));

        assertThat(completionHandlers.keySet(), equalTo(Set.of(deletionEntry.uuid())));
        final var listeners = Objects.requireNonNull(completionHandlers.get(deletionEntry.uuid()));
        assertThat(listeners, hasSize(1));
        assertFalse(deletionFuture.isDone());
        listeners.getFirst().onResponse(null);

        assertTrue(deletionFuture.isDone());
        safeAwait(deletionFuture);
    }

    public void testLogging() {
        final var snapshot = randomSnapshot("a-very-long-string-of-characters-to-ensure-that-the-log-message-is-truncated-");
        final var snapshotName = snapshot.getSnapshotId().getName();
        addCompleteSnapshot(snapshot);

        final var iterations =
            // enough tasks to get to just under the 4kiB limit
            ByteSizeUnit.KB.toIntBytes(4) / (snapshotName.length() + 2)
                // the first task runs unbatched
                + 1
                // the log messages overflows by at most a single item
                + 1
                // an item to omit
                + 1;

        for (int i = 0; i < iterations; i++) {
            startDeletion(snapshotName);
        }

        MockLog.assertThatLogger(
            deterministicTaskQueue::runAllTasksInTimeOrder,
            SnapshotDeletionStartBatcher.class,
            new MockLog.SeenEventExpectation(
                "singleton message",
                SnapshotDeletionStartBatcher.class.getCanonicalName(),
                Level.INFO,
                Strings.format("deleting snapshots [%s] from repository [default/%s]", snapshotName, repoName)
            ),
            new MockLog.SeenEventExpectation(
                "truncated message",
                SnapshotDeletionStartBatcher.class.getCanonicalName(),
                Level.INFO,
                Strings.format(
                    "deleting snapshots [*, ... (%d in total, 1 omitted)] from repository [default/%s]",
                    iterations - 1,
                    repoName
                )
            )
        );
    }

}
