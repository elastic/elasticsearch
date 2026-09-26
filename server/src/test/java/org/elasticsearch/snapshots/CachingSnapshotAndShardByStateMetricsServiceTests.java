/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.Node;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CachingSnapshotAndShardByStateMetricsServiceTests extends ESTestCase {

    private final List<Runnable> tearDownTasks = new ArrayList<>();

    @After
    public void closeResources() {
        tearDownTasks.forEach(Runnable::run);
    }

    public void testMetricsAreOnlyCalculatedWhileClusterServiceIsStartedAndLocalNodeIsMaster() {
        final var indexName = randomIdentifier();
        final var repositoryName = randomIdentifier();
        final ClusterService clusterService = mock(ClusterService.class);
        final ThreadPool threadPool = new TestThreadPool("test");
        tearDownTasks.add(threadPool::shutdownNow);
        when(clusterService.threadPool()).thenReturn(threadPool);
        final CachingSnapshotAndShardByStateMetricsService byStateMetricsService = new CachingSnapshotAndShardByStateMetricsService(
            clusterService
        );

        // No metrics should be recorded before the cluster service is started
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.INITIALIZED);
        assertThat(byStateMetricsService.getShardsByState(), empty());
        assertThat(byStateMetricsService.getSnapshotsByState(), empty());
        verify(clusterService, never()).state();

        // Simulate the metrics service being started/a state being applied
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);
        final ClusterState withSnapshotsInProgress = createClusterStateWithSnapshotsInProgress(indexName, repositoryName);
        when(clusterService.state()).thenReturn(withSnapshotsInProgress);

        // This time we should publish some metrics
        final Collection<LongWithAttributes> shardsByState = byStateMetricsService.getShardsByState();
        final Collection<LongWithAttributes> snapshotsByState = byStateMetricsService.getSnapshotsByState();
        assertThat(shardsByState, not(empty()));
        assertThat(snapshotsByState, not(empty()));
        verify(clusterService, times(2)).state();

        reset(clusterService);
        when(clusterService.threadPool()).thenReturn(threadPool);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STARTED);

        // Then observe a new state in which we aren't master
        final ClusterState noLongerMaster = ClusterState.builder(withSnapshotsInProgress)
            .nodes(
                DiscoveryNodes.builder(withSnapshotsInProgress.nodes())
                    .masterNodeId(
                        randomValueOtherThan(
                            withSnapshotsInProgress.nodes().getLocalNodeId(),
                            () -> randomFrom(withSnapshotsInProgress.nodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet()))
                        )
                    )
            )
            .incrementVersion()
            .build();
        when(clusterService.state()).thenReturn(noLongerMaster);

        // We should no longer publish metrics
        assertThat(byStateMetricsService.getShardsByState(), empty());
        assertThat(byStateMetricsService.getSnapshotsByState(), empty());

        // Become master again
        final ClusterState masterAgain = ClusterState.builder(noLongerMaster)
            .nodes(DiscoveryNodes.builder(noLongerMaster.nodes()).masterNodeId(noLongerMaster.nodes().getLocalNodeId()))
            .incrementVersion()
            .build();
        when(clusterService.state()).thenReturn(masterAgain);

        // We should return cached metrics because the SnapshotsInProgress hasn't changed
        final Collection<LongWithAttributes> secondShardsByState = byStateMetricsService.getShardsByState();
        final Collection<LongWithAttributes> secondSnapshotsByState = byStateMetricsService.getSnapshotsByState();
        assertThat(secondShardsByState, sameInstance(shardsByState));
        assertThat(secondSnapshotsByState, sameInstance(snapshotsByState));

        // Update SnapshotsInProgress
        final ClusterState newSnapshotsInProgress = ClusterState.builder(masterAgain)
            .putCustom(SnapshotsInProgress.TYPE, createSnapshotsInProgress(masterAgain, indexName, repositoryName))
            .incrementVersion()
            .build();
        when(clusterService.state()).thenReturn(newSnapshotsInProgress);

        // We should return fresh metrics because the SnapshotsInProgress has changed
        final Collection<LongWithAttributes> thirdShardsByState = byStateMetricsService.getShardsByState();
        final Collection<LongWithAttributes> thirdSnapshotsByState = byStateMetricsService.getSnapshotsByState();
        assertThat(thirdShardsByState, not(empty()));
        assertThat(thirdSnapshotsByState, not(empty()));
        assertThat(thirdShardsByState, not(sameInstance(shardsByState)));
        assertThat(thirdSnapshotsByState, not(sameInstance(snapshotsByState)));

        // Then the cluster service is stopped, we should no longer publish metrics
        reset(clusterService);
        when(clusterService.lifecycleState()).thenReturn(Lifecycle.State.STOPPED);
        assertThat(byStateMetricsService.getShardsByState(), empty());
        assertThat(byStateMetricsService.getSnapshotsByState(), empty());
        verify(clusterService, never()).state();
    }

    private ClusterState createClusterStateWithSnapshotsInProgress(String indexName, String repositoryName) {
        // Need to have at least 2 nodes, so we can test when another node is the master
        final ClusterState state = ClusterStateCreationUtils.state(indexName, randomIntBetween(2, 5), randomIntBetween(1, 2));
        return ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder(state.nodes()).masterNodeId(state.nodes().getLocalNodeId()))
            .putProjectMetadata(
                ProjectMetadata.builder(state.getMetadata().getProject(ProjectId.DEFAULT))
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(List.of(new RepositoryMetadata(repositoryName, "fs", Settings.EMPTY)))
                    )
            )
            .putCustom(SnapshotsInProgress.TYPE, createSnapshotsInProgress(state, indexName, repositoryName))
            .incrementVersion()
            .build();
    }

    private SnapshotsInProgress createSnapshotsInProgress(ClusterState clusterState, String indexName, String repositoryName) {
        final IndexMetadata index = clusterState.projectState(ProjectId.DEFAULT).metadata().index(indexName);
        return SnapshotsInProgress.EMPTY.withAddedEntry(createEntry(index, repositoryName));
    }

    private SnapshotsInProgress.Entry createEntry(IndexMetadata indexMetadata, String repositoryName) {
        return SnapshotsInProgress.Entry.snapshot(
            new Snapshot(ProjectId.DEFAULT, repositoryName, new SnapshotId("", "")),
            false,
            randomBoolean(),
            SnapshotsInProgress.State.STARTED,
            Map.of(indexMetadata.getIndex().getName(), new IndexId(indexMetadata.getIndex().getName(), randomIdentifier())),
            List.of(),
            Collections.emptyList(),
            0,
            1,
            IntStream.range(0, indexMetadata.getNumberOfShards())
                .mapToObj(i -> new ShardId(indexMetadata.getIndex(), i))
                .collect(
                    Collectors.toUnmodifiableMap(
                        Function.identity(),
                        shardId -> new SnapshotsInProgress.ShardSnapshotStatus(randomIdentifier(), ShardGeneration.newGeneration())
                    )
                ),
            null,
            null,
            null
        );
    }

    public void testGetLongestWaitingTimeMillis() {
        final DiscoveryNode localNode = DiscoveryNodeUtils.create("test-node");
        final Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), localNode.getName())
            .put(ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL.getKey(), TimeValue.ZERO)
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        tearDownTasks.add(deterministicTaskQueue::runAllRunnableTasks);
        final ClusterApplierService clusterApplierService = new ClusterApplierService(
            localNode.getName(),
            settings,
            clusterSettings,
            deterministicTaskQueue.getThreadPool()
        ) {
            @Override
            protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
            }
        };
        final MasterService masterService = new FakeThreadPoolMasterService(
            localNode.getName(),
            deterministicTaskQueue.getThreadPool(),
            deterministicTaskQueue::scheduleNow
        );
        final ClusterService clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);
        tearDownTasks.add(clusterService::close);
        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());

        final ProjectId project1 = ProjectId.fromId("project-1");
        final ProjectId project2 = ProjectId.fromId("project-2");
        final String repo1 = "repo-1";  // in project1
        final String repo2 = "repo-2";  // in project1
        final String repo3 = "repo-3";  // in project2
        final Snapshot snapshot1 = new Snapshot(project1, repo1, new SnapshotId("snap-1", UUID.randomUUID().toString())); // for repo1
        final Snapshot snapshot2 = new Snapshot(project1, repo2, new SnapshotId("snap-2", UUID.randomUUID().toString())); // for repo2
        final Snapshot snapshot3 = new Snapshot(project2, repo3, new SnapshotId("snap-3", UUID.randomUUID().toString())); // for repo3
        final Index index1 = new Index("index-1", UUID.randomUUID().toString()); // involved in snapshot1
        final Index index2 = new Index("index-2", UUID.randomUUID().toString()); // involved in snapshot2
        final Index index3 = new Index("index-3", UUID.randomUUID().toString()); // involved in snapshot3
        final Index index4 = new Index("index-4", UUID.randomUUID().toString()); // involved in snapshot3
        final ShardId shard0 = new ShardId(index1, randomIntBetween(0, 16)); // in index1
        final ShardId shard1 = new ShardId(index1, randomValueOtherThan(shard0.getId(), () -> randomIntBetween(0, 16))); // in index1
        final ShardId shard2 = new ShardId(index2, randomIntBetween(0, 16)); // in index2
        final ShardId shard3 = new ShardId(index3, randomIntBetween(0, 16)); // in index3
        final ShardId shard4 = new ShardId(index4, randomIntBetween(0, 16)); // in index4

        final String nodeId = localNode.getId();
        final ShardGeneration shardGeneration = new ShardGeneration("test-gen");
        final SnapshotsInProgress.ShardSnapshotStatus initStatus = new SnapshotsInProgress.ShardSnapshotStatus(
            nodeId,
            SnapshotsInProgress.ShardState.INIT,
            shardGeneration
        );
        final SnapshotsInProgress.ShardSnapshotStatus waitingStatus = new SnapshotsInProgress.ShardSnapshotStatus(
            nodeId,
            SnapshotsInProgress.ShardState.WAITING,
            shardGeneration
        );

        // Initial state has snapshot1 with shard 0 in initializing state:
        SnapshotsInProgress snapshots = updateSnapshot(SnapshotsInProgress.EMPTY, snapshot1, List.of(index1), Map.of(shard0, initStatus));

        final ClusterState initialState = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
            .putProjectMetadata(
                ProjectMetadata.builder(project1)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(
                            List.of(
                                new RepositoryMetadata(repo1, "fs", Settings.EMPTY),
                                new RepositoryMetadata(repo2, "fs", Settings.EMPTY)
                            )
                        )
                    )
            )
            .putProjectMetadata(
                ProjectMetadata.builder(project2)
                    .putCustom(
                        RepositoriesMetadata.TYPE,
                        new RepositoriesMetadata(List.of(new RepositoryMetadata(repo3, "fs", Settings.EMPTY)))
                    )
            )
            .putCustom(SnapshotsInProgress.TYPE, snapshots)
            .build();

        clusterApplierService.setInitialState(initialState);
        masterService.setClusterStatePublisher(ClusterServiceUtils.createClusterStatePublisher(clusterApplierService));
        masterService.setClusterStateSupplier(clusterApplierService::state);
        clusterService.start();
        deterministicTaskQueue.runAllRunnableTasks();
        deterministicTaskQueue.runTasksUpToTimeInOrder(System.currentTimeMillis()); // just to set the clock

        final CachingSnapshotAndShardByStateMetricsService metricsService = new CachingSnapshotAndShardByStateMetricsService(
            clusterService
        );

        // No waiting shards, expect metric to be zero:
        assertThat(metricsService.getLongestWaitingTimeMillis(), contains(new LongWithAttributes(0L)));

        // Add shard1 in WAITING state:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        long shard1WaitingTimestamp = deterministicTaskQueue.getCurrentTimeMillis();
        snapshots = updateSnapshot(snapshots, snapshot1, List.of(index1), Map.of(shard0, initStatus, shard1, waitingStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard1 waiting", snapshots);
        // shard1 has just been observed waiting for the first time, so expect metric to be zero:
        assertThat(metricsService.getLongestWaitingTimeMillis(), contains(new LongWithAttributes(0L)));

        // Check that we still get zero, rather than a negative value, if the clock goes backwards:
        advanceTime(deterministicTaskQueue, -1L);
        assertThat(metricsService.getLongestWaitingTimeMillis(), contains(new LongWithAttributes(0L)));

        // Wait some time:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        // shard1 has been waiting for some time, expect metric to reflect elapsed time:
        assertThat(
            metricsService.getLongestWaitingTimeMillis(),
            contains(new LongWithAttributes(deterministicTaskQueue.getCurrentTimeMillis() - shard1WaitingTimestamp))
        );

        // Add shard2 in WAITING state:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        snapshots = updateSnapshot(snapshots, snapshot2, List.of(index2), Map.of(shard2, waitingStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard2 waiting", snapshots);
        // shard1 has still been waiting longest:
        assertThat(
            metricsService.getLongestWaitingTimeMillis(),
            contains(new LongWithAttributes(deterministicTaskQueue.getCurrentTimeMillis() - shard1WaitingTimestamp))
        );

        // Add shard3 in WAITING state:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        long shard3WaitingTimestamp = deterministicTaskQueue.getCurrentTimeMillis();
        snapshots = updateSnapshot(snapshots, snapshot3, List.of(index3, index4), Map.of(shard3, waitingStatus, shard4, initStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard3 waiting", snapshots);
        // shard1 has still been waiting longest:
        assertThat(
            metricsService.getLongestWaitingTimeMillis(),
            contains(new LongWithAttributes(deterministicTaskQueue.getCurrentTimeMillis() - shard1WaitingTimestamp))
        );

        // Add shard4 in WAITING state:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        long shard4WaitingTimestamp = deterministicTaskQueue.getCurrentTimeMillis();
        snapshots = updateSnapshot(snapshots, snapshot3, List.of(index3, index4), Map.of(shard3, waitingStatus, shard4, waitingStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard4 waiting", snapshots);
        // shard1 has still been waiting longest:
        assertThat(
            metricsService.getLongestWaitingTimeMillis(),
            contains(new LongWithAttributes(deterministicTaskQueue.getCurrentTimeMillis() - shard1WaitingTimestamp))
        );

        // Move shard2 out of WAITING state:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        snapshots = updateSnapshot(snapshots, snapshot2, List.of(index2), Map.of(shard2, initStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard2 no longer waiting", snapshots);
        // shard1 has still been waiting longest:
        assertThat(
            metricsService.getLongestWaitingTimeMillis(),
            contains(new LongWithAttributes(deterministicTaskQueue.getCurrentTimeMillis() - shard1WaitingTimestamp))
        );

        // Move shard1 out of WAITING state:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        snapshots = updateSnapshot(snapshots, snapshot1, List.of(index1), Map.of(shard0, initStatus, shard1, initStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard1 no longer waiting", snapshots);
        // Now shard3 has been waiting the longest:
        assertThat(
            metricsService.getLongestWaitingTimeMillis(),
            contains(new LongWithAttributes(deterministicTaskQueue.getCurrentTimeMillis() - shard3WaitingTimestamp))
        );

        // Remove shard3 from SnapshotsInProgress:
        advanceTime(deterministicTaskQueue, randomLongBetween(100, 1000));
        snapshots = updateSnapshot(snapshots, snapshot3, List.of(index4), Map.of(shard4, waitingStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard3 removed", snapshots);
        // Now shard4 has been waiting the longest:
        assertThat(
            metricsService.getLongestWaitingTimeMillis(),
            contains(new LongWithAttributes(deterministicTaskQueue.getCurrentTimeMillis() - shard4WaitingTimestamp))
        );

        // Move shard3 out of WAITING state:
        snapshots = updateSnapshot(snapshots, snapshot3, List.of(index4), Map.of(shard4, initStatus));
        applyNewSnapshotsInProgress(clusterService, deterministicTaskQueue, "shard4 no longer waiting", snapshots);
        // No waiting shards left, so expect metric to be zero:
        assertThat(metricsService.getLongestWaitingTimeMillis(), contains(new LongWithAttributes(0L)));
    }

    private void advanceTime(DeterministicTaskQueue deterministicTaskQueue, long millis) {
        deterministicTaskQueue.runTasksUpToTimeInOrder(deterministicTaskQueue.getCurrentTimeMillis() + millis);
    }

    private SnapshotsInProgress updateSnapshot(
        SnapshotsInProgress snapshots,
        Snapshot snapshot,
        List<Index> indices,
        Map<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards
    ) {
        return snapshots.createCopyWithUpdatedEntriesForRepo(
            snapshot.getProjectId(),
            snapshot.getRepository(),
            List.of(
                SnapshotsInProgress.Entry.snapshot(
                    snapshot,
                    false,
                    false,
                    SnapshotsInProgress.State.STARTED,
                    indices.stream().collect(Collectors.toMap(Index::getName, index -> new IndexId(index.getName(), index.getUUID()))),
                    List.of(),
                    List.of(),
                    0L,
                    1L,
                    shards,
                    null,
                    null,
                    IndexVersion.current()
                )
            )
        );
    }

    private void applyNewSnapshotsInProgress(
        ClusterService clusterService,
        DeterministicTaskQueue deterministicTaskQueue,
        String reason,
        SnapshotsInProgress newSnapshots
    ) {
        final AtomicBoolean applied = new AtomicBoolean();
        clusterService.submitUnbatchedStateUpdateTask(reason, new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, newSnapshots).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                applied.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("unexpected failure applying cluster state", e);
            }
        });
        deterministicTaskQueue.runAllRunnableTasks();
        assertTrue(applied.get());
    }
}
