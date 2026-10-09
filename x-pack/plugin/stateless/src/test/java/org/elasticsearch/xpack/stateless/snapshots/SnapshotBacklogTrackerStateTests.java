/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.ProjectRepo;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.LocalShard;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.LocalShards;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.RepositoryBacklog;
import org.junit.Before;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.commitFiles;
import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.shardSnapshots;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Runs a {@link SnapshotBacklogTracker} on a deterministic task queue, to interleave what happens on its state executor with what the
 * master and the repository do.
 */
public class SnapshotBacklogTrackerStateTests extends ESTestCase {

    private static final IndexId INDEX = new IndexId("index", "index-id");

    private record PendingRequest(GetShardGenerationsRequest request, ActionListener<GetShardGenerationsResponse> listener) {}

    /**
     * A master that does not answer until the test says so
     */
    private class FakeMaster extends NoOpClient {
        final List<PendingRequest> requests = new ArrayList<>();

        FakeMaster(ThreadPool threadPool) {
            super(threadPool);
        }

        @Override
        @SuppressWarnings("unchecked")
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            assertThat(action, equalTo(TransportGetShardGenerationsAction.TYPE));
            requests.add(
                new PendingRequest(
                    (GetShardGenerationsRequest) request,
                    (ActionListener<GetShardGenerationsResponse>) (ActionListener<?>) listener
                )
            );
        }

        /**
         * Answers with the shard generations of the given shards, in the repository generation of the repository
         */
        void respond(int request) {
            final Map<ShardId, RepositoryShardGeneration> generations = new HashMap<>();
            for (ShardId shardId : requests.get(request).request().getShardIds()) {
                generations.put(shardId, new RepositoryShardGeneration(INDEX, generationOf(shardId)));
            }
            requests.get(request).listener().onResponse(new GetShardGenerationsResponse(repositoryGeneration.get(), generations));
        }
    }

    private class FakeLocalShards implements LocalShards {
        final List<LocalShard> shards = new ArrayList<>();
        final AtomicInteger reads = new AtomicInteger();
        Runnable duringRead = () -> {};

        @Override
        public List<LocalShard> getShards() {
            reads.incrementAndGet();
            duringRead.run();
            return List.copyOf(shards);
        }

        @Override
        public Map<ProjectId, Set<ShardId>> getShardIds() {
            final Set<ShardId> shardIds = new HashSet<>();
            shards.forEach(shard -> shardIds.add(shard.shardId()));
            return Map.of(ProjectId.DEFAULT, shardIds);
        }
    }

    private final DeterministicTaskQueue queue = new DeterministicTaskQueue();
    private final ThreadPool threadPool = queue.getThreadPool();
    private final ClusterSettings clusterSettings = new ClusterSettings(
        Settings.EMPTY,
        Set.of(SnapshotBacklogTracker.BACKLOG_TRACKING_ENABLED_SETTING, SnapshotBacklogTracker.EVALUATION_INTERVAL_SETTING)
    );
    private final AtomicLong repositoryGeneration = new AtomicLong(3);
    private final ProjectRepo projectRepo = new ProjectRepo(ProjectId.DEFAULT, "repo");
    private final FakeMaster master = new FakeMaster(threadPool);
    private final FakeLocalShards localShards = new FakeLocalShards();
    private final ShardId shard0 = new ShardId(new Index("index", "index-uuid"), 0);
    private final ShardId shard1 = new ShardId(new Index("index", "index-uuid"), 1);
    private SnapshotBacklogTracker tracker;

    private ShardGeneration generationOf(ShardId shardId) {
        return new ShardGeneration("gen" + shardId.id());
    }

    @Before
    public void createTracker() throws Exception {
        final var clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        // a repository that holds _0.cfs of 100 bytes of shard 0 and nothing of shard 1
        final var repository = mock(BlobStoreRepository.class);
        when(repository.isReadOnly()).thenReturn(false);
        when(repository.getProjectRepo()).thenReturn(projectRepo);
        when(repository.getMetadata()).thenAnswer(
            invocation -> new RepositoryMetadata(
                "repo",
                "repo-uuid",
                "fs",
                Settings.EMPTY,
                repositoryGeneration.get(),
                repositoryGeneration.get()
            )
        );
        when(repository.getBlobStoreIndexShardSnapshots(any(), anyInt(), any())).thenAnswer(
            invocation -> shardSnapshots("_" + invocation.getArgument(1) + ".cfs", 100L)
        );
        final var repositoriesService = mock(RepositoriesService.class);
        when(repositoriesService.getRepositories()).thenReturn(List.of(repository));

        tracker = new SnapshotBacklogTracker(clusterService, master, localShards, repositoriesService, threadPool, MeterRegistry.NOOP);
        tracker.start();
    }

    private void setEnabled(boolean enabled) {
        clusterSettings.applySettings(
            Settings.builder().put(SnapshotBacklogTracker.BACKLOG_TRACKING_ENABLED_SETTING.getKey(), enabled).build()
        );
    }

    /**
     * Lets the periodic evaluation run, and everything that it and the answers of the master set in motion
     */
    private void tick() {
        queue.advanceTime();
        queue.runAllRunnableTasks();
    }

    private void addShards(ShardId... shardIds) {
        for (ShardId shardId : shardIds) {
            // commit file _N.cfs has 100 bytes, which the repository holds for shard N, and the shard has one more file of 50 bytes
            localShards.shards.add(
                new LocalShard(
                    shardId,
                    ProjectId.DEFAULT,
                    commitFiles("_" + shardId.id() + ".cfs", 100L, "_" + shardId.id() + "b.cfs", 50L)
                )
            );
        }
    }

    public void testNothingHappensUntilTheTrackingIsTurnedOn() {
        addShards(shard0);
        tick();
        tick();
        assertThat(localShards.reads.get(), equalTo(0));
        assertThat(master.requests, hasSize(0));
        assertThat(tracker.getBacklog(), anEmptyMap());
    }

    public void testTheBacklogIsAPlainReadOfTheLatestEvaluation() {
        setEnabled(true);
        addShards(shard0);
        tick();
        // the shard is unknown until the master has answered, and the repository has been read
        assertThat(tracker.getBacklog().get(projectRepo), equalTo(new RepositoryBacklog(0, 0, 1, 0)));
        assertThat(master.requests, hasSize(1));
        master.respond(0);
        queue.runAllRunnableTasks();
        // reading the backlog does not evaluate anything, or ask anybody
        assertThat(tracker.getBacklog().get(projectRepo), equalTo(new RepositoryBacklog(0, 0, 1, 0)));
        assertThat(localShards.reads.get(), equalTo(1));
        assertThat(master.requests, hasSize(1));

        tick();
        assertThat(tracker.getBacklog().get(projectRepo), equalTo(new RepositoryBacklog(50, 1, 0, 50)));
    }

    public void testATickAndAnAnswerOfTheMasterAreInterleavedWithoutLosingAnUpdate() {
        setEnabled(true);
        addShards(shard0);
        tick();
        assertThat(master.requests, hasSize(1));
        assertThat(master.requests.get(0).request().getShardIds(), containsInAnyOrder(shard0));

        // a shard started on the node, and the answer about the shard that was there before is due when the next evaluation is
        addShards(shard1);
        switch (between(0, 2)) {
            case 0 -> {
                // the answer arrives, and the evaluation after it
                master.respond(0);
                tick();
            }
            case 1 -> {
                // the evaluation, and the answer after it
                queue.advanceTime();
                queue.runAllRunnableTasks();
                master.respond(0);
                queue.runAllRunnableTasks();
            }
            default -> {
                // the answer arrives while the evaluation is running
                localShards.duringRead = () -> {
                    master.respond(0);
                    localShards.duringRead = () -> {};
                };
                tick();
            }
        }

        // whichever it was, one more request is sent for both shards, because shard 1 had no shard generation
        assertThat(master.requests, hasSize(2));
        assertThat(master.requests.get(1).request().getShardIds(), containsInAnyOrder(shard0, shard1));
        master.respond(1);
        queue.runAllRunnableTasks();
        tick();
        assertThat(master.requests, hasSize(2));
        assertThat(tracker.getBacklog().get(projectRepo), equalTo(new RepositoryBacklog(100, 2, 0, 50)));
    }

    public void testTurningTheTrackingOffWhileEvaluatingPublishesNothing() {
        setEnabled(true);
        addShards(shard0);
        tick();
        master.respond(0);
        queue.runAllRunnableTasks();
        tick();
        assertThat(tracker.getBacklog().get(projectRepo), equalTo(new RepositoryBacklog(50, 1, 0, 50)));

        // it is turned off during an evaluation, which then does not publish what it computed
        localShards.duringRead = () -> setEnabled(false);
        tick();
        assertThat(tracker.getBacklog(), anEmptyMap());

        // and nothing is evaluated, asked or published any more
        final int reads = localShards.reads.get();
        localShards.duringRead = () -> {};
        tick();
        tick();
        assertThat(localShards.reads.get(), equalTo(reads));
        assertThat(master.requests, hasSize(1));
        assertThat(tracker.getBacklog(), anEmptyMap());
    }

    public void testTurningTheTrackingOnAgainStartsFromScratch() {
        setEnabled(true);
        addShards(shard0);
        tick();
        master.respond(0);
        queue.runAllRunnableTasks();
        tick();
        assertThat(tracker.getBacklog().get(projectRepo), equalTo(new RepositoryBacklog(50, 1, 0, 50)));

        setEnabled(false);
        queue.runAllRunnableTasks();
        assertThat(tracker.getBacklog(), anEmptyMap());

        // what the master said earlier is not remembered
        setEnabled(true);
        tick();
        assertThat(tracker.getBacklog().get(projectRepo), equalTo(new RepositoryBacklog(0, 0, 1, 0)));
        assertThat(master.requests, hasSize(2));
    }

    public void testAnAnswerThatArrivesAfterTheTrackingWasTurnedOffIsIgnored() {
        setEnabled(true);
        addShards(shard0);
        tick();
        setEnabled(false);
        master.respond(0);
        queue.runAllRunnableTasks();
        assertThat(tracker.getBacklog(), anEmptyMap());
        assertThat(master.requests, hasSize(1));
    }

    public void testClusterStateChangesOnlyAskTheMasterIfRepositoriesChanged() {
        setEnabled(true);
        addShards(shard0);
        final var withoutRepositories = ClusterState.builder(ClusterName.DEFAULT).build();
        final var withRepository = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(
                        ProjectMetadata.builder(ProjectId.DEFAULT)
                            .putCustom(
                                RepositoriesMetadata.TYPE,
                                new RepositoriesMetadata(List.of(new RepositoryMetadata("repo", "fs", Settings.EMPTY)))
                            )
                    )
            )
            .build();

        tracker.clusterChanged(new ClusterChangedEvent("test", withoutRepositories, withoutRepositories));
        queue.runAllRunnableTasks();
        assertThat(master.requests, hasSize(0));
        assertThat(localShards.reads.get(), equalTo(0));

        // a change of the repositories is picked up without waiting for the evaluation, which does not read the commits of the shards
        tracker.clusterChanged(new ClusterChangedEvent("test", withRepository, withoutRepositories));
        // ... and many such changes at once are one refresh
        tracker.clusterChanged(new ClusterChangedEvent("test", withRepository, withoutRepositories));
        queue.runAllRunnableTasks();
        assertThat(master.requests, hasSize(1));
        assertThat(localShards.reads.get(), equalTo(0));
    }
}
