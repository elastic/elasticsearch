/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.LocalShard;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.RepositoryBacklog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.commitFiles;
import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.shardSnapshots;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SnapshotBacklogTrackerTests extends ESTestCase {

    private static final ShardGeneration GENERATION = new ShardGeneration("gen");

    /**
     * A cache that knows what the test tells it, and nothing else
     */
    private static class FixedCache extends RepositoryFilesCache {
        final Map<ShardId, RepositoryShardFiles> known = new HashMap<>();

        FixedCache() {
            super("repo", null, command -> {});
        }

        @Override
        RepositoryShardFiles getShardFiles(ShardId shardId) {
            return known.get(shardId);
        }
    }

    private final FixedCache cache = new FixedCache();
    private final ProjectId projectId = ProjectId.DEFAULT;

    private ShardId shardId(int id) {
        return new ShardId(new Index("index", "index-uuid"), id);
    }

    private static RepositoryBacklog compute(
        FixedCache cache,
        List<LocalShard> shards,
        Map<ShardId, List<IndexShardSnapshotStatus>> statuses
    ) {
        return SnapshotBacklogTracker.computeRepositoryBacklog(cache, shards, shard -> statuses.getOrDefault(shard, List.of()));
    }

    public void testBacklogIsTheSumOverTheShardsWithTheLargestOneReported() {
        cache.known.put(shardId(0), RepositoryShardFiles.of(GENERATION, shardSnapshots("_0.cfs", 100L)));
        cache.known.put(shardId(1), RepositoryShardFiles.NONE);
        final var shards = List.of(
            new LocalShard(shardId(0), projectId, commitFiles("_0.cfs", 100L, "_1.cfs", 40L)),
            new LocalShard(shardId(1), projectId, commitFiles("_0.cfs", 100L, "_0.si", 5L))
        );
        assertThat(compute(cache, shards, Map.of()), equalTo(new RepositoryBacklog(140, 2, 0, 100)));
    }

    public void testAShardWithNothingToUploadIsCountedNotUnknown() {
        cache.known.put(shardId(0), RepositoryShardFiles.of(GENERATION, shardSnapshots("_0.cfs", 100L)));
        final var shards = List.of(new LocalShard(shardId(0), projectId, commitFiles("_0.cfs", 100L)));
        assertThat(compute(cache, shards, Map.of()), equalTo(new RepositoryBacklog(0, 1, 0, 0)));
    }

    public void testShardsWhoseFilesAreNotKnownAreReportedAsUnknownNotAsZero() {
        cache.known.put(shardId(0), RepositoryShardFiles.NONE);
        // shard 1 has no file list in the cache yet, and shard 2 has no commit files at the moment
        cache.known.put(shardId(2), RepositoryShardFiles.NONE);
        final var shards = List.of(
            new LocalShard(shardId(0), projectId, commitFiles("_0.cfs", 100L)),
            new LocalShard(shardId(1), projectId, commitFiles("_0.cfs", 70L)),
            new LocalShard(shardId(2), projectId, null)
        );
        assertThat(compute(cache, shards, Map.of()), equalTo(new RepositoryBacklog(100, 1, 2, 100)));
    }

    public void testWhatARunningSnapshotUploadedIsTakenOffTheBacklog() {
        cache.known.put(shardId(0), RepositoryShardFiles.of(GENERATION, shardSnapshots("_0.cfs", 10L)));
        cache.known.put(shardId(1), RepositoryShardFiles.of(GENERATION, shardSnapshots("_0.cfs", 10L)));
        final var shards = List.of(
            new LocalShard(shardId(0), projectId, commitFiles("_0.cfs", 10L, "_1.cfs", 100L)),
            new LocalShard(shardId(1), projectId, commitFiles("_0.cfs", 10L, "_1.cfs", 100L))
        );
        final var status = IndexShardSnapshotStatus.newInitializing(GENERATION, 1);
        status.moveToStarted(1, 1, 2, 100, 110);
        status.addProcessedFile(25);

        final var backlog = compute(cache, shards, Map.of(shardId(0), List.of(status)));
        assertThat(backlog, equalTo(new RepositoryBacklog(75 + 100, 2, 0, 100)));
    }

    public void testShardsOfAllRepositoryStatesAddUpToNothingWhenThereAreNoShards() {
        assertThat(compute(cache, List.of(), Map.of()), equalTo(new RepositoryBacklog(0, 0, 0, 0)));
        assertTrue(compute(cache, List.of(), Map.of()).isEmpty());
        assertFalse(new RepositoryBacklog(0, 0, 1, 0).isEmpty());
    }

    public void testTheBacklogOfAShardDoesNotGoUnknownWhileTheRepositoryIsRefreshed() {
        final var oldFiles = RepositoryShardFiles.of(GENERATION, shardSnapshots("_0.cfs", 10L));
        cache.known.put(shardId(0), oldFiles);
        final var shards = List.of(new LocalShard(shardId(0), projectId, commitFiles("_0.cfs", 10L, "_1.cfs", 100L)));
        assertThat(compute(cache, shards, Map.of()), equalTo(new RepositoryBacklog(100, 1, 0, 100)));

        // a snapshot of the shard finished: the repository has not caught up yet, the shard still has its old list and the
        // finished snapshot's uploads are subtracted
        final var status = IndexShardSnapshotStatus.newInitializing(GENERATION, 1);
        status.moveToStarted(1, 1, 2, 100, 110);
        status.addProcessedFile(100);
        status.moveToFinalize();
        status.moveToDone(2, new ShardSnapshotResult(new ShardGeneration("new"), ByteSizeValue.ofBytes(100), 1));
        assertThat(compute(cache, shards, Map.of(shardId(0), List.of(status))), equalTo(new RepositoryBacklog(0, 1, 0, 0)));

        // and once the new list is in, the status is not subtracted again
        cache.known.put(shardId(0), RepositoryShardFiles.of(new ShardGeneration("new"), shardSnapshots("_0.cfs", 10L, "_1.cfs", 100L)));
        assertThat(compute(cache, shards, Map.of(shardId(0), List.of(status))), equalTo(new RepositoryBacklog(0, 1, 0, 0)));
    }

    public void testTurningTheTrackingOffAndOnAgain() {
        final var threadPool = new TestThreadPool(getTestName());
        final ClusterService clusterService = ClusterServiceUtils.createClusterService(
            threadPool,
            new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, SnapshotBacklogTracker.BACKLOG_TRACKING_ENABLED_SETTING)
            )
        );
        try {
            final var indicesService = mock(IndicesService.class);
            final var repositoriesService = mock(RepositoriesService.class);
            when(repositoriesService.getRepositories()).thenReturn(List.of());
            final var client = mock(Client.class);
            final var tracker = new SnapshotBacklogTracker(
                clusterService,
                client,
                indicesService,
                mock(StatelessCommitService.class),
                repositoriesService,
                threadPool,
                MeterRegistry.NOOP
            );
            final var shardId = shardId(0);
            final var snapshot = new Snapshot(ProjectId.DEFAULT, "repo", new SnapshotId("snap", "snap-uuid"));
            final var status = IndexShardSnapshotStatus.newInitializing(GENERATION, 1);

            // off by default: nothing is looked at, asked for or kept
            tracker.registerShardSnapshot(snapshot, shardId, status);
            tracker.clusterChanged(new ClusterChangedEvent("test", clusterService.state(), clusterService.state()));
            assertThat(tracker.getBacklog(), equalTo(Map.of()));
            verifyNoInteractions(repositoriesService, client, indicesService);

            clusterService.getClusterSettings()
                .applySettings(Settings.builder().put(SnapshotBacklogTracker.BACKLOG_TRACKING_ENABLED_SETTING.getKey(), true).build());
            assertThat(tracker.getBacklog(), equalTo(Map.of()));
            verify(repositoriesService).getRepositories();

            clusterService.getClusterSettings()
                .applySettings(Settings.builder().put(SnapshotBacklogTracker.BACKLOG_TRACKING_ENABLED_SETTING.getKey(), false).build());
            assertThat(tracker.getBacklog(), equalTo(Map.of()));
            verifyNoMoreInteractions(repositoriesService, client, indicesService);
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }
}
