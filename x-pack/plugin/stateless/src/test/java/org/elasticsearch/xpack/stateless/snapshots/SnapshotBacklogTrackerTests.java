/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.LocalShard;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTracker.RepositoryBacklog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.commitFiles;
import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.shardSnapshots;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SnapshotBacklogTrackerTests extends ESTestCase {

    private static final ShardGeneration GENERATION = new ShardGeneration("gen");

    /**
     * A cache that knows what the test tells it, and nothing else
     */
    private static class FixedCache extends RepositoryFilesCache {
        final Map<ShardId, RepositoryShardFiles> known = new HashMap<>();

        FixedCache() {
            super("repo", null, command -> {}, Runnable::run);
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

    public void testTheFilesOfACommitAreCountedWithTheirLengthsFromTheDirectoryWithoutBlobLocations() throws Exception {
        // a commit that is only in the directory of the shard, as one that is not uploaded yet: no blob location exists for its files
        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                writer.addDocument(List.of(new StringField("field", randomAlphaOfLength(20), Field.Store.YES)));
                writer.commit();
            }
            final IndexCommit commit = DirectoryReader.listCommits(directory).get(0);

            final Map<String, Long> commitFiles = SnapshotBacklogTracker.getCommitFiles(commit);
            assertThat(commitFiles.keySet(), equalTo(Set.copyOf(commit.getFileNames())));
            long expectedBytes = 0;
            for (String fileName : commit.getFileNames()) {
                assertThat(commitFiles.get(fileName), equalTo(directory.fileLength(fileName)));
                if (Store.MetadataSnapshot.isReadAsHash(fileName) == false) {
                    expectedBytes += directory.fileLength(fileName);
                }
            }
            assertThat(expectedBytes, greaterThan(0L));

            // the repository holds nothing, so the shard counts, rather than being unknown
            cache.known.put(shardId(0), RepositoryShardFiles.NONE);
            final var backlog = compute(cache, List.of(new LocalShard(shardId(0), projectId, commitFiles)), Map.of());
            assertThat(backlog, equalTo(new RepositoryBacklog(expectedBytes, 1, 0, expectedBytes)));
        }
    }
}
