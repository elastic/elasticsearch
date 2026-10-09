/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Executor;

import static org.elasticsearch.xpack.stateless.snapshots.SnapshotBacklogTestUtils.shardSnapshots;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class RepositoryFilesCacheTests extends ESTestCase {

    private static final IndexId INDEX = new IndexId("index", "index-id");

    /**
     * A repository the test can change, which also records what is read from it
     */
    private static class FakeRepository implements RepositoryFilesCache.Reader {
        final Map<Long, RepositoryData> repositoryDatas = new HashMap<>();
        final Map<ShardGeneration, BlobStoreIndexShardSnapshots> shardFiles = new HashMap<>();
        final List<Long> repositoryDataReads = new ArrayList<>();
        final List<ShardGeneration> shardReads = new ArrayList<>();

        @Override
        public RepositoryData readRepositoryData(long repositoryGeneration) throws IOException {
            repositoryDataReads.add(repositoryGeneration);
            final var data = repositoryDatas.get(repositoryGeneration);
            if (data == null) {
                throw new NoSuchFileException("index-" + repositoryGeneration);
            }
            return data;
        }

        @Override
        public BlobStoreIndexShardSnapshots readShardSnapshots(IndexId indexId, int shardId, ShardGeneration shardGeneration)
            throws IOException {
            shardReads.add(shardGeneration);
            final var files = shardFiles.get(shardGeneration);
            if (files == null) {
                throw new NoSuchFileException("index-" + shardGeneration);
            }
            return files;
        }

        void publish(long repositoryGeneration, ShardGeneration... shardGenerations) {
            final var builder = ShardGenerations.builder();
            for (int shard = 0; shard < shardGenerations.length; shard++) {
                builder.put(INDEX, shard, shardGenerations[shard]);
            }
            repositoryDatas.put(
                repositoryGeneration,
                new RepositoryData(
                    "uuid",
                    repositoryGeneration,
                    Map.of("snap", new SnapshotId("snap", "snap-uuid")),
                    Map.of(),
                    Map.of(INDEX, List.of(new SnapshotId("snap", "snap-uuid"))),
                    builder.build(),
                    IndexMetaDataGenerations.EMPTY,
                    "cluster-uuid"
                )
            );
        }
    }

    /**
     * An executor that runs tasks when the test says so
     */
    private static class ManualExecutor implements Executor {
        final Queue<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        int runAll() {
            int count = 0;
            Runnable task;
            while ((task = tasks.poll()) != null) {
                task.run();
                count++;
            }
            return count;
        }
    }

    private final FakeRepository repository = new FakeRepository();
    private final ManualExecutor executor = new ManualExecutor();
    private final RepositoryFilesCache cache = new RepositoryFilesCache("repo", repository, executor);
    private final ShardId shard0 = new ShardId(new Index("index", "index-uuid"), 0);
    private final ShardId shard1 = new ShardId(new Index("index", "index-uuid"), 1);
    private final ShardGeneration gen0 = new ShardGeneration("gen0");
    private final ShardGeneration gen1 = new ShardGeneration("gen1");

    private static boolean holds(RepositoryShardFiles files, String name, long length) {
        return files.contains(name, length);
    }

    public void testShardsAreUnknownUntilTheirFilesAreRead() {
        repository.publish(1, gen0, gen1);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        repository.shardFiles.put(gen1, shardSnapshots("_1.cfs", 20L));

        // nothing is known before the repository data is loaded
        assertThat(cache.getShardFiles(shard0), nullValue());
        assertThat(cache.getShardFiles(shard1), nullValue());
        cache.onRepositoryGeneration(1);
        assertThat(cache.getShardFiles(shard0), nullValue());

        // loading the repository data starts reading the files of the shards we know about
        executor.runAll();
        assertThat(repository.shardReads, containsInAnyOrder(gen0, gen1));
        assertThat(cache.getShardFiles(shard0).generation(), equalTo(gen0));
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));
        assertTrue(holds(cache.getShardFiles(shard1), "_1.cfs", 20));
    }

    public void testNothingIsKnownWhileTheRepositoryGenerationIsUnknown() {
        cache.onRepositoryGeneration(RepositoryData.UNKNOWN_REPO_GEN);
        assertThat(executor.runAll(), equalTo(0));
        assertThat(cache.getShardFiles(shard0), nullValue());
    }

    public void testAnEmptyRepositoryHoldsNoFiles() {
        repository.repositoryDatas.put(RepositoryData.EMPTY_REPO_GEN, RepositoryData.EMPTY);
        cache.onRepositoryGeneration(RepositoryData.EMPTY_REPO_GEN);
        executor.runAll();
        assertThat(cache.getShardFiles(shard0), sameInstance(RepositoryShardFiles.NONE));
        assertThat(repository.shardReads, empty());
    }

    public void testAShardTheRepositoryHasNoMetadataForHoldsNoFiles() {
        // the repository knows shard 0 of the index, but not shard 1, and a failed first snapshot may leave the new shard generation
        repository.publish(1, gen0);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.getShardFiles(shard0);
        cache.onRepositoryGeneration(1);
        executor.runAll();
        assertThat(cache.getShardFiles(shard1), sameInstance(RepositoryShardFiles.NONE));

        repository.publish(2, gen0, ShardGenerations.NEW_SHARD_GEN);
        cache.onRepositoryGeneration(2);
        executor.runAll();
        assertThat(cache.getShardFiles(shard1), sameInstance(RepositoryShardFiles.NONE));
        assertThat(repository.shardReads, contains(gen0));
    }

    public void testAnIndexThatIsNotInTheRepositoryHoldsNoFiles() {
        repository.publish(1, gen0);
        cache.onRepositoryGeneration(1);
        executor.runAll();
        final var otherIndexShard = new ShardId(new Index("other", "other-uuid"), 0);
        assertThat(cache.getShardFiles(otherIndexShard), sameInstance(RepositoryShardFiles.NONE));
    }

    public void testOnlyShardsWhoseGenerationChangedAreReadAgain() {
        repository.publish(1, gen0, gen1);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        repository.shardFiles.put(gen1, shardSnapshots("_1.cfs", 20L));
        cache.getShardFiles(shard0);
        cache.getShardFiles(shard1);
        cache.onRepositoryGeneration(1);
        executor.runAll();
        assertThat(repository.shardReads, containsInAnyOrder(gen0, gen1));
        repository.shardReads.clear();

        // a snapshot finished, and changed the generation of shard 1 only
        final var newGen1 = new ShardGeneration("newGen1");
        repository.publish(2, gen0, newGen1);
        repository.shardFiles.put(newGen1, shardSnapshots("_1.cfs", 20L, "_2.cfs", 30L));
        cache.onRepositoryGeneration(2);
        assertThat(repository.repositoryDataReads, contains(1L));
        executor.runAll();

        assertThat(repository.repositoryDataReads, contains(1L, 2L));
        assertThat(repository.shardReads, contains(newGen1));
        assertTrue(holds(cache.getShardFiles(shard1), "_2.cfs", 30));
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));
    }

    public void testAChangedShardIsUnknownUntilItsNewFilesAreRead() {
        repository.publish(1, gen0);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.getShardFiles(shard0);
        cache.onRepositoryGeneration(1);
        executor.runAll();

        // a snapshot was deleted, and the shard has a new generation
        repository.publish(2, gen1);
        repository.shardFiles.put(gen1, shardSnapshots("_5.cfs", 5L));
        cache.onRepositoryGeneration(2);
        // load the repository data, but do not read the shard yet
        executor.tasks.poll().run();
        assertThat(cache.getShardFiles(shard0), nullValue());
        executor.runAll();
        assertTrue(holds(cache.getShardFiles(shard0), "_5.cfs", 5));
    }

    public void testTheSameRepositoryGenerationIsLoadedOnce() {
        repository.publish(1, gen0);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onRepositoryGeneration(1);
        cache.onRepositoryGeneration(1);
        executor.runAll();
        cache.onRepositoryGeneration(1);
        assertThat(executor.runAll(), equalTo(0));
        assertThat(repository.repositoryDataReads, contains(1L));
    }

    public void testAMissingShardLevelBlobMakesTheShardUnknownAndIsReadAgainLater() {
        repository.publish(1, gen0);
        cache.onRepositoryGeneration(1);
        executor.runAll();

        assertThat(cache.getShardFiles(shard0), nullValue());
        executor.runAll();
        assertThat(cache.getShardFiles(shard0), nullValue()); // read failed, shard stays unknown
        executor.runAll();

        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        assertThat(cache.getShardFiles(shard0), nullValue());
        executor.runAll();
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));
    }

    public void testAShardIsNotReadTwiceWhileItsReadIsRunning() {
        repository.publish(1, gen0);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onRepositoryGeneration(1);
        executor.runAll();

        assertThat(cache.getShardFiles(shard0), nullValue());
        assertThat(cache.getShardFiles(shard0), nullValue());
        assertThat(executor.runAll(), equalTo(1));
        assertThat(repository.shardReads, contains(gen0));
    }

    public void testAFailedRepositoryDataLoadIsRetried() {
        cache.onRepositoryGeneration(3); // index-3 is not there (yet)
        executor.runAll();
        assertThat(cache.getShardFiles(shard0), nullValue());

        repository.publish(3, gen0);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onRepositoryGeneration(3);
        executor.runAll();
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));
        assertThat(repository.repositoryDataReads, contains(3L, 3L));
    }

    public void testShardsThatLeftTheNodeAreForgotten() {
        repository.publish(1, gen0, gen1);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        repository.shardFiles.put(gen1, shardSnapshots("_1.cfs", 20L));
        cache.getShardFiles(shard0);
        cache.getShardFiles(shard1);
        cache.onRepositoryGeneration(1);
        executor.runAll();

        repository.shardReads.clear();
        cache.retainShards(Set.of(shard0));
        repository.publish(2, gen0, gen1);
        cache.onRepositoryGeneration(2);
        executor.runAll();
        assertThat(repository.shardReads, empty()); // shard 0 is unchanged, and shard 1 is not on the node any more
    }
}
