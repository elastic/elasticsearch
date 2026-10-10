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
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
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
        final Map<ShardGeneration, BlobStoreIndexShardSnapshots> shardFiles = new HashMap<>();
        final List<ShardGeneration> shardReads = new ArrayList<>();

        @Override
        public BlobStoreIndexShardSnapshots readShardSnapshots(IndexId indexId, int shardId, ShardGeneration shardGeneration)
            throws IOException {
            assertThat(indexId, equalTo(INDEX));
            shardReads.add(shardGeneration);
            final var files = shardFiles.get(shardGeneration);
            if (files == null) {
                throw new NoSuchFileException("index-" + shardGeneration);
            }
            return files;
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
    private final RepositoryFilesCache cache = new RepositoryFilesCache("repo", repository, executor, Runnable::run);
    private final ShardId shard0 = new ShardId(new Index("index", "index-uuid"), 0);
    private final ShardId shard1 = new ShardId(new Index("index", "index-uuid"), 1);
    private final ShardGeneration gen0 = new ShardGeneration("gen0");
    private final ShardGeneration gen1 = new ShardGeneration("gen1");

    private static boolean holds(RepositoryShardFiles files, String name, long length) {
        return files.contains(name, length);
    }

    /**
     * @param generations the generation of shard 0, shard 1 and so on, which are {@code null} for a shard the repository has no
     *                    shard-level metadata for
     */
    private Map<ShardId, RepositoryShardGeneration> generations(ShardGeneration... generations) {
        final Map<ShardId, RepositoryShardGeneration> map = new HashMap<>();
        for (int shard = 0; shard < generations.length; shard++) {
            map.put(
                new ShardId(shard0.getIndex(), shard),
                generations[shard] == null ? null : new RepositoryShardGeneration(INDEX, generations[shard])
            );
        }
        return map;
    }

    public void testShardsAreUnknownUntilTheirFilesAreRead() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        repository.shardFiles.put(gen1, shardSnapshots("_1.cfs", 20L));

        // nothing is known before the shard generations are
        assertThat(cache.getShardFiles(shard0), nullValue());
        assertThat(cache.getShardFiles(shard1), nullValue());
        assertThat(cache.getRepositoryGeneration(), equalTo(RepositoryData.UNKNOWN_REPO_GEN));
        assertFalse(cache.hasShardGeneration(shard0));

        // receiving them starts reading the files of the shards we know about
        cache.onShardGenerations(1, generations(gen0, gen1));
        assertThat(cache.getRepositoryGeneration(), equalTo(1L));
        assertTrue(cache.hasShardGeneration(shard0));
        assertThat(cache.getShardFiles(shard0), nullValue());
        executor.runAll();
        assertThat(repository.shardReads, containsInAnyOrder(gen0, gen1));
        assertThat(cache.getShardFiles(shard0).generation(), equalTo(gen0));
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));
        assertTrue(holds(cache.getShardFiles(shard1), "_1.cfs", 20));
    }

    public void testAShardIsUpToDateOnlyWhenItsFilesAreThoseOfTheLatestGeneration() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        repository.shardFiles.put(gen1, shardSnapshots("_1.cfs", 20L));
        // no generation known, and the shards are asked about, which is what makes the cache read their files
        assertFalse(cache.isUpToDate(shard0));
        cache.getShardFiles(shard0);
        cache.getShardFiles(shard1);

        // known but not read yet, so not up to date, and a shard without a generation holds nothing, which is known right away
        cache.onShardGenerations(1, generations(gen0, null));
        assertFalse(cache.isUpToDate(shard0));
        assertTrue(cache.isUpToDate(shard1));
        executor.runAll();
        assertTrue(cache.isUpToDate(shard0));

        // a new generation: the old files are served but are not up to date, until the new ones are read
        cache.onShardGenerations(2, generations(gen1, null));
        assertFalse(cache.isUpToDate(shard0));
        executor.runAll();
        assertTrue(cache.isUpToDate(shard0));
    }

    public void testAShardWithoutAGenerationHoldsNoFilesButAShardThatWasNotAnsweredIsUnknown() {
        // the repository knows nothing of shard 0, e.g. a new index, and the answer does not include shard 1
        cache.getShardFiles(shard0);
        cache.onShardGenerations(1, generations((ShardGeneration) null));
        executor.runAll();
        assertTrue(cache.hasShardGeneration(shard0));
        assertThat(cache.getShardFiles(shard0), sameInstance(RepositoryShardFiles.NONE));
        assertFalse(cache.hasShardGeneration(shard1));
        assertThat(cache.getShardFiles(shard1), nullValue());
        assertThat(repository.shardReads, empty());
    }

    public void testTheShardGenerationsAreReplacedAsAWhole() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onShardGenerations(1, generations(gen0, gen1));
        cache.onShardGenerations(2, Map.of(shard0, new RepositoryShardGeneration(INDEX, gen0)));
        assertTrue(cache.hasShardGeneration(shard0));
        assertFalse(cache.hasShardGeneration(shard1));
        assertThat(cache.getRepositoryGeneration(), equalTo(2L));
    }

    public void testOnlyShardsWhoseGenerationChangedAreReadAgain() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        repository.shardFiles.put(gen1, shardSnapshots("_1.cfs", 20L));
        cache.getShardFiles(shard0);
        cache.getShardFiles(shard1);
        cache.onShardGenerations(1, generations(gen0, gen1));
        executor.runAll();
        assertThat(repository.shardReads, containsInAnyOrder(gen0, gen1));
        repository.shardReads.clear();

        // a snapshot finished, and changed the generation of shard 1 only
        final var newGen1 = new ShardGeneration("newGen1");
        repository.shardFiles.put(newGen1, shardSnapshots("_1.cfs", 20L, "_2.cfs", 30L));
        cache.onShardGenerations(2, generations(gen0, newGen1));
        executor.runAll();

        assertThat(repository.shardReads, contains(newGen1));
        assertTrue(holds(cache.getShardFiles(shard1), "_2.cfs", 30));
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));
    }

    public void testAChangedShardKeepsItsOldFilesUntilTheNewOnesAreRead() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.getShardFiles(shard0);
        cache.onShardGenerations(1, generations(gen0));
        executor.runAll();

        // a snapshot was deleted, and the shard has a new generation
        repository.shardFiles.put(gen1, shardSnapshots("_5.cfs", 5L));
        cache.onShardGenerations(2, generations(gen1));

        // while the shard is being read, the shard is not unknown
        assertThat(cache.getShardFiles(shard0).generation(), equalTo(gen0));
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));

        executor.runAll();
        assertThat(cache.getShardFiles(shard0).generation(), equalTo(gen1));
        assertTrue(holds(cache.getShardFiles(shard0), "_5.cfs", 5));
    }

    public void testAChangedShardKeepsItsOldFilesWhenReadingTheNewOnesFails() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.getShardFiles(shard0);
        cache.onShardGenerations(1, generations(gen0));
        executor.runAll();

        cache.onShardGenerations(2, generations(gen1)); // index-gen1 is not there (yet)
        executor.runAll();
        assertThat(cache.getShardFiles(shard0).generation(), equalTo(gen0));
    }

    public void testAMissingShardLevelBlobMakesTheShardUnknownAndIsReadAgainLater() {
        cache.onShardGenerations(1, generations(gen0));

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
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onShardGenerations(1, generations(gen0));

        assertThat(cache.getShardFiles(shard0), nullValue());
        assertThat(cache.getShardFiles(shard0), nullValue());
        assertThat(executor.runAll(), equalTo(1));
        assertThat(repository.shardReads, contains(gen0));
    }

    public void testShardsThatLeftTheNodeAreForgotten() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        repository.shardFiles.put(gen1, shardSnapshots("_1.cfs", 20L));
        cache.getShardFiles(shard0);
        cache.getShardFiles(shard1);
        cache.onShardGenerations(1, generations(gen0, gen1));
        executor.runAll();

        repository.shardReads.clear();
        cache.retainShards(Set.of(shard0));
        assertTrue(cache.hasShardGeneration(shard0));
        assertFalse(cache.hasShardGeneration(shard1));
        assertThat(cache.getShardFiles(shard1), nullValue()); // not read either, as it has no shard generation any more
        cache.onShardGenerations(2, generations(gen0, gen1));
        executor.runAll();
        assertThat(repository.shardReads, contains(gen1)); // shard 0 is unchanged
    }

    public void testNothingIsReadOnceClosed() {
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onShardGenerations(1, generations(gen0));
        assertThat(cache.getShardFiles(shard0), nullValue()); // the read is queued
        cache.close();
        executor.runAll();
        assertThat(repository.shardReads, empty());

        cache.onShardGenerations(2, generations(gen1));
        assertThat(cache.getRepositoryGeneration(), equalTo(1L));
    }

    public void testTheResultOfAReadIsHandedBackToTheStateExecutor() {
        final var stateExecutor = new ManualExecutor();
        final var cache = new RepositoryFilesCache("repo", repository, executor, stateExecutor);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onShardGenerations(1, generations(gen0));
        assertThat(cache.getShardFiles(shard0), nullValue());

        // the read ran, but its result is only there once the state executor has it
        executor.runAll();
        assertThat(repository.shardReads, contains(gen0));
        assertThat(cache.getShardFiles(shard0), nullValue());
        stateExecutor.runAll();
        assertTrue(holds(cache.getShardFiles(shard0), "_0.cfs", 10));
    }

    public void testTheResultOfAReadOfAShardThatLeftMeanwhileIsDropped() {
        final var stateExecutor = new ManualExecutor();
        final var cache = new RepositoryFilesCache("repo", repository, executor, stateExecutor);
        repository.shardFiles.put(gen0, shardSnapshots("_0.cfs", 10L));
        cache.onShardGenerations(1, generations(gen0));
        cache.getShardFiles(shard0);
        executor.runAll();
        cache.retainShards(Set.of());
        stateExecutor.runAll();

        repository.shardReads.clear();
        cache.onShardGenerations(2, generations(gen0));
        assertThat(cache.getShardFiles(shard0), nullValue()); // not the result of the read for the shard that had left
        assertThat(executor.runAll(), equalTo(1));
    }
}
