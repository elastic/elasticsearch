/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.full;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.Constants;
import org.elasticsearch.blobcache.BlobCacheTestUtils.FSyncTrackingFileSystemProvider;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.ShardEviction;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.Collections.emptySortedSet;
import static org.elasticsearch.blobcache.BlobCacheTestUtils.randomRanges;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.randomPopulateAndReads;
import static org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService.resolveSnapshotCache;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@LuceneTestCase.SuppressFileSystems("ExtrasFS") // we don't want extra empty dirs in snapshot cache root dirs
public class CacheServiceTests extends AbstractSearchableSnapshotsTestCase {

    private static FSyncTrackingFileSystemProvider fileSystemProvider;

    @BeforeClass
    public static void installFileSystem() {
        fileSystemProvider = new FSyncTrackingFileSystemProvider(PathUtils.getDefaultFileSystem(), createTempDir());
        PathUtilsForTesting.installMock(fileSystemProvider.getFileSystem(null));
    }

    @AfterClass
    public static void removeFileSystem() {
        fileSystemProvider.tearDown();
        fileSystemProvider = null;
    }

    public void testCacheSynchronization() throws Exception {
        final int numShards = randomIntBetween(1, 3);
        final Index index = new Index(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()));
        final String snapshotUUID = UUIDs.randomBase64UUID(random());
        final String snapshotIndexName = UUIDs.randomBase64UUID(random());

        logger.debug("--> creating shard cache directories on disk");
        final Path[] shardsCacheDirs = new Path[numShards];
        for (int i = 0; i < numShards; i++) {
            final Path shardDataPath = randomShardPath(new ShardId(index, i));
            assertFalse(Files.exists(shardDataPath));

            logger.debug("--> creating directories [{}] for shard [{}]", shardDataPath.toAbsolutePath(), i);
            shardsCacheDirs[i] = Files.createDirectories(CacheService.resolveSnapshotCache(shardDataPath).resolve(snapshotUUID));
        }

        try (CacheService cacheService = defaultCacheService()) {
            logger.debug("--> setting large cache sync interval (explicit cache synchronization calls in test)");
            cacheService.setCacheSyncInterval(TimeValue.timeValueMillis(Long.MAX_VALUE));
            cacheService.start();

            // Keep a count of the number of writes for every cache file existing in the cache
            final Map<CacheKey, Tuple<CacheFile, Integer>> previous = new HashMap<>();

            for (int iteration = 0; iteration < between(1, 10); iteration++) {

                final Map<CacheKey, Tuple<CacheFile, Integer>> updates = new HashMap<>();

                logger.trace("--> more random reads/writes from existing cache files");
                for (Map.Entry<CacheKey, Tuple<CacheFile, Integer>> cacheEntry : randomSubsetOf(previous.entrySet())) {
                    final CacheKey cacheKey = cacheEntry.getKey();
                    final CacheFile cacheFile = cacheEntry.getValue().v1();

                    final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                    cacheFile.acquire(listener);

                    final SortedSet<ByteRange> newCacheRanges = randomPopulateAndReads(cacheFile);
                    assertThat(cacheService.isCacheFileToSync(cacheFile), is(newCacheRanges.isEmpty() == false));
                    if (newCacheRanges.isEmpty() == false) {
                        final int numberOfWrites = cacheEntry.getValue().v2() + 1;
                        updates.put(cacheKey, Tuple.tuple(cacheFile, numberOfWrites));
                    }
                    cacheFile.release(listener);
                }

                logger.trace("--> creating new cache files and randomly read/write them");
                for (int i = 0; i < between(1, 25); i++) {
                    final ShardId shardId = new ShardId(index, randomIntBetween(0, numShards - 1));
                    final String fileName = Strings.format("file_%d_%d", iteration, i);
                    final CacheKey cacheKey = new CacheKey(snapshotUUID, snapshotIndexName, shardId, fileName);
                    final CacheFile cacheFile = cacheService.get(cacheKey, randomIntBetween(0, 10_000), shardsCacheDirs[shardId.id()]);

                    final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                    cacheFile.acquire(listener);

                    final SortedSet<ByteRange> newRanges = randomPopulateAndReads(cacheFile);
                    assertThat(cacheService.isCacheFileToSync(cacheFile), is(newRanges.isEmpty() == false));
                    updates.put(cacheKey, Tuple.tuple(cacheFile, newRanges.isEmpty() ? 0 : 1));
                    cacheFile.release(listener);
                }

                logger.trace("--> evicting random cache files");
                final Map<CacheFile, Integer> evictions = new HashMap<>();
                for (CacheKey evictedCacheKey : randomSubsetOf(Sets.union(previous.keySet(), updates.keySet()))) {
                    cacheService.removeFromCache(evictedCacheKey);
                    Tuple<CacheFile, Integer> evicted = previous.remove(evictedCacheKey);
                    if (evicted != null) {
                        evictions.put(evicted.v1(), evicted.v2());
                        updates.remove(evictedCacheKey);
                    } else {
                        evicted = updates.remove(evictedCacheKey);
                        evictions.put(evicted.v1(), 0);
                    }
                }

                logger.trace("--> capturing expected number of fsyncs per cache directory before synchronization");
                final Map<Path, Integer> cacheDirFSyncs = new HashMap<>();
                for (int i = 0; i < shardsCacheDirs.length; i++) {
                    final Path shardCacheDir = shardsCacheDirs[i];
                    final ShardId shardId = new ShardId(index, i);
                    final Integer numberOfFSyncs = fileSystemProvider.getNumberOfFSyncs(shardCacheDir);
                    if (updates.entrySet()
                        .stream()
                        .filter(update -> update.getValue().v2() != null)
                        .filter(update -> update.getValue().v2() > 0)
                        .anyMatch(update -> update.getKey().shardId().equals(shardId))) {
                        cacheDirFSyncs.put(shardCacheDir, numberOfFSyncs == null ? 1 : numberOfFSyncs + 1);
                    } else {
                        cacheDirFSyncs.put(shardCacheDir, numberOfFSyncs);
                    }
                }

                logger.debug("--> synchronizing cache files [#{}]", iteration);
                cacheService.synchronizeCache();

                logger.trace("--> verifying cache synchronization correctness");
                cacheDirFSyncs.forEach(
                    (dir, expectedNumberOfFSyncs) -> assertThat(
                        fileSystemProvider.getNumberOfFSyncs(dir),
                        Constants.WINDOWS ? nullValue() : equalTo(expectedNumberOfFSyncs)
                    )
                );
                evictions.forEach((cacheFile, expectedNumberOfFSyncs) -> {
                    assertThat(cacheService.isCacheFileToSync(cacheFile), is(false));
                    assertThat(fileSystemProvider.getNumberOfFSyncs(cacheFile.getFile()), equalTo(expectedNumberOfFSyncs));
                });
                previous.putAll(updates);
                previous.forEach((key, cacheFileAndExpectedNumberOfFSyncs) -> {
                    CacheFile cacheFile = cacheFileAndExpectedNumberOfFSyncs.v1();
                    assertThat(cacheService.isCacheFileToSync(cacheFile), is(false));
                    assertThat(fileSystemProvider.getNumberOfFSyncs(cacheFile.getFile()), equalTo(cacheFileAndExpectedNumberOfFSyncs.v2()));
                });
            }
        }
    }

    public void testPut() throws Exception {
        try (CacheService cacheService = defaultCacheService()) {
            final long fileLength = randomLongBetween(0L, 1000L);
            final CacheKey cacheKey = new CacheKey(
                UUIDs.randomBase64UUID(random()),
                randomAlphaOfLength(5).toLowerCase(Locale.ROOT),
                new ShardId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()), randomInt(5)),
                randomAlphaOfLength(105).toLowerCase(Locale.ROOT)
            );

            final Path cacheDir = Files.createDirectories(
                resolveSnapshotCache(randomShardPath(cacheKey.shardId())).resolve(cacheKey.snapshotUUID())
            );
            final String cacheFileUuid = UUIDs.randomBase64UUID(random());
            final SortedSet<ByteRange> cacheFileRanges = randomBoolean() ? randomRanges(fileLength) : emptySortedSet();

            if (randomBoolean()) {
                final Path cacheFilePath = cacheDir.resolve(cacheFileUuid);
                Files.createFile(cacheFilePath);

                cacheService.put(cacheKey, fileLength, cacheDir, cacheFileUuid, cacheFileRanges);

                cacheService.start();
                final CacheFile cacheFile = cacheService.get(cacheKey, fileLength, cacheDir);
                assertThat(cacheFile, notNullValue());
                assertThat(cacheFile.getFile(), equalTo(cacheFilePath));
                assertThat(cacheFile.getCacheKey(), equalTo(cacheKey));
                assertThat(cacheFile.getLength(), equalTo(fileLength));

                for (ByteRange cacheFileRange : cacheFileRanges) {
                    assertThat(cacheFile.getAbsentRangeWithin(cacheFileRange), nullValue());
                }
            } else {
                final FileNotFoundException exception = expectThrows(
                    FileNotFoundException.class,
                    () -> cacheService.put(cacheKey, fileLength, cacheDir, cacheFileUuid, cacheFileRanges)
                );
                cacheService.start();
                assertThat(exception.getMessage(), containsString(cacheFileUuid));
            }
        }
    }

    public void testMarkShardAsEvictedInCache() throws Exception {
        final CacheService cacheService = defaultCacheService();
        cacheService.start();

        final List<CacheFile> randomCacheFiles = randomCacheFiles(cacheService);
        assertThat(cacheService.pendingShardsEvictions(), aMapWithSize(0));

        final ShardEviction shard = randomShardEvictionFrom(randomCacheFiles);
        final List<CacheFile> cacheFilesAssociatedWithShard = filterByShard(shard, randomCacheFiles);
        cacheFilesAssociatedWithShard.forEach(cacheFile -> assertTrue(Files.exists(cacheFile.getFile())));

        final BlockingEvictionListener blockingListener = new BlockingEvictionListener();
        final CacheFile randomCacheFile = randomFrom(cacheFilesAssociatedWithShard);
        assertTrue(Files.exists(randomCacheFile.getFile()));
        randomCacheFile.acquire(blockingListener);

        final List<CacheFile> randomEvictedCacheFiles = randomSubsetOf(randomCacheFiles);
        for (CacheFile randomEvictedCacheFile : randomEvictedCacheFiles) {
            if (randomEvictedCacheFile != randomCacheFile) {
                cacheService.removeFromCache(randomEvictedCacheFile.getCacheKey());
            }
        }

        for (int i = 0; i < between(1, 3); i++) {
            cacheService.markShardAsEvictedInCache(shard.snapshotUUID(), shard.snapshotIndexName(), shard.shardId());
        }

        blockingListener.waitForBlock();

        assertThat(cacheService.pendingShardsEvictions(), aMapWithSize(1));
        assertTrue(cacheService.isPendingShardEviction(shard));

        blockingListener.unblock();

        assertBusy(() -> assertThat(cacheService.pendingShardsEvictions(), aMapWithSize(0)));

        for (CacheFile cacheFile : randomCacheFiles) {
            final boolean evicted = cacheFilesAssociatedWithShard.contains(cacheFile) || randomEvictedCacheFiles.contains(cacheFile);
            assertThat(
                "Cache file [" + cacheFile + "] should " + (evicted ? "be deleted" : "exist"),
                Files.notExists(cacheFile.getFile()),
                equalTo(evicted)
            );
        }
        cacheService.close();

        if (randomBoolean()) {
            // mark shard as evicted after cache service is stopped should have no effect
            cacheService.markShardAsEvictedInCache(shard.snapshotUUID(), shard.snapshotIndexName(), shard.shardId());
            assertThat(cacheService.pendingShardsEvictions(), aMapWithSize(0));
        }
    }

    public void testProcessShardEviction() throws Exception {
        final CacheService cacheService = defaultCacheService();
        cacheService.start();

        final List<CacheFile> randomCacheFiles = randomCacheFiles(cacheService);
        assertThat(cacheService.pendingShardsEvictions(), aMapWithSize(0));

        final ShardEviction shard = randomShardEvictionFrom(randomCacheFiles);
        final List<CacheFile> cacheFilesAssociatedWithShard = filterByShard(shard, randomCacheFiles);
        cacheFilesAssociatedWithShard.forEach(cacheFile -> assertTrue(Files.exists(cacheFile.getFile())));

        final BlockingEvictionListener blockingListener = new BlockingEvictionListener();
        final CacheFile randomCacheFile = randomFrom(cacheFilesAssociatedWithShard);
        assertTrue(Files.exists(randomCacheFile.getFile()));
        randomCacheFile.acquire(blockingListener);

        cacheService.markShardAsEvictedInCache(shard.snapshotUUID(), shard.snapshotIndexName(), shard.shardId());

        final Map<CacheFile, Boolean> afterShardRecoveryCacheFiles = ConcurrentCollections.newConcurrentMap();
        final Future<?> waitForShardEvictionFuture = threadPool.generic().submit(() -> {
            cacheService.waitForCacheFilesEvictionIfNeeded(shard.snapshotUUID(), shard.snapshotIndexName(), shard.shardId());
            for (CacheFile cacheFile : cacheFilesAssociatedWithShard) {
                afterShardRecoveryCacheFiles.put(cacheFile, Files.exists(cacheFile.getFile()));
            }
        });

        blockingListener.waitForBlock();

        final Map<ShardEviction, Future<?>> pendingShardsEvictions = cacheService.pendingShardsEvictions();
        assertTrue(cacheService.isPendingShardEviction(shard));
        assertThat(pendingShardsEvictions, aMapWithSize(1));

        final Future<?> pendingShardEvictionFuture = pendingShardsEvictions.get(shard);
        assertTrue(Files.exists(randomCacheFile.getFile()));
        assertThat(pendingShardEvictionFuture, notNullValue());
        assertFalse(pendingShardEvictionFuture.isDone());

        blockingListener.unblock();
        FutureUtils.get(waitForShardEvictionFuture);

        assertTrue(pendingShardEvictionFuture.isDone());
        FutureUtils.get(pendingShardEvictionFuture);

        cacheFilesAssociatedWithShard.forEach(
            cacheFile -> assertFalse("Cache file should be evicted: " + cacheFile, Files.exists(cacheFile.getFile()))
        );
        afterShardRecoveryCacheFiles.forEach(
            (cacheFile, exists) -> assertFalse("Cache file should have been evicted after shard recovery: " + cacheFile, exists)
        );
        assertThat(cacheService.pendingShardsEvictions(), aMapWithSize(0));

        cacheService.stop();
    }

    private static class BlockingEvictionListener implements CacheFile.EvictionListener {

        private final CountDownLatch evictionLatch = new CountDownLatch(1);
        private final CountDownLatch releaseLatch = new CountDownLatch(1);

        @Override
        public void onEviction(CacheFile evictedCacheFile) {
            try {
                evictionLatch.countDown();
                releaseLatch.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } finally {
                evictedCacheFile.release(this);
            }
        }

        public void waitForBlock() {
            try {
                evictionLatch.await();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            }
        }

        public void unblock() {
            releaseLatch.countDown();
        }
    }

    /**
     * Picks up a random searchable snapshot shard from a list of existing cache files and builds a {@link ShardEviction} object from it.
     *
     * @param cacheFiles a list of existing cache files
     * @return a random {@link ShardEviction} object
     */
    private static ShardEviction randomShardEvictionFrom(List<CacheFile> cacheFiles) {
        return randomFrom(listOfShardEvictions(cacheFiles));
    }

    private static Set<ShardEviction> listOfShardEvictions(List<CacheFile> cacheFiles) {
        return cacheFiles.stream()
            .map(CacheFile::getCacheKey)
            .map(cacheKey -> new ShardEviction(cacheKey.snapshotUUID(), cacheKey.snapshotIndexName(), cacheKey.shardId()))
            .collect(Collectors.toSet());
    }

    private List<CacheFile> filterByShard(ShardEviction shard, List<CacheFile> cacheFiles) {
        return cacheFiles.stream().filter(cacheFile -> shard.matches(cacheFile.getCacheKey())).collect(Collectors.toList());
    }
}
