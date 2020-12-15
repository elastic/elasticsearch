/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.apache.lucene.util.Constants;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.io.PathUtilsForTesting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.index.store.cache.TestUtils.FSyncTrackingFileSystemProvider;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.SortedSet;

import static java.util.Collections.emptySortedSet;
import static org.elasticsearch.index.store.cache.TestUtils.randomPopulateAndReads;
import static org.elasticsearch.index.store.cache.TestUtils.randomRanges;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

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
    }

    public void testCacheSynchronization() throws Exception {
        assumeFalse("https://github.com/elastic/elasticsearch/issues/65543", Constants.WINDOWS);
        final int numShards = randomIntBetween(1, 3);
        final Index index = new Index(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()));
        final SnapshotId snapshotId = new SnapshotId("_snapshot_name", UUIDs.randomBase64UUID(random()));
        final IndexId indexId = new IndexId("_index_name", UUIDs.randomBase64UUID(random()));

        logger.debug("--> creating shard cache directories on disk");
        final Path[] shardsCacheDirs = new Path[numShards];
        for (int i = 0; i < numShards; i++) {
            final Path shardDataPath = randomFrom(nodeEnvironment.availableShardPaths(new ShardId(index, i)));
            assertFalse(Files.exists(shardDataPath));

            logger.debug("--> creating directories [{}] for shard [{}]", shardDataPath.toAbsolutePath(), i);
            shardsCacheDirs[i] = Files.createDirectories(CacheService.resolveSnapshotCache(shardDataPath).resolve(snapshotId.getUUID()));
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

                    final SortedSet<Tuple<Long, Long>> newCacheRanges = randomPopulateAndReads(cacheFile);
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
                    final String fileName = String.format(Locale.ROOT, "file_%d_%d", iteration, i);
                    final CacheKey cacheKey = new CacheKey(snapshotId, indexId, shardId, fileName);
                    final CacheFile cacheFile = cacheService.get(cacheKey, randomIntBetween(1, 10_000), shardsCacheDirs[shardId.id()]);

                    final CacheFile.EvictionListener listener = evictedCacheFile -> {};
                    cacheFile.acquire(listener);

                    final SortedSet<Tuple<Long, Long>> newRanges = randomPopulateAndReads(cacheFile);
                    assertThat(cacheService.isCacheFileToSync(cacheFile), is(newRanges.isEmpty() == false));
                    updates.put(cacheKey, Tuple.tuple(cacheFile, newRanges.isEmpty() ? 0 : 1));
                    cacheFile.release(listener);
                }

                logger.trace("--> evicting random cache files");
                final Map<CacheFile, Integer> evictions = new HashMap<>();
                for (CacheKey evictedCacheKey : randomSubsetOf(Sets.union(previous.keySet(), updates.keySet()))) {
                    cacheService.removeFromCache(evictedCacheKey::equals);
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
                        .anyMatch(update -> update.getKey().getShardId().equals(shardId))) {
                        cacheDirFSyncs.put(shardCacheDir, numberOfFSyncs == null ? 1 : numberOfFSyncs + 1);
                    } else {
                        cacheDirFSyncs.put(shardCacheDir, numberOfFSyncs);
                    }
                }

                logger.debug("--> synchronizing cache files [#{}]", iteration);
                cacheService.synchronizeCache();

                logger.trace("--> verifying cache synchronization correctness");
                cacheDirFSyncs.forEach(
                    (dir, expectedNumberOfFSyncs) -> assertThat(fileSystemProvider.getNumberOfFSyncs(dir), equalTo(expectedNumberOfFSyncs))
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
        final Path cacheDir = createTempDir();
        try (CacheService cacheService = defaultCacheService()) {
            final long fileLength = randomLongBetween(0L, 1000L);
            final CacheKey cacheKey = new CacheKey(
                new SnapshotId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random())),
                new IndexId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random())),
                new ShardId(randomAlphaOfLength(5).toLowerCase(Locale.ROOT), UUIDs.randomBase64UUID(random()), randomInt(5)),
                randomAlphaOfLength(105).toLowerCase(Locale.ROOT)
            );
            final String cacheFileUuid = UUIDs.randomBase64UUID(random());
            final SortedSet<Tuple<Long, Long>> cacheFileRanges = randomBoolean() ? randomRanges(fileLength) : emptySortedSet();

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

                for (Tuple<Long, Long> cacheFileRange : cacheFileRanges) {
                    assertThat(cacheFile.getAbsentRangeWithin(cacheFileRange.v1(), cacheFileRange.v2()), nullValue());
                }
            } else {
                final FileNotFoundException exception = expectThrows(
                    FileNotFoundException.class,
                    () -> cacheService.put(cacheKey, fileLength, cacheDir, cacheFileUuid, cacheFileRanges)
                );
                assertThat(exception.getMessage(), containsString(cacheFileUuid));
            }
        }
    }
}
