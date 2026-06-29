/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StatelessOnlinePrewarmingServiceTests extends ESTestCase {

    @Override
    public String[] tmpPaths() {
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    public void testPrewarmingOneOrTowRegionsFromSearchDirectory() throws Exception {
        long primaryTerm = 1L;
        try (FakeStatelessNode fakeNode = createFakeNode(primaryTerm, false)) {
            // randomise a commit of size either less or greater than the size of a cache region
            var indexCommits = fakeNode.generateIndexCommits(randomBoolean() ? 5 : 60);
            int regionSize = fakeNode.sharedCacheService.getRegionSize();
            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "fake-node-id",
                primaryTerm,
                indexCommits.get(0).getGeneration(),
                (v) -> null,
                ESTestCase::randomNonNegativeLong,
                regionSize,
                randomIntBetween(0, regionSize)
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
            }
            vbcc.freeze();
            // upload vbcc
            var blobContainer = fakeNode.getShardContainer();
            try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload()) {
                blobContainer.writeBlobAtomic(
                    OperationPurpose.INDICES,
                    vbcc.getBlobName(),
                    vbccInputStream,
                    vbcc.getTotalSizeInBytes(),
                    false
                );
            }
            var frozenBcc = vbcc.getFrozenBatchedCompoundCommit();

            // update search directory with latest commit
            fakeNode.searchDirectory.updateCommit(frozenBcc.lastCompoundCommit());

            SharedBlobCacheService.Stats beforeWarmingStats = fakeNode.sharedCacheService.getStats();
            CountDownLatch prewarmLatch = new CountDownLatch(1);
            IndexShard searchShard = mockSearchShard(fakeNode);

            long expectedBytesToWrite = 0L;
            Collection<BlobFileRanges> highestOffsetBlobRanges = fakeNode.searchDirectory.getHighestOffsetSegmentInfos();
            boolean twoRegionsToWarm = false;
            long secondWarmedRegionLength = 0L;
            for (BlobFileRanges highestOffsetBlobRange : highestOffsetBlobRanges) {
                if (highestOffsetBlobRange.fileOffset() + highestOffsetBlobRange.fileLength() > regionSize) {
                    // 2 regions to read
                    secondWarmedRegionLength = Math.min(
                        highestOffsetBlobRange.fileLength() + highestOffsetBlobRange.fileOffset() - regionSize,
                        regionSize
                    );
                    expectedBytesToWrite += regionSize + getAlignedByteRange(0, secondWarmedRegionLength).length();
                    twoRegionsToWarm = true;
                } else {
                    // up to one region to read
                    long lengthToRead = Math.min(regionSize, highestOffsetBlobRange.fileLength() + highestOffsetBlobRange.fileOffset());
                    expectedBytesToWrite += getAlignedByteRange(0, lengthToRead).length();
                }
            }

            fakeNode.onlinePrewarmingService.prewarm(searchShard, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    prewarmLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    prewarmLatch.countDown();
                    fail(e);
                }
            });
            prewarmLatch.await(5, TimeUnit.SECONDS);

            assertThat(fakeNode.sharedCacheService.getStats().writeBytes(), is(expectedBytesToWrite));

            CountDownLatch secondPrewamLatch = new CountDownLatch(1);
            // this should be no-op as we prewarmed the shard already
            fakeNode.onlinePrewarmingService.prewarm(searchShard, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    secondPrewamLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    secondPrewamLatch.countDown();
                    fail(e);
                }
            });
            secondPrewamLatch.await(5, TimeUnit.SECONDS);
            // no more bytes written to the cache
            assertThat(fakeNode.sharedCacheService.getStats().writeBytes(), is(expectedBytesToWrite));

            // let's also check the cache is accessible using the fast path for the ranges we prewarmed
            BlobFileRanges blobRange = highestOffsetBlobRanges.iterator().next();
            FileCacheKey cacheKey = new FileCacheKey(fakeNode.shardId, primaryTerm, blobRange.blobName());
            SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile;
            if (twoRegionsToWarm) {
                cacheFile = fakeNode.sharedCacheService.getCacheFile(
                    cacheKey,
                    regionSize + secondWarmedRegionLength,
                    SharedBlobCacheService.CacheMissHandler.NOOP
                );
            } else {
                cacheFile = fakeNode.sharedCacheService.getCacheFile(cacheKey, regionSize, SharedBlobCacheService.CacheMissHandler.NOOP);
            }

            long length = Math.min(regionSize, blobRange.fileLength() + blobRange.fileOffset());
            ByteBuffer testBuffer = ByteBuffer.allocate((int) length);
            // reading the first region should use the fast path
            assertThat(cacheFile.tryRead(testBuffer, 0), equalTo(true));

            if (twoRegionsToWarm) {
                // if we prewarmed it, reading the second region should also be fast
                ByteBuffer secondRegionBuffer = ByteBuffer.allocate((int) secondWarmedRegionLength);
                assertThat(cacheFile.tryRead(secondRegionBuffer, regionSize), equalTo(true));
            }
        }
    }

    public void testPrewarmingFailure() throws Exception {
        long primaryTerm = 1L;
        try (FakeStatelessNode fakeNode = createFakeNode(primaryTerm, true)) {
            var indexCommits = fakeNode.generateIndexCommits(randomBoolean() ? 5 : 60);
            int regionSize = fakeNode.sharedCacheService.getRegionSize();
            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "fake-node-id",
                primaryTerm,
                indexCommits.get(0).getGeneration(),
                (v) -> null,
                ESTestCase::randomNonNegativeLong,
                regionSize,
                randomIntBetween(0, regionSize)
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
            }
            vbcc.freeze();
            // upload vbcc
            var blobContainer = fakeNode.getShardContainer();
            try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload()) {
                blobContainer.writeBlobAtomic(
                    OperationPurpose.INDICES,
                    vbcc.getBlobName(),
                    vbccInputStream,
                    vbcc.getTotalSizeInBytes(),
                    false
                );
            }
            var frozenBcc = vbcc.getFrozenBatchedCompoundCommit();

            // update search directory with latest commit
            fakeNode.searchDirectory.updateCommit(frozenBcc.lastCompoundCommit());

            CountDownLatch prewarmLatch = new CountDownLatch(1);
            IndexShard searchShard = mockSearchShard(fakeNode);
            fakeNode.onlinePrewarmingService.prewarm(searchShard, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    prewarmLatch.countDown();
                    fail("expected prewarming to fail");
                }

                @Override
                public void onFailure(Exception e) {
                    prewarmLatch.countDown();
                    assertThat(e, instanceOf(IllegalStateException.class));
                    assertThat(e.getMessage(), is("oh no"));
                }
            });
            prewarmLatch.await(5, TimeUnit.SECONDS);
        }
    }

    private FakeStatelessNode createFakeNode(long primaryTerm, boolean failingCacheService) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                return Settings.builder()
                    .put(super.nodeSettings())
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "8MB")
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "128KB")
                    .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), "128KB")
                    .build();
            }

            @Override
            protected SearchDirectory createSearchDirectory(
                StatelessSharedBlobCacheService sharedCacheService,
                ShardId shardId,
                CacheBlobReaderService cacheBlobReaderService,
                MutableObjectStoreUploadTracker objectStoreUploadTracker
            ) {
                var customCacheBlobReaderService = new CacheBlobReaderService(nodeSettings, sharedCacheService, client, threadPool) {
                    @Override
                    public CacheBlobReader getCacheBlobReader(
                        ShardId shardId,
                        LongFunction<BlobContainer> blobContainer,
                        BlobFile blobFile,
                        MutableObjectStoreUploadTracker objectStoreUploadTracker,
                        LongConsumer bytesReadFromObjectStore,
                        LongConsumer bytesReadFromIndexing,
                        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
                        Executor objectStoreFetchExecutor,
                        String fileName
                    ) {
                        var originalCacheBlobReader = cacheBlobReaderService.getCacheBlobReader(
                            shardId,
                            blobContainer,
                            blobFile,
                            // The test expects to go through the blob store always
                            MutableObjectStoreUploadTracker.ALWAYS_UPLOADED,
                            bytesReadFromObjectStore,
                            bytesReadFromIndexing,
                            cachePopulationReason,
                            objectStoreFetchExecutor,
                            fileName
                        );
                        return new CacheBlobReader() {
                            @Override
                            public ByteRange getRange(long position, int length, long remainingFileLength) {
                                return getAlignedByteRange(position, length);
                            }

                            @Override
                            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                                originalCacheBlobReader.getRangeInputStream(position, length, listener);
                            }
                        };

                    }
                };
                return super.createSearchDirectory(sharedCacheService, shardId, customCacheBlobReaderService, objectStoreUploadTracker);
            }

            @Override
            protected StatelessSharedBlobCacheService createCacheService(
                NodeEnvironment nodeEnvironment,
                Settings settings,
                ThreadPool threadPool,
                MeterRegistry meterRegistry
            ) {
                if (failingCacheService) {
                    return new StatelessSharedBlobCacheService(
                        nodeEnvironment,
                        settings,
                        threadPool,
                        BlobCacheMetrics.NOOP,
                        clusterService,
                        TestUtils.mockIndicesService(clusterService),
                        new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
                    ) {
                        @Override
                        public void maybeFetchRange(
                            FileCacheKey cacheKey,
                            int region,
                            ByteRange range,
                            long blobLength,
                            RangeMissingHandler writer,
                            Executor fetchExecutor,
                            ActionListener<Boolean> listener
                        ) {
                            listener.onFailure(new IllegalStateException("oh no"));
                        }
                    };
                }
                return super.createCacheService(nodeEnvironment, settings, threadPool, meterRegistry);
            }
        };
    }

    private static ByteRange getAlignedByteRange(long position, long length) {
        var start = BlobCacheUtils.toPageAlignedSize(Math.max(position - SharedBytes.PAGE_SIZE + 1L, 0L));
        var end = BlobCacheUtils.toPageAlignedSize(position + length);
        return ByteRange.of(start, end);
    }

    private static IndexShard mockSearchShard(FakeStatelessNode node) {
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(node.shardId);
        when(indexShard.store()).thenReturn(node.searchStore);
        when(indexShard.state()).thenReturn(IndexShardState.STARTED);
        return indexShard;
    }

    /**
     * Verifies that {@link SharedBlobCacheService#isRangeFullyCached} returns {@code true} for regions
     * populated via {@link SharedBlobCacheService#restoreOccupancy} (snapshot-restore path), without any
     * prior blob-store fetch. This ensures that the {@code isRangeFullyCached} guard in
     * {@link StatelessOnlinePrewarmingService} correctly skips regions whose occupancy was seeded from a
     * cache snapshot rather than from a live fetch.
     */
    public void testIsRangeFullyCachedReturnsTrueForSnapshotRestoredEntries() throws Exception {
        int numRegions = 4;
        int regionSize = SharedBytes.PAGE_SIZE * 4;
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "test-node")
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes((long) regionSize * numRegions).getStringRep()
            )
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir().toAbsolutePath().toString())
            .put("path.data", createTempDir().toAbsolutePath().toString())
            .build();

        DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment nodeEnv = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            TestCacheService cacheService = new TestCacheService(nodeEnv, settings, taskQueue)
        ) {
            ShardId shardId = new ShardId(new Index("test-index", "uuid1"), 0);
            FileCacheKey key = new FileCacheKey(shardId, 1L, "test.cfs");

            SortedSet<ByteRange> completedRanges = new TreeSet<>();
            completedRanges.add(ByteRange.of(0, regionSize));
            SharedBlobCacheService.CacheIndexEntry<FileCacheKey> entry = new SharedBlobCacheService.CacheIndexEntry<>(
                0,
                key,
                0,
                regionSize,
                completedRanges
            );

            cacheService.restoreOccupancyForTest(List.of(entry));

            // The entire restored region must be reported as fully cached.
            assertTrue(
                "isRangeFullyCached must return true for the full restored range",
                cacheService.isRangeFullyCached(key, 0, ByteRange.of(0, regionSize))
            );

            // A sub-range within the restored region must also be reported as fully cached.
            assertTrue(
                "isRangeFullyCached must return true for a sub-range of the restored region",
                cacheService.isRangeFullyCached(key, 0, ByteRange.of(regionSize / 2, regionSize))
            );

            // A different cache key must not be reported as cached.
            FileCacheKey otherKey = new FileCacheKey(new ShardId("other", "uuid2", 0), 1L, "other.cfs");
            assertFalse(
                "isRangeFullyCached must return false for a key that was never restored",
                cacheService.isRangeFullyCached(otherKey, 0, ByteRange.of(0, regionSize))
            );
        }
    }

    /**
     * Verifies that a second call to {@link StatelessOnlinePrewarmingService#prewarm} writes no new bytes
     * to the cache. After the first prewarm populates the cache regions via blob-store fetches, the
     * {@code isRangeFullyCached} guard in {@link StatelessOnlinePrewarmingService} fires for every
     * already-cached region and skips the fetch, leaving {@link SharedBlobCacheService.Stats#writeBytes()}
     * unchanged.
     */
    public void testPrewarmingSkipsRegionsRestoredByGuard() throws Exception {
        long primaryTerm = 1L;
        try (FakeStatelessNode fakeNode = createFakeNode(primaryTerm, false)) {
            // Use a small commit (5 segments) to keep the test fast and ensure we warm at most one region.
            var indexCommits = fakeNode.generateIndexCommits(5);
            int regionSize = fakeNode.sharedCacheService.getRegionSize();
            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "fake-node-id",
                primaryTerm,
                indexCommits.get(0).getGeneration(),
                (v) -> null,
                ESTestCase::randomNonNegativeLong,
                regionSize,
                randomIntBetween(0, regionSize)
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
            }
            vbcc.freeze();

            // Upload the vbcc blob to the fake object store.
            var blobContainer = fakeNode.getShardContainer();
            try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload()) {
                blobContainer.writeBlobAtomic(
                    OperationPurpose.INDICES,
                    vbcc.getBlobName(),
                    vbccInputStream,
                    vbcc.getTotalSizeInBytes(),
                    false
                );
            }
            var frozenBcc = vbcc.getFrozenBatchedCompoundCommit();

            // Point the search directory at the newly uploaded commit.
            fakeNode.searchDirectory.updateCommit(frozenBcc.lastCompoundCommit());

            IndexShard searchShard = mockSearchShard(fakeNode);

            // First prewarm: fetches data from the blob store and populates the cache.
            CountDownLatch latch1 = new CountDownLatch(1);
            fakeNode.onlinePrewarmingService.prewarm(searchShard, ActionListener.wrap(v -> latch1.countDown(), e -> {
                fail(e);
                latch1.countDown();
            }));
            assertTrue("first prewarm timed out", latch1.await(10, TimeUnit.SECONDS));

            long writeBytesAfterFirstWarm = fakeNode.sharedCacheService.getStats().writeBytes();
            assertTrue("first prewarm must write some bytes to the cache", writeBytesAfterFirstWarm > 0);

            // Second prewarm: the isRangeFullyCached guard fires for every warmed region and skips fetches.
            CountDownLatch latch2 = new CountDownLatch(1);
            fakeNode.onlinePrewarmingService.prewarm(searchShard, ActionListener.wrap(v -> latch2.countDown(), e -> {
                fail(e);
                latch2.countDown();
            }));
            assertTrue("second prewarm timed out", latch2.await(10, TimeUnit.SECONDS));

            assertThat(
                "isRangeFullyCached guard must prevent any new cache writes on second prewarm",
                fakeNode.sharedCacheService.getStats().writeBytes(),
                equalTo(writeBytesAfterFirstWarm)
            );
        }
    }

    /**
     * A minimal subclass of {@link StatelessSharedBlobCacheService} for unit tests. Uses the protected
     * "for tests" constructor so that no snapshot handling is triggered at construction time, and
     * overrides {@link #syncSlotRange} as a no-op since there is no physical file to sync.
     */
    private static final class TestCacheService extends StatelessSharedBlobCacheService {

        TestCacheService(NodeEnvironment environment, Settings settings, DeterministicTaskQueue taskQueue) {
            super(
                environment,
                settings,
                taskQueue.getThreadPool(),
                BlobCacheMetrics.NOOP,
                new DefaultEvictionPolicy<>(),
                System::nanoTime,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            );
        }

        @Override
        public void syncSlotRange(int slot, int flags) throws IOException {
            // no-op: no physical file descriptor in unit tests
        }

        /** Expose the protected {@code restoreOccupancy} to tests. */
        public void restoreOccupancyForTest(List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> entries) {
            restoreOccupancy(entries);
        }
    }

}
