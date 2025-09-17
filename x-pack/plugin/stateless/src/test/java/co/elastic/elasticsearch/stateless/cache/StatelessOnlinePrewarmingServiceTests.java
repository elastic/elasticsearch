/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

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
                assertTrue(vbcc.appendCommit(ref, randomBoolean()));
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
                cacheFile = fakeNode.sharedCacheService.getCacheFile(cacheKey, regionSize + secondWarmedRegionLength);
            } else {
                cacheFile = fakeNode.sharedCacheService.getCacheFile(cacheKey, regionSize);
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
                assertTrue(vbcc.appendCommit(ref, randomBoolean()));
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
                        BlobLocation location,
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
                            location,
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
                    return new StatelessSharedBlobCacheService(nodeEnvironment, settings, threadPool, BlobCacheMetrics.NOOP) {
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

}
