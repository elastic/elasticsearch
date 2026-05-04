/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.IndexingShardCacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.ObjectStoreCacheBlobReader;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommitTestUtils;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobCacheIndexInput;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput.BUFFER_SIZE;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING_EARLY;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type.SEARCH;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SharedBlobCacheWarmingServiceTests extends ESTestCase {

    @Override
    public String[] tmpPaths() {
        // cache requires a single data path
        return new String[] { createTempDir().toAbsolutePath().toString() };
    }

    public void testUploadWarmingSingleRegion() throws IOException {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var indexCommits = fakeNode.generateIndexCommits(between(1, 3));

            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                indexCommits.get(0).getGeneration(),
                (v) -> null,
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
            }

            vbcc.freeze();

            assertThat(Math.toIntExact(vbcc.getTotalSizeInBytes()), lessThanOrEqualTo(fakeNode.sharedCacheService.getRegionSize()));

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            fakeNode.warmingService.warmCacheBeforeUpload(vbcc, future);
            future.actionGet();

            StatelessSharedBlobCacheService sharedCacheService = fakeNode.sharedCacheService;

            SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile = sharedCacheService.getCacheFile(
                new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName()),
                vbcc.getTotalSizeInBytes(),
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(vbcc.getTotalSizeInBytes()));
            assertTrue(cacheFile.tryRead(buffer, 0));

            BytesStreamOutput output = new BytesStreamOutput();
            vbcc.getBytesByRange(0, vbcc.getTotalSizeInBytes(), output);

            buffer.flip();
            BytesReference bytesReference = BytesReference.fromByteBuffer(buffer);
            assertEquals(output.bytes(), bytesReference);
        }
    }

    public void testUploadWarmingMultiRegionCommit() throws IOException {
        var primaryTerm = 1;
        try (var fakeNode = createFakeNode(primaryTerm)) {
            var indexCommits = fakeNode.generateIndexCommits(between(60, 80));

            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                indexCommits.get(0).getGeneration(),
                (v) -> null,
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
            }

            vbcc.freeze();
            long totalSize = vbcc.getTotalSizeInBytes();
            int regionSize = fakeNode.sharedCacheService.getRegionSize();
            assertThat(Math.toIntExact(totalSize), greaterThan(regionSize));

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            fakeNode.warmingService.warmCacheBeforeUpload(vbcc, future);
            future.actionGet();

            StatelessSharedBlobCacheService sharedCacheService = fakeNode.sharedCacheService;

            SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile = sharedCacheService.getCacheFile(
                new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName()),
                totalSize,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // Verify all regions are warmed
            int endingRegion = sharedCacheService.getEndingRegion(totalSize);
            for (int region = 0; region <= endingRegion; region++) {
                long regionStart = (long) region * regionSize;
                int regionLength = Math.toIntExact(Math.min(regionSize, totalSize - regionStart));
                ByteBuffer buffer = ByteBuffer.allocate(regionLength);
                assertTrue("region " + region + " should be cached", cacheFile.tryRead(buffer, regionStart));

                BytesStreamOutput output = new BytesStreamOutput();
                vbcc.getBytesByRange(regionStart, regionLength, output);

                buffer.flip();
                BytesReference bytesReference = BytesReference.fromByteBuffer(buffer);
                assertEquals("region " + region + " data mismatch", output.bytes(), bytesReference);
            }
        }
    }

    public void testUploadWarmingRespectsMaxPrewarmSizeSetting() throws IOException {
        var primaryTerm = 1;
        int maxPrewarmRegions = randomIntBetween(2, 4);

        // Test 1: maxPrewarmSize limits how many regions are warmed
        try (
            var fakeNode = createFakeNodeWithUploadPrewarmMaxSize(primaryTerm, ByteSizeValue.ofBytes((long) maxPrewarmRegions * 64 * 1024))
        ) {
            int regionSize = fakeNode.sharedCacheService.getRegionSize();
            long maxPrewarmSizeBytes = (long) maxPrewarmRegions * regionSize;

            var indexCommits = fakeNode.generateIndexCommits(between(60, 80));
            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
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
            while (vbcc.getTotalSizeInBytes() <= maxPrewarmSizeBytes + regionSize) {
                indexCommits = fakeNode.generateIndexCommits(between(20, 40));
                for (StatelessCommitRef ref : indexCommits) {
                    assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
                }
            }
            vbcc.freeze();

            long totalSize = vbcc.getTotalSizeInBytes();
            int warmableEndRegion = fakeNode.sharedCacheService.getEndingRegion(maxPrewarmSizeBytes);
            int totalEndRegion = fakeNode.sharedCacheService.getEndingRegion(totalSize);
            assertThat("VBCC should span more regions than the prewarm limit", totalEndRegion, greaterThan(warmableEndRegion));

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            fakeNode.warmingService.warmCacheBeforeUpload(vbcc, future);
            future.actionGet();

            StatelessSharedBlobCacheService sharedCacheService = fakeNode.sharedCacheService;
            SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile = sharedCacheService.getCacheFile(
                new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName()),
                totalSize,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // Regions within the prewarm limit should be warmed with correct data
            for (int region = 0; region <= warmableEndRegion; region++) {
                long regionStart = (long) region * regionSize;
                int regionLength = Math.toIntExact(Math.min(regionSize, totalSize - regionStart));
                ByteBuffer buffer = ByteBuffer.allocate(regionLength);
                assertTrue("region " + region + " should be cached", cacheFile.tryRead(buffer, regionStart));

                BytesStreamOutput output = new BytesStreamOutput();
                vbcc.getBytesByRange(regionStart, regionLength, output);
                buffer.flip();
                assertEquals("region " + region + " data mismatch", output.bytes(), BytesReference.fromByteBuffer(buffer));
            }

            // Regions beyond the prewarm limit should NOT be warmed
            for (int region = warmableEndRegion + 1; region <= totalEndRegion; region++) {
                long regionStart = (long) region * regionSize;
                int regionLength = Math.toIntExact(Math.min(regionSize, totalSize - regionStart));
                ByteBuffer buffer = ByteBuffer.allocate(regionLength);
                assertFalse("region " + region + " should NOT be cached", cacheFile.tryRead(buffer, regionStart));
            }
        }

        // Test 2: Everything is warmed when maxPrewarmSize exceeds the VBCC size
        try (var fakeNode = createFakeNodeWithUploadPrewarmMaxSize(primaryTerm, ByteSizeValue.ofMb(8))) {
            int regionSize = fakeNode.sharedCacheService.getRegionSize();

            var indexCommits = fakeNode.generateIndexCommits(between(60, 80));
            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
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
            while (vbcc.getTotalSizeInBytes() <= regionSize) {
                indexCommits = fakeNode.generateIndexCommits(between(20, 40));
                for (StatelessCommitRef ref : indexCommits) {
                    assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
                }
            }
            vbcc.freeze();

            long totalSize = vbcc.getTotalSizeInBytes();
            int totalEndRegion = fakeNode.sharedCacheService.getEndingRegion(totalSize);
            assertThat(totalEndRegion, greaterThan(0));

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            fakeNode.warmingService.warmCacheBeforeUpload(vbcc, future);
            future.actionGet();

            StatelessSharedBlobCacheService sharedCacheService = fakeNode.sharedCacheService;
            SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile = sharedCacheService.getCacheFile(
                new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName()),
                totalSize,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // ALL regions should be warmed with correct data
            for (int region = 0; region <= totalEndRegion; region++) {
                long regionStart = (long) region * regionSize;
                int regionLength = Math.toIntExact(Math.min(regionSize, totalSize - regionStart));
                ByteBuffer buffer = ByteBuffer.allocate(regionLength);
                assertTrue("region " + region + " should be cached", cacheFile.tryRead(buffer, regionStart));

                BytesStreamOutput output = new BytesStreamOutput();
                vbcc.getBytesByRange(regionStart, regionLength, output);
                buffer.flip();
                assertEquals("region " + region + " data mismatch", output.bytes(), BytesReference.fromByteBuffer(buffer));
            }
        }
    }

    public void testReadReferencedCompoundCommitsFromCacheAfterSearchShardWarming() throws Exception {
        final long primaryTerm = randomLongBetween(10, 42);
        RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        // very small cache region and cache range sizes in order to verify independent cache prefetching calls
        // for the individual CC headers inside BCC blobs (we need to avoid that the first cache prefetch populates everything)
        long regionSizeInBytes = SharedBytes.PAGE_SIZE * randomLongBetween(1, 3);
        long rangeSizeInBytes = regionSizeInBytes + SharedBytes.PAGE_SIZE * randomFrom(0L, 1L, 2L);
        AtomicBoolean isObjectStoreAccessible = new AtomicBoolean(true);
        try (
            var fakeNode = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                recordingMeterRegistry
            ) {
                @Override
                protected Settings nodeSettings() {
                    Settings settings = super.nodeSettings();
                    return Settings.builder()
                        .put(settings)
                        .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(4))
                        .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
                        .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSizeInBytes))
                        // this test explicitly covers the "offline warming" search shard recovery path
                        .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), true)
                        .put(
                            SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(),
                            ByteSizeValue.ofBytes(SharedBytes.PAGE_SIZE)
                        )
                        .build();
                }

                @Override
                protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                    return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool) {
                        @Override
                        public CacheBlobReader getCacheBlobReader(
                            ShardId shardId,
                            LongFunction<BlobContainer> blobContainer,
                            BlobFile blobFile,
                            MutableObjectStoreUploadTracker objectStoreUploadTracker,
                            LongConsumer totalBytesReadFromObjectStore,
                            LongConsumer totalBytesReadFromIndexing,
                            BlobCacheMetrics.CachePopulationReason cachePopulationReason,
                            Executor objectStoreFetchExecutor,
                            String fileName
                        ) {
                            return new ObjectStoreCacheBlobReader(
                                blobContainer.apply(blobFile.primaryTerm()),
                                blobFile.blobName(),
                                cacheService.getRangeSize(),
                                objectStoreFetchExecutor
                            ) {
                                @Override
                                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                                    if (isObjectStoreAccessible.get() == false) {
                                        listener.onFailure(new Exception("Unexpected invocation after cache population"));
                                    } else {
                                        super.getRangeInputStream(position, length, listener);
                                    }
                                }
                            };
                        }
                    };
                }
            }
        ) {
            int bccCount = randomIntBetween(1, 16);
            Map<String, BlobLocation> uploadedBlobLocations = new HashMap<>();
            VirtualBatchedCompoundCommit vbcc = null;
            for (int i = 0; i < bccCount; i++) {
                var indexCommits = fakeNode.generateIndexCommits(
                    randomIntBetween(1, 16),
                    randomBoolean(),
                    randomBoolean(),
                    generation -> {}
                );
                vbcc = new VirtualBatchedCompoundCommit(
                    fakeNode.shardId,
                    "fake-node-id",
                    primaryTerm,
                    indexCommits.get(0).getGeneration(),
                    fileName -> uploadedBlobLocations.get(fileName),
                    ESTestCase::randomNonNegativeLong,
                    fakeNode.sharedCacheService.getRegionSize(),
                    randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
                );
                for (StatelessCommitRef ref : indexCommits) {
                    assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
                }
                vbcc.freeze();
                // upload vbcc
                var indexBlobContainer = fakeNode.getShardContainer();
                try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload()) {
                    indexBlobContainer.writeBlobAtomic(
                        OperationPurpose.INDICES,
                        vbcc.getBlobName(),
                        vbccInputStream,
                        vbcc.getTotalSizeInBytes(),
                        true
                    );
                }
                uploadedBlobLocations.putAll(vbcc.lastCompoundCommit().commitFiles());
            }
            var lastCommit = vbcc.getFrozenBatchedCompoundCommit().lastCompoundCommit();
            fakeNode.searchDirectory.updateCommit(lastCommit);
            // test harness to be able to call `warmingService.warmCache`
            var indexShard = mockIndexShard(fakeNode);
            PlainActionFuture<Void> refillCacheCompletionListener = new PlainActionFuture<>();
            fakeNode.warmingService.warmCache(
                SEARCH,
                indexShard,
                lastCommit,
                fakeNode.searchDirectory,
                null,
                false,
                refillCacheCompletionListener
            );
            safeGet(refillCacheCompletionListener);
            // make the object store inaccessible
            isObjectStoreAccessible.set(false);
            // this read should be served up entirely from the cache
            PlainActionFuture<Void> readReferencedCommitsListener = new PlainActionFuture<>();
            ObjectStoreService.readReferencedCompoundCommitsUsingCache(
                lastCommit.commitFiles(),
                null,
                fakeNode.searchDirectory,
                randomFrom(IOContext.DEFAULT, BlobCacheIndexInput.WARMING),
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                referencedCC -> {},
                readReferencedCommitsListener
            );
            safeGet(readReferencedCommitsListener);
        }
    }

    public void testByteRangeWarmingForBCCAndVBCC() throws Exception {
        final long primaryTerm = randomLongBetween(10, 42);
        // very small cache region, cache range sizes, and vbcc get chunk size, in order to verify independent cache prefetching calls
        long regionSizeInBytes = SharedBytes.PAGE_SIZE * randomLongBetween(1, 3);
        long vbccGetSizeInBytes = SharedBytes.PAGE_SIZE * randomLongBetween(1, 3);
        long rangeSizeInBytes = regionSizeInBytes + SharedBytes.PAGE_SIZE * randomFrom(0L, 1L, 2L);
        final List<VirtualBatchedCompoundCommit> vbccs = new ArrayList<>();
        try (
            var fakeNode = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                new RecordingMeterRegistry()
            ) {
                @Override
                protected Settings nodeSettings() {
                    Settings settings = super.nodeSettings();
                    return Settings.builder()
                        .put(settings)
                        .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(4))
                        .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
                        .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSizeInBytes))
                        .put(
                            CacheBlobReaderService.TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.getKey(),
                            ByteSizeValue.ofBytes(vbccGetSizeInBytes)
                        )
                        .put(
                            SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(),
                            ByteSizeValue.ofBytes(SharedBytes.PAGE_SIZE)
                        )
                        .build();
                }

                @Override
                protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                    return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool) {
                        @Override
                        protected CacheBlobReader getIndexingShardCacheBlobReader(
                            ShardId shardId,
                            PrimaryTermAndGeneration primaryTermAndGeneration,
                            String preferredNodeId
                        ) {
                            return new IndexingShardCacheBlobReader(
                                shardId,
                                primaryTermAndGeneration,
                                clusterService.localNode().getId(),
                                client,
                                TRANSPORT_BLOB_READER_CHUNK_SIZE_SETTING.get(nodeSettings),
                                threadPool
                            ) {
                                // we need to override this because the {@link TransportGetVirtualBatchedCompoundCommitChunkAction}
                                // communication between the search and the indexing nodes is not mocked in the {@link FakeStatelessNode}
                                @Override
                                public void getVirtualBatchedCompoundCommitChunk(
                                    final PrimaryTermAndGeneration virtualBccTermAndGen,
                                    final long offset,
                                    final int length,
                                    final String preferredNodeId,
                                    final ActionListener<ReleasableBytesReference> listener
                                ) {
                                    // we only expect the VBCC fetch for the second BCC, as the first one is served from the blobstore
                                    assertEquals(vbccs.get(1).getPrimaryTermAndGeneration(), virtualBccTermAndGen);
                                    int finalLength = Math.min(length, Math.toIntExact(vbccs.get(1).getTotalSizeInBytes() - offset));
                                    var bytesStreamOutput = new BytesStreamOutput(finalLength);
                                    threadPool.executor(StatelessPlugin.GET_VIRTUAL_BATCHED_COMPOUND_COMMIT_CHUNK_THREAD_POOL)
                                        .execute(() -> {
                                            ActionListener.run(listener, l -> {
                                                vbccs.get(1).getBytesByRange(offset, finalLength, bytesStreamOutput);
                                                l.onResponse(
                                                    new ReleasableBytesReference(bytesStreamOutput.bytes(), bytesStreamOutput::close)
                                                );
                                            });
                                        });
                                }
                            };
                        }
                    };
                }
            }
        ) {
            Map<String, BlobLocation> uploadedBlobLocations = new HashMap<>();
            // 2 BCCs (only the first one is uploaded)
            for (int i = 0; i < 2; i++) {
                var indexCommits = fakeNode.generateIndexCommits(randomIntBetween(1, 10), false);
                VirtualBatchedCompoundCommit vbcc = new VirtualBatchedCompoundCommit(
                    fakeNode.shardId,
                    "fake-node-id",
                    primaryTerm,
                    indexCommits.get(0).getGeneration(),
                    fileName -> uploadedBlobLocations.get(fileName),
                    ESTestCase::randomNonNegativeLong,
                    fakeNode.sharedCacheService.getRegionSize(),
                    randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
                );
                do {
                    appendCommitsToVbcc(vbcc, fakeNode.searchDirectory, indexCommits);
                    // warming is not concerned with region 0, so we need BCCs larger than 1 region for this test
                    if (vbcc.getTotalSizeInBytes() > regionSizeInBytes) {
                        break;
                    }
                    indexCommits = fakeNode.generateIndexCommits(randomIntBetween(1, 10), false);
                } while (true);
                vbcc.freeze();
                vbccs.add(vbcc);
                // upload the first vbcc only
                if (i == 0) {
                    var indexBlobContainer = fakeNode.getShardContainer();
                    try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload()) {
                        indexBlobContainer.writeBlobAtomic(
                            OperationPurpose.INDICES,
                            vbcc.getBlobName(),
                            vbccInputStream,
                            vbcc.getTotalSizeInBytes(),
                            true
                        );
                    }
                    BlobStoreCacheDirectoryTestUtils.updateLatestUploadedBcc(fakeNode.searchDirectory, vbcc.primaryTermAndGeneration());
                    BlobStoreCacheDirectoryTestUtils.updateLatestCommitInfo(
                        fakeNode.searchDirectory,
                        vbcc.lastCompoundCommit().primaryTermAndGeneration(),
                        fakeNode.clusterService.localNode().getId()
                    );
                    uploadedBlobLocations.putAll(vbcc.lastCompoundCommit().commitFiles());
                }
            }
            var indexShard = mock(IndexShard.class);
            when(indexShard.store()).thenReturn(fakeNode.searchStore);
            when(indexShard.shardId()).thenReturn(fakeNode.shardId);

            for (var vbcc : vbccs) {
                // make sure cache is clean
                fakeNode.sharedCacheService.forceEvict(ignore -> true);
                // random offset to warm up to (exclusive), must be beyond the first region and should not exceed the total vbcc length
                var endOffset = randomLongBetween(regionSizeInBytes + 1, vbcc.getTotalSizeInBytes());
                // warm the range after the first region and the offset
                PlainActionFuture<Void> warmListener = new PlainActionFuture<>();
                // warm up to the endOffset (exclusive)
                fakeNode.warmingService.warmBlobOffsets(
                    indexShard,
                    Map.of(new BlobFile(vbcc.getBlobName(), vbcc.getPrimaryTermAndGeneration()), endOffset),
                    warmListener
                );
                safeGet(warmListener);
                List<ByteRange> testRanges = randomList(16, 16, () -> {
                    long start = randomLongBetween(regionSizeInBytes, endOffset - 1);
                    // end is exclusive for byte ranges
                    long end = randomLongBetween(start + 1, endOffset);
                    return ByteRange.of(start, end);
                });
                ByteBuffer tempBuffer = ByteBuffer.allocate((int) regionSizeInBytes);
                for (var testRange : testRanges) {
                    SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile = fakeNode.sharedCacheService.getCacheFile(
                        new FileCacheKey(indexShard.shardId(), primaryTerm, vbcc.getBlobName()),
                        vbcc.getTotalSizeInBytes(),
                        new SharedBlobCacheService.CacheMissHandler() {
                            @Override
                            public Releasable record(long bytes) {
                                throw new AssertionError("this should not happen, this represents a test failure");
                            }

                            @Override
                            public SharedBlobCacheService.CacheMissHandler copy() {
                                return this;
                            }
                        }
                    );
                    // this read should be served up from the warmed cache
                    var read = cacheFile.populateAndRead(
                        testRange,
                        testRange,
                        (SharedBlobCacheService.RangeAvailableHandler) (channel, channelPos, relativePos, length) -> {
                            // assert that what's read from the cache matches the vbcc contents
                            tempBuffer.clear();
                            tempBuffer.limit(length);
                            channel.read(tempBuffer, channelPos);
                            tempBuffer.flip();
                            try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload((int) testRange.start() + relativePos, length)) {
                                byte[] vbccBytes = vbccInputStream.readAllBytes();
                                var vbccBuffer = ByteBuffer.wrap(vbccBytes);
                                var compare = vbccBuffer.compareTo(tempBuffer);
                                assertThat(compare, equalTo(0));
                            }
                            return length;
                        },
                        (SharedBlobCacheService.RangeMissingHandler) (
                            channel,
                            channelPos,
                            streamFactory,
                            relativePos,
                            length,
                            progressUpdater,
                            completionListener) -> {
                            throw new AssertionError("this should not happen, this represents a test failure");
                        },
                        "_na_"
                    );
                    assertThat((long) read, equalTo(testRange.end() - testRange.start()));
                }
            }
        }
    }

    public void testPopulateCacheWithSharedSourceInputStreamFactory() throws Exception {

        var actualRangeInputStreamPosition = new SetOnce<Long>();
        var actualRangeInputStreamLength = new SetOnce<Integer>();
        final long primaryTerm = randomLongBetween(10, 42);

        RecordingMeterRegistry recordingMeterRegistry = new RecordingMeterRegistry();
        try (
            var fakeNode = new FakeStatelessNode(
                this::newEnvironment,
                this::newNodeEnvironment,
                xContentRegistry(),
                primaryTerm,
                TestProjectResolvers.DEFAULT_PROJECT_ONLY,
                recordingMeterRegistry
            ) {
                @Override
                protected Settings nodeSettings() {
                    Settings settings = super.nodeSettings();
                    return Settings.builder()
                        .put(settings)
                        .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
                        .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(512))
                        // range size must match region size to avoid warming going outside the blob size/region size.
                        .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(512))
                        .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), randomBoolean())
                        .put(SharedBlobCacheWarmingService.PREWARM_INDEX_SHARD_FOR_ID_LOOKUPS_SETTING.getKey(), randomBoolean())
                        .put(SharedBlobCacheWarmingService.ID_LOOKUP_PREWARM_RATIO_SETTING.getKey(), randomDoubleBetween(0, 1, true))
                        .build();
                }

                @Override
                protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                    return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool) {
                        @Override
                        public CacheBlobReader getCacheBlobReader(
                            ShardId shardId,
                            LongFunction<BlobContainer> blobContainer,
                            BlobFile blobFile,
                            MutableObjectStoreUploadTracker objectStoreUploadTracker,
                            LongConsumer totalBytesReadFromObjectStore,
                            LongConsumer totalBytesReadFromIndexing,
                            BlobCacheMetrics.CachePopulationReason cachePopulationReason,
                            Executor objectStoreFetchExecutor,
                            String fileName
                        ) {
                            return new ObjectStoreCacheBlobReader(
                                blobContainer.apply(blobFile.primaryTerm()),
                                blobFile.blobName(),
                                cacheService.getRangeSize(),
                                objectStoreFetchExecutor
                            ) {
                                @Override
                                protected InputStream getRangeInputStream(long position, int length) throws IOException {
                                    actualRangeInputStreamPosition.set(position);
                                    actualRangeInputStreamLength.set(length);
                                    return super.getRangeInputStream(position, length);
                                }
                            };
                        }
                    };
                }
            }
        ) {
            var indexCommits = fakeNode.generateIndexCommits(randomIntBetween(10, 20));

            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "fake-node-id",
                primaryTerm,
                indexCommits.get(0).getGeneration(),
                (v) -> null,
                ESTestCase::randomNonNegativeLong,
                fakeNode.sharedCacheService.getRegionSize(),
                randomIntBetween(0, fakeNode.sharedCacheService.getRegionSize())
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref, randomBoolean(), null));
            }

            vbcc.freeze();

            // ensure that everything fits single region
            assertThat(Math.toIntExact(vbcc.getTotalSizeInBytes()), lessThan(fakeNode.sharedCacheService.getRegionSize()));

            // upload vbcc
            var indexBlobContainer = fakeNode.getShardContainer();
            try (var vbccInputStream = vbcc.getFrozenInputStreamForUpload()) {
                indexBlobContainer.writeBlobAtomic(
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

            // test harness to be able to call `warmingService.warmCache`
            var indexShard = mockIndexShard(fakeNode);

            // read disjoint ranges of BCC into cache effectively creating cache holes
            var cacheKey = new FileCacheKey(indexShard.shardId(), primaryTerm, vbcc.getBlobName());
            SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile = fakeNode.sharedCacheService.getCacheFile(
                cacheKey,
                vbcc.getTotalSizeInBytes(),
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            var writeBuffer = ByteBuffer.allocate(8192);
            for (int start = SharedBytes.PAGE_SIZE; start < vbcc.getTotalSizeInBytes() - SharedBytes.PAGE_SIZE; start += 2
                * SharedBytes.PAGE_SIZE) {
                var range = ByteRange.of(start, start + SharedBytes.PAGE_SIZE);
                String resourceDescription = vbcc.getBlobName();
                long bytesRead = cacheFile.populateAndRead(
                    range,
                    range,
                    (channel, channelPos, relativePos, length) -> length,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> {
                        var shardContainer = fakeNode.getShardContainer();
                        try (
                            var in = shardContainer.readBlob(
                                OperationPurpose.INDICES,
                                resourceDescription,
                                range.start() + relativePos,
                                length
                            )
                        ) {
                            SharedBytes.copyToCacheFileAligned(channel, in, channelPos, progressUpdater, writeBuffer.clear());
                        }
                        ActionListener.completeWith(completionListener, () -> null);
                    },
                    resourceDescription
                );
                assertThat(bytesRead, equalTo((long) SharedBytes.PAGE_SIZE));
            }

            ByteBuffer testBuffer = ByteBuffer.allocate((int) vbcc.getTotalSizeInBytes());
            // there is no continues data in cache since there are cache holes
            assertThat(cacheFile.tryRead(testBuffer, 0), equalTo(false));

            // re-populate cache and fill holes
            PlainActionFuture<Void> refillCacheCompletionListener = new PlainActionFuture<>();
            fakeNode.warmingService.warmCache(
                SEARCH,
                indexShard,
                frozenBcc.lastCompoundCommit(),
                fakeNode.searchDirectory,
                null,
                false,
                refillCacheCompletionListener
            );
            safeGet(refillCacheCompletionListener);

            // assert that whole bcc is cached which implies absence of cache holes
            assertThat(cacheFile.tryRead(testBuffer.clear(), 0), equalTo(true));
            // check that position and length were set only once
            assertThat(actualRangeInputStreamPosition.get(), equalTo(0L));
            assertThat(actualRangeInputStreamLength.get(), equalTo(fakeNode.sharedCacheService.getRegionSize()));
            List<Measurement> measurements = fakeNode.meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.miss_that_triggered_read.total");
            Measurement first = measurements.getFirst();
            assertThat(first.attributes().get("file_extension"), is("other"));
            assertThat(first.value(), is(1L));
        }
    }

    public void testOnlyTheFirstRegionIsLoadedWhenReplicatedContentIsPresent() throws IOException {
        var primaryTerm = 1;
        var cacheSize = ByteSizeValue.ofMb(10L);
        var regionSize = ByteSizeValue.ofBytes((long) randomIntBetween(1, 3) * 2 * SharedBytes.PAGE_SIZE);
        try (
            var node = new FakeStatelessNode(
                TestEnvironment::newEnvironment,
                settings -> new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings)),
                xContentRegistry(),
                primaryTerm
            ) {
                @Override
                protected Settings nodeSettings() {
                    return Settings.builder()
                        .put(super.nodeSettings())
                        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                        .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
                        .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
                        .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
                        .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), regionSize)
                        .put(SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(), regionSize)
                        .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
                        .build();
                }
            }
        ) {
            final var primaryTermAndGeneration = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomLongBetween(3, 42));
            final var blobFile = new BlobFile(
                StatelessCompoundCommit.blobNameFromGeneration(primaryTermAndGeneration.generation()),
                primaryTermAndGeneration
            );

            var commit = TestUtils.getCommitWithInternalFilesReplicatedRanges(
                node.shardId,
                blobFile,
                node.node.getEphemeralId(),
                0,
                regionSize.getBytes()
            );

            var blobFileRanges = BlobFileRanges.computeBlobFileRanges(true, commit, 0, commit.internalFiles());
            node.indexingDirectory.getBlobStoreCacheDirectory().updateMetadata(blobFileRanges, randomNonNegativeLong());

            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            node.warmingService.warmCache(
                INDEXING,
                mockIndexShard(node),
                commit,
                node.indexingDirectory.getBlobStoreCacheDirectory(),
                Map.of(), // don't do any warming besides the regular shard recovery kind
                false,
                future
            );
            safeGet(future);

            SharedBlobCacheService.Stats stats = node.sharedCacheService.getStats();
            assertThat(stats.writeCount(), equalTo(0L));
            assertThat(stats.missCount(), equalTo(0L));
        }
    }

    public void testMinimizedRange() throws Exception {
        final long rangeSize = ByteSizeValue.ofMb(between(1, 16)).getBytes();
        final long stepSize = rangeSize / 4;
        try (var node = createFakeNodeForMinimisingRange(rangeSize, stepSize)) {
            final IndexShard indexShard = mockIndexShard(node);
            final var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomLongBetween(3, 42));
            // Test file located on each quarter
            for (int i = 1; i <= 4; i++) {
                Mockito.clearInvocations(node.sharedCacheService);
                node.sharedCacheService.forceEvict(ignore -> true);
                final int region = between(1, 10);
                final long fileLength = randomLongBetween(1, 100);
                final long rangeStart = region * rangeSize;
                final long offset = randomLongBetween(stepSize * (i - 1), stepSize * i - fileLength) + rangeStart;
                final long minimizedEnd = rangeStart + stepSize * i;
                final var blobLocation = new BlobLocation(
                    new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(termAndGen.generation()), termAndGen),
                    offset,
                    fileLength
                );
                final var commit = new StatelessCompoundCommit(
                    node.shardId,
                    termAndGen,
                    1L,
                    node.node.getEphemeralId(),
                    Map.of("file", blobLocation),
                    0,
                    Set.of(),
                    0L,
                    InternalFilesReplicatedRanges.EMPTY,
                    Map.of(),
                    null
                );

                // Warm the cache and verify the range is fetched with minimization as expected
                final PlainActionFuture<Void> future = new PlainActionFuture<>();
                node.warmingService.warmCache(
                    randomFrom(Type.values()),
                    indexShard,
                    commit,
                    node.indexingDirectory.getBlobStoreCacheDirectory(),
                    null,
                    false,
                    future
                );
                safeGet(future);
                verify(node.sharedCacheService).maybeFetchRange(
                    any(),
                    eq(region),
                    eq(ByteRange.of(rangeStart, minimizedEnd)),
                    anyLong(),
                    any(),
                    any(),
                    anyActionListener()
                );

                // Data should be available in the cache
                final var cacheFile = node.sharedCacheService.getCacheFile(
                    new FileCacheKey(node.shardId, blobLocation.primaryTerm(), blobLocation.blobName()),
                    minimizedEnd,
                    SharedBlobCacheService.CacheMissHandler.NOOP
                );
                assertTrue(cacheFile.tryRead(ByteBuffer.allocate(Math.toIntExact(minimizedEnd - rangeStart)), rangeStart));
            }
        }
    }

    // proves that range minimization is not effective when warming (for recovery) locations inside the first cache region
    public void testWarmFirstRegionInFullEvenWithMinimizedRange() throws Exception {
        final long rangeSize = ByteSizeValue.ofMb(between(1, 16)).getBytes();
        final long stepSize = rangeSize / 4;
        try (var node = createFakeNodeForMinimisingRange(rangeSize, stepSize)) {
            final IndexShard indexShard = mockIndexShard(node);
            final var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomLongBetween(3, 42));

            final long fileLength = randomLongBetween(1, rangeSize);
            final long offset = randomLongBetween(0, rangeSize - fileLength);
            final var blobLocation = new BlobLocation(
                new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(termAndGen.generation()), termAndGen),
                offset,
                fileLength
            );
            final var commit = new StatelessCompoundCommit(
                node.shardId,
                termAndGen,
                1L,
                node.node.getEphemeralId(),
                Map.of("file", blobLocation),
                0,
                Set.of(),
                0L,
                InternalFilesReplicatedRanges.EMPTY,
                Map.of(),
                null
            );

            // Warm the cache and verify the range is fetched with minimization as expected
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            node.warmingService.warmCache(
                randomValueOtherThanMany(t -> t.skipsWarmingForRegion0Locations, () -> randomFrom(Type.values())),
                indexShard,
                commit,
                node.indexingDirectory.getBlobStoreCacheDirectory(),
                Map.of(), // don't do any warming besides the regular shard recovery kind
                false,
                future
            );
            safeGet(future);
            verify(node.sharedCacheService).maybeFetchRange(
                any(),
                eq(0),// Always fully warm up region 0
                eq(ByteRange.of(0, rangeSize)),
                anyLong(),
                any(),
                any(),
                anyActionListener()
            );

            // Data should be available in the cache
            final var cacheFile = node.sharedCacheService.getCacheFile(
                new FileCacheKey(node.shardId, blobLocation.primaryTerm(), blobLocation.blobName()),
                rangeSize,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );
            assertTrue(cacheFile.tryRead(ByteBuffer.allocate(Math.toIntExact(rangeSize)), 0));
        }
    }

    public void testIdLookupPreWarming() throws Exception {
        final boolean preWarmEnabled = usually();
        final boolean preWarmRequested = usually();
        final double ratio = randomDoubleBetween(0.0, 1.0, true);
        final boolean shouldPreWarmForIdLookup = preWarmEnabled && preWarmRequested;
        final long regionSize = SharedBytes.PAGE_SIZE;

        try (var node = createFakeNodeForPreWarming(ByteSizeValue.ofMb(4), regionSize, SharedBytes.PAGE_SIZE, preWarmEnabled, ratio)) {
            final IndexShard indexShard = mockIndexShard(node);
            final var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomLongBetween(3, 42));
            final var blobFile = new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(termAndGen.generation()), termAndGen);

            final Map<String, BlobLocation> commitFiles = new HashMap<>();
            long currentOffset = regionSize;
            for (var fileName : List.of("_0_0.tim", "_0_0.tip", "_0_ES87BloomFilter_0.bfi", "_1_Lucene90_0.dvd")) {
                final var length = randomLongBetween(10_000, 200_000);
                commitFiles.put(fileName, new BlobLocation(blobFile, currentOffset, length));
                currentOffset += length;
            }

            final var commit = new StatelessCompoundCommit(
                node.shardId,
                termAndGen,
                1L,
                node.node.getEphemeralId(),
                commitFiles,
                0,
                Set.of(),
                0L,
                InternalFilesReplicatedRanges.EMPTY,
                Map.of(),
                null
            );

            // Warm the cache with INDEXING_EARLY or INDEXING type and preWarmForIdLookup=true
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            node.warmingService.warmCache(
                randomFrom(INDEXING_EARLY, INDEXING),
                indexShard,
                commit,
                node.indexingDirectory.getBlobStoreCacheDirectory(),
                null,
                preWarmRequested,
                future
            );
            safeGet(future);

            final var cacheFile = node.sharedCacheService.getCacheFile(
                new FileCacheKey(node.shardId, blobFile.primaryTerm(), blobFile.blobName()),
                currentOffset,
                SharedBlobCacheService.CacheMissHandler.NOOP
            );

            // For each file, verify the expected caching behavior based on shouldPreWarmForIdLookup and ratio.
            // When shouldPreWarmForIdLookup:
            // .tip/.bfi: fully cached
            // .tim: header = max(ratio * fileLength, 1024)
            // .dvd: header = 1024
            // When disabled:
            // all files: header = 1024 only
            for (var entry : commitFiles.entrySet()) {
                var fileName = entry.getKey();
                var loc = entry.getValue();
                var ext = LuceneFilesExtensions.fromFile(fileName);
                boolean shouldBeFullyWarmed = shouldPreWarmForIdLookup
                    && (ext == LuceneFilesExtensions.TIP || ext == LuceneFilesExtensions.BFI);

                if (shouldBeFullyWarmed) {
                    // All regions of the file should be cached
                    int startRegion = node.sharedCacheService.getRegion(loc.offset());
                    int endRegion = node.sharedCacheService.getEndingRegion(loc.offset() + loc.fileLength());
                    for (int r = startRegion; r <= endRegion; r++) {
                        long rStart = (long) r * regionSize;
                        long readStart = Math.max(rStart, loc.offset());
                        long readEnd = Math.min(rStart + regionSize, loc.offset() + loc.fileLength());
                        assertTrue(
                            fileName + " region " + r + " should be cached (fully warmed)",
                            cacheFile.tryRead(ByteBuffer.allocate(Math.toIntExact(readEnd - readStart)), readStart)
                        );
                    }
                } else {
                    // Only the header portion should be cached.
                    // For .tim with id lookup pre-warm: header = max(ratio * fileLength, 1024)
                    // Otherwise: header = 1024 (BUFFER_SIZE)
                    long headerPreWarmSize;
                    if (shouldPreWarmForIdLookup && ext == LuceneFilesExtensions.TIM) {
                        headerPreWarmSize = Math.max((long) (ratio * loc.fileLength()), BUFFER_SIZE);
                    } else {
                        headerPreWarmSize = BUFFER_SIZE;
                    }
                    int startRegion = node.sharedCacheService.getRegion(loc.offset());
                    int lastHeaderRegion = node.sharedCacheService.getEndingRegion(loc.offset() + headerPreWarmSize);
                    // All regions covering the header should be cached
                    for (int r = startRegion; r <= lastHeaderRegion; r++) {
                        long rStart = (long) r * regionSize;
                        long readStart = Math.max(rStart, loc.offset());
                        long readEnd = Math.min(rStart + regionSize, loc.offset() + loc.fileLength());
                        assertTrue(
                            fileName + " header region " + r + " should be cached",
                            cacheFile.tryRead(ByteBuffer.allocate(Math.toIntExact(readEnd - readStart)), readStart)
                        );
                    }
                    // The region immediately after the header should NOT be cached, but we must skip the footer region.
                    int firstUncachedRegion = lastHeaderRegion + 1;
                    int footerRegion = node.sharedCacheService.getEndingRegion(loc.offset() + loc.fileLength());
                    if (firstUncachedRegion < footerRegion) {
                        long uncachedRegionStart = (long) firstUncachedRegion * regionSize;
                        assertFalse(
                            fileName + " region " + firstUncachedRegion + " (after header) should NOT be cached",
                            cacheFile.tryRead(ByteBuffer.allocate(Math.toIntExact(regionSize)), uncachedRegionStart)
                        );
                    }
                }
            }
        }
    }

    private FakeStatelessNode createFakeNodeForMinimisingRange(long rangeSize, long stepSize) throws IOException {
        return createFakeNodeForPreWarming(
            ByteSizeValue.ofBytes(2L * rangeSize),
            rangeSize,
            stepSize,
            false,
            randomDoubleBetween(0, 1, true)
        );
    }

    private FakeStatelessNode createFakeNodeForPreWarming(
        ByteSizeValue cacheSize,
        long rangeSize,
        long stepSize,
        boolean preWarmForIdLookupEnabled,
        double idLookupPreWarmRatio
    ) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected Settings nodeSettings() {
                Settings settings = super.nodeSettings();
                return Settings.builder()
                    .put(settings)
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
                    .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSize))
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSize))
                    .put(SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(), ByteSizeValue.ofBytes(stepSize))
                    // this test is designed to cover warming of commit files headers & footers but offline warming works differently
                    // and is out of scope for this test
                    .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING.getKey(), "false")
                    .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), "false")
                    .put(SharedBlobCacheWarmingService.PREWARM_INDEX_SHARD_FOR_ID_LOOKUPS_SETTING.getKey(), preWarmForIdLookupEnabled)
                    .put(SharedBlobCacheWarmingService.ID_LOOKUP_PREWARM_RATIO_SETTING.getKey(), idLookupPreWarmRatio)
                    .build();
            }

            @Override
            protected StatelessSharedBlobCacheService createCacheService(
                NodeEnvironment nodeEnvironment,
                Settings settings,
                ThreadPool threadPool,
                MeterRegistry meterRegistry
            ) {
                return spy(super.createCacheService(nodeEnvironment, settings, threadPool, meterRegistry));
            }

            @Override
            public BlobContainer wrapBlobContainer(BlobPath path, BlobContainer innerContainer) {
                return new FilterBlobContainer(innerContainer) {
                    @Override
                    protected BlobContainer wrapChild(BlobContainer child) {
                        return child;
                    }

                    @Override
                    public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                        return new InputStream() {
                            private long remaining = length;

                            @Override
                            public int read() throws IOException {
                                if (remaining == 0) {
                                    return -1;
                                } else {
                                    remaining -= 1;
                                    return 1;
                                }
                            }
                        };
                    }
                };
            }
        };
    }

    private static IndexShard mockIndexShard(FakeStatelessNode node) {
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexShard.shardId()).thenReturn(node.shardId);
        when(indexShard.store()).thenReturn(node.indexingStore);
        when(indexShard.state()).thenReturn(IndexShardState.RECOVERING);
        when(indexShard.routingEntry()).thenReturn(
            TestShardRouting.newShardRouting(node.shardId, node.node.getId(), true, ShardRoutingState.INITIALIZING)
        );
        return indexShard;
    }

    private FakeStatelessNode createFakeNodeWithUploadPrewarmMaxSize(long primaryTerm, ByteSizeValue maxPrewarmSize) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                Settings settings = super.nodeSettings();
                return Settings.builder()
                    .put(settings)
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "8MB")
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "64KB")
                    .put(SharedBlobCacheWarmingService.UPLOAD_PREWARM_MAX_SIZE_SETTING.getKey(), maxPrewarmSize)
                    .build();
            }
        };
    }

    private FakeStatelessNode createFakeNode(long primaryTerm) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry(), primaryTerm) {
            @Override
            protected Settings nodeSettings() {
                Settings settings = super.nodeSettings();
                return Settings.builder()
                    .put(settings)
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "2MB")
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "64KB")
                    .build();
            }
        };
    }

    private void appendCommitsToVbcc(
        VirtualBatchedCompoundCommit virtualBatchedCompoundCommit,
        SearchDirectory searchDirectory,
        List<StatelessCommitRef> commits
    ) {
        for (StatelessCommitRef statelessCommitRef : commits) {
            assertTrue(virtualBatchedCompoundCommit.appendCommit(statelessCommitRef, randomBoolean(), null));
            var pendingCompoundCommits = VirtualBatchedCompoundCommitTestUtils.getPendingStatelessCompoundCommits(
                virtualBatchedCompoundCommit
            );
            StatelessCompoundCommit latestCommit = pendingCompoundCommits.get(pendingCompoundCommits.size() - 1);
            searchDirectory.updateCommit(latestCommit);
        }
    }
}
