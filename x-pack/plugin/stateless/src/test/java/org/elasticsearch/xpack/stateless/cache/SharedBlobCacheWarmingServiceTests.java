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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReaderService;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.ObjectStoreCacheBlobReader;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.VirtualBatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobCacheIndexInput;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.lucene.StatelessCommitRef;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.test.FakeStatelessNode;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING;
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
                vbcc.getTotalSizeInBytes()
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
            assertThat(Math.toIntExact(vbcc.getTotalSizeInBytes()), greaterThan(fakeNode.sharedCacheService.getRegionSize()));

            PlainActionFuture<Void> future = new PlainActionFuture<>();
            fakeNode.warmingService.warmCacheBeforeUpload(vbcc, future);
            future.actionGet();

            StatelessSharedBlobCacheService sharedCacheService = fakeNode.sharedCacheService;

            SharedBlobCacheService<FileCacheKey>.CacheFile cacheFile = sharedCacheService.getCacheFile(
                new FileCacheKey(vbcc.getShardId(), vbcc.getPrimaryTermAndGeneration().primaryTerm(), vbcc.getBlobName()),
                vbcc.getTotalSizeInBytes()
            );

            ByteBuffer buffer = ByteBuffer.allocate(fakeNode.sharedCacheService.getRegionSize());
            assertTrue(cacheFile.tryRead(buffer, 0));
            assertFalse(cacheFile.tryRead(ByteBuffer.allocate(1), buffer.capacity()));

            BytesStreamOutput output = new BytesStreamOutput();
            vbcc.getBytesByRange(0, fakeNode.sharedCacheService.getRegionSize(), output);

            buffer.flip();
            BytesReference bytesReference = BytesReference.fromByteBuffer(buffer);
            assertEquals(output.bytes(), bytesReference);
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
            fakeNode.warmingService.warmCacheRecovery(
                SEARCH,
                indexShard,
                lastCommit,
                fakeNode.searchDirectory,
                null,
                refillCacheCompletionListener
            );
            safeGet(refillCacheCompletionListener);
            // make the object store inaccessible
            isObjectStoreAccessible.set(false);
            // this read should be served up entirely from the cache
            PlainActionFuture<Void> readReferencedCommitsListener = new PlainActionFuture<>();
            ObjectStoreService.readReferencedCompoundCommitsUsingCache(
                lastCommit,
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
                        .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), randomBoolean())
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
                vbcc.getTotalSizeInBytes()
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
            fakeNode.warmingService.warmCacheRecovery(
                SEARCH,
                indexShard,
                frozenBcc.lastCompoundCommit(),
                fakeNode.searchDirectory,
                null,
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
            node.warmingService.warmCacheRecovery(
                INDEXING,
                mockIndexShard(node),
                commit,
                node.indexingDirectory.getBlobStoreCacheDirectory(),
                null,
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
                node.warmingService.warmCacheRecovery(
                    randomFrom(Type.values()),
                    indexShard,
                    commit,
                    node.indexingDirectory.getBlobStoreCacheDirectory(),
                    null,
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
                    minimizedEnd
                );
                assertTrue(cacheFile.tryRead(ByteBuffer.allocate(Math.toIntExact(minimizedEnd - rangeStart)), rangeStart));
            }
        }
    }

    public void testWarmFirstRegionInFullWithMinimizedRange() throws Exception {
        final long rangeSize = ByteSizeValue.ofMb(between(1, 16)).getBytes();
        final long stepSize = rangeSize / 4;
        try (var node = createFakeNodeForMinimisingRange(rangeSize, stepSize)) {
            final IndexShard indexShard = mockIndexShard(node);
            final var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomLongBetween(3, 42));

            // Always fully warm up region 0
            final int region = 0;
            final long fileLength = randomLongBetween(1, 100);
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
            node.warmingService.warmCacheRecovery(
                SEARCH, // needs to be search so that region 0 is not skipped by index warming with replicated content
                indexShard,
                commit,
                node.indexingDirectory.getBlobStoreCacheDirectory(),
                null,
                future
            );
            safeGet(future);
            verify(node.sharedCacheService).maybeFetchRange(
                any(),
                eq(region),
                eq(ByteRange.of(0, rangeSize)),
                anyLong(),
                any(),
                any(),
                anyActionListener()
            );

            // Data should be available in the cache
            final var cacheFile = node.sharedCacheService.getCacheFile(
                new FileCacheKey(node.shardId, blobLocation.primaryTerm(), blobLocation.blobName()),
                rangeSize
            );
            assertTrue(cacheFile.tryRead(ByteBuffer.allocate(Math.toIntExact(rangeSize)), 0));
        }
    }

    public void testWarmingRatioCalculations() {
        long boostWindowMillis = TimeUnit.DAYS.toMillis(7);
        int searchPower = 100;

        // verify a middle timestamp in the middle of the boost window is half warmed
        long now = System.currentTimeMillis();
        long deltaMs = TimeUnit.DAYS.toMillis(3) + TimeUnit.HOURS.toMillis(12); // 3.5 days
        long middleTimestamp = now - deltaMs;
        double warmingRatio = SharedBlobCacheWarmingService.calculateWarmingRatio(now, middleTimestamp, boostWindowMillis, searchPower);
        assertEquals(0.5, warmingRatio, 0);

        // verify a timestamp of now is fully warmed
        now = System.currentTimeMillis();
        deltaMs = 0;
        middleTimestamp = now - deltaMs;
        warmingRatio = SharedBlobCacheWarmingService.calculateWarmingRatio(now, middleTimestamp, boostWindowMillis, searchPower);
        assertEquals(1.0, warmingRatio, 0);

        // verify a timestamp at the start of the boost window is not warmed at all
        now = System.currentTimeMillis();
        deltaMs = TimeUnit.DAYS.toMillis(7);
        middleTimestamp = now - deltaMs;
        warmingRatio = SharedBlobCacheWarmingService.calculateWarmingRatio(now, middleTimestamp, boostWindowMillis, searchPower);
        assertEquals(0, warmingRatio, 0);

        // verify a timestamp before the start of the boost window is not warmed at all
        now = System.currentTimeMillis();
        deltaMs = TimeUnit.DAYS.toMillis(14);
        middleTimestamp = now - deltaMs;
        warmingRatio = SharedBlobCacheWarmingService.calculateWarmingRatio(now, middleTimestamp, boostWindowMillis, searchPower);
        assertEquals(0, warmingRatio, 0);
    }

    private FakeStatelessNode createFakeNodeForMinimisingRange(long rangeSize, long stepSize) throws IOException {
        return new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected Settings nodeSettings() {
                Settings settings = super.nodeSettings();
                return Settings.builder()
                    .put(settings)
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(2L * rangeSize))
                    .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSize))
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSize))
                    .put(SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(), ByteSizeValue.ofBytes(stepSize))
                    // this test is designed to cover warming of commit files headers & footers but offline warming works differently
                    // and is out of scope for this test
                    .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_PREFETCH_COMMITS_ENABLED_SETTING.getKey(), "false")
                    .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), "false")
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
}
