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

import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService.Type;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReaderService;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreCacheBlobReader;
import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobFileRangesTestUtils;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges;
import co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.InternalFileReplicatedRange;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
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
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService.Type.INDEXING;
import static co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService.Type.SEARCH;
import static co.elastic.elasticsearch.stateless.commits.InternalFilesReplicatedRanges.REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
                assertTrue(vbcc.appendCommit(ref, randomBoolean()));
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
                assertTrue(vbcc.appendCommit(ref, randomBoolean()));
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

    public void testPopulateCacheWithSharedSourceInputStreamFactory() throws Exception {

        var actualRangeInputStreamPosition = new SetOnce<Long>();
        var actualRangeInputStreamLength = new SetOnce<Integer>();

        try (var fakeNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
            @Override
            protected Settings nodeSettings() {
                Settings settings = super.nodeSettings();
                return Settings.builder()
                    .put(settings)
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(512))
                    .build();
            }

            @Override
            protected CacheBlobReaderService createCacheBlobReaderService(StatelessSharedBlobCacheService cacheService) {
                return new CacheBlobReaderService(nodeSettings, cacheService, client, threadPool) {
                    @Override
                    public CacheBlobReader getCacheBlobReader(
                        ShardId shardId,
                        LongFunction<BlobContainer> blobContainer,
                        BlobLocation location,
                        MutableObjectStoreUploadTracker objectStoreUploadTracker,
                        LongConsumer totalBytesReadFromObjectStore,
                        LongConsumer totalBytesReadFromIndexing,
                        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
                        Executor objectStoreFetchExecutor
                    ) {
                        return new ObjectStoreCacheBlobReader(
                            blobContainer.apply(location.primaryTerm()),
                            location.blobName(),
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
        }) {
            var indexCommits = fakeNode.generateIndexCommits(randomIntBetween(10, 20));

            var primaryTerm = 1;
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
                assertTrue(vbcc.appendCommit(ref, randomBoolean()));
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
                long bytesRead = cacheFile.populateAndRead(
                    range,
                    range,
                    (channel, channelPos, relativePos, length) -> length,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> {
                        var shardContainer = fakeNode.getShardContainer();
                        try (
                            var in = shardContainer.readBlob(
                                OperationPurpose.INDICES,
                                vbcc.getBlobName(),
                                range.start() + relativePos,
                                length
                            )
                        ) {
                            SharedBytes.copyToCacheFileAligned(channel, in, channelPos, progressUpdater, writeBuffer.clear());
                        }
                        ActionListener.completeWith(completionListener, () -> null);
                    }
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
                refillCacheCompletionListener
            );
            safeGet(refillCacheCompletionListener);

            // assert that whole bcc is cached which implies absence of cache holes
            assertThat(cacheFile.tryRead(testBuffer.clear(), 0), equalTo(true));
            // check that position and length were set only once
            assertThat(actualRangeInputStreamPosition.get(), equalTo(0L));
            assertThat(actualRangeInputStreamLength.get(), equalTo(fakeNode.sharedCacheService.getRegionSize()));
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

            long fileOffset = 0;
            List<InternalFileReplicatedRange> replicatedRanges = new ArrayList<>();
            Map<String, BlobLocation> commitFiles = new HashMap<>();

            long files = Math.min(
                randomIntBetween(1, 10),
                Math.floorDiv(regionSize.getBytes(), REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE) // ensures all ranges fit in the first region
            );
            for (int i = 0; i < files; i++) {
                var file = "_" + i + ".cfs";
                var size = randomIntBetween(256, 10240);
                if (size < REPLICATED_CONTENT_MAX_SINGLE_FILE_SIZE) {
                    replicatedRanges.add(new InternalFileReplicatedRange(fileOffset, (short) size));
                } else {
                    replicatedRanges.add(new InternalFileReplicatedRange(fileOffset, (short) 1024));
                    replicatedRanges.add(new InternalFileReplicatedRange(fileOffset + size - 16, (short) 16));
                }
                commitFiles.put(file, new BlobLocation(blobFile, fileOffset, size));
                fileOffset += size;
            }
            InternalFilesReplicatedRanges ranges = InternalFilesReplicatedRanges.from(replicatedRanges);
            commitFiles = Maps.transformValues(
                commitFiles,
                location -> new BlobLocation(location.blobFile(), ranges.dataSizeInBytes() + location.offset(), location.fileLength())
            );

            final var commit = new StatelessCompoundCommit(
                node.shardId,
                primaryTermAndGeneration,
                1L,
                node.node.getEphemeralId(),
                commitFiles,
                0,
                commitFiles.keySet(),
                0L,
                ranges
            );

            var blobFileRanges = BlobFileRangesTestUtils.computeBlobFileRanges(true, commit, 0, commit.internalFiles());
            node.indexingDirectory.getBlobStoreCacheDirectory().updateMetadata(blobFileRanges, randomNonNegativeLong());

            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            node.warmingService.warmCacheRecovery(
                INDEXING,
                mockIndexShard(node),
                commit,
                node.indexingDirectory.getBlobStoreCacheDirectory(),
                future
            );
            safeGet(future);

            assertThat(node.sharedCacheService.getStats().writeCount(), equalTo(0L));
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
                    InternalFilesReplicatedRanges.EMPTY
                );

                // Warm the cache and verify the range is fetched with minimization as expected
                final PlainActionFuture<Void> future = new PlainActionFuture<>();
                node.warmingService.warmCacheRecovery(
                    randomFrom(Type.values()),
                    indexShard,
                    commit,
                    node.indexingDirectory.getBlobStoreCacheDirectory(),
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
                InternalFilesReplicatedRanges.EMPTY
            );

            // Warm the cache and verify the range is fetched with minimization as expected
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            node.warmingService.warmCacheRecovery(
                SEARCH, // needs to be search so that region 0 is not skipped by index warming with replicated content
                indexShard,
                commit,
                node.indexingDirectory.getBlobStoreCacheDirectory(),
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
                    .build();
            }

            @Override
            protected StatelessSharedBlobCacheService createCacheService(
                NodeEnvironment nodeEnvironment,
                Settings settings,
                ThreadPool threadPool
            ) {
                return spy(super.createCacheService(nodeEnvironment, settings, threadPool));
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
