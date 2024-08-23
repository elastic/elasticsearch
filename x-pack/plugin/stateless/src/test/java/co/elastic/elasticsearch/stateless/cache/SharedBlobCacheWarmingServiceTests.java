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

import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
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
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.greaterThan;
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
                ESTestCase::randomNonNegativeLong
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref));
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
            var indexCommits = fakeNode.generateIndexCommits(between(30, 40));

            var vbcc = new VirtualBatchedCompoundCommit(
                fakeNode.shardId,
                "node-id",
                primaryTerm,
                indexCommits.get(0).getGeneration(),
                (v) -> null,
                ESTestCase::randomNonNegativeLong
            );

            for (StatelessCommitRef ref : indexCommits) {
                assertTrue(vbcc.appendCommit(ref));
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
                    termAndGen.generation(),
                    termAndGen.primaryTerm(),
                    node.node.getEphemeralId(),
                    Map.of("file", blobLocation),
                    0,
                    Set.of()
                );

                // Warm the cache and verify the range is fetched with minimization as expected
                final PlainActionFuture<Void> future = new PlainActionFuture<>();
                node.warmingService.warmCache(
                    randomAlphaOfLength(10),
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
                termAndGen.generation(),
                termAndGen.primaryTerm(),
                node.node.getEphemeralId(),
                Map.of("file", blobLocation),
                0,
                Set.of()
            );

            // Warm the cache and verify the range is fetched with minimization as expected
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            node.warmingService.warmCache(
                randomAlphaOfLength(10),
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
                    .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), "1MB")
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), "32KB")
                    .build();
            }
        };
    }
}
