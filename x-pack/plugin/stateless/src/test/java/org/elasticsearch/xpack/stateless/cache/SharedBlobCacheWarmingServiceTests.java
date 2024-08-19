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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.test.ESTestCase;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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

    @SuppressWarnings("unchecked")
    public void testMinimizeRange() throws Exception {
        final long rangeSize = ByteSizeValue.ofMb(between(1, 16)).getBytes();
        final long stepSize = rangeSize / 4;

        try (var node = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry()) {
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
        }) {
            final var cacheService = mock(StatelessSharedBlobCacheService.class);
            when(cacheService.getRangeSize()).thenReturn(Math.toIntExact(rangeSize));
            when(cacheService.getRegionSize()).thenReturn(Math.toIntExact(rangeSize));
            final IndexShard indexShard = mock(IndexShard.class);
            when(indexShard.shardId()).thenReturn(node.shardId);
            when(indexShard.store()).thenReturn(node.indexingStore);
            when(indexShard.state()).thenReturn(IndexShardState.RECOVERING);
            when(indexShard.routingEntry()).thenReturn(
                TestShardRouting.newShardRouting(node.shardId, node.node.getId(), true, ShardRoutingState.INITIALIZING)
            );

            final var warmingService = new SharedBlobCacheWarmingService(
                cacheService,
                node.threadPool,
                TelemetryProvider.NOOP,
                Settings.builder()
                    .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSize))
                    .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(rangeSize))
                    .put(SharedBlobCacheWarmingService.PREWARMING_RANGE_MINIMIZATION_STEP.getKey(), ByteSizeValue.ofBytes(stepSize))
                    .build()
            );

            final var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomLongBetween(3, 42));
            // Test file located on each quarter
            for (int i = 1; i <= 4; i++) {
                Mockito.clearInvocations(cacheService);
                final int region = between(1, 10);
                final long fileLength = randomLongBetween(1, 100);
                final long rangeStart = region * rangeSize;
                final long offset = randomLongBetween(stepSize * (i - 1), stepSize * i - fileLength) + rangeStart;
                final long minimizedEnd = rangeStart + stepSize * i;
                final var commit = new StatelessCompoundCommit(
                    node.shardId,
                    termAndGen.generation(),
                    termAndGen.primaryTerm(),
                    node.node.getEphemeralId(),
                    Map.of(
                        "file",
                        new BlobLocation(
                            new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(termAndGen.generation()), termAndGen),
                            offset,
                            fileLength
                        )
                    ),
                    0,
                    Set.of()
                );

                warmingService.warmCacheForShardRecovery(
                    randomAlphaOfLength(10),
                    indexShard,
                    commit,
                    node.indexingDirectory.getBlobStoreCacheDirectory()
                );
                final ArgumentCaptor<ActionListener<Boolean>> listener = ArgumentCaptor.forClass(ActionListener.class);

                assertBusy(() -> {
                    verify(cacheService).maybeFetchRange(
                        any(),
                        eq(region),
                        eq(ByteRange.of(rangeStart, minimizedEnd)),
                        anyLong(),
                        any(),
                        any(),
                        listener.capture()
                    );
                    listener.getValue().onResponse(true);
                });
            }

            {
                // Always fully warm up region 0
                Mockito.clearInvocations(cacheService);
                final int region = 0;
                final long fileLength = randomLongBetween(1, 100);
                final long offset = randomLongBetween(0, rangeSize - fileLength);
                final var commit = new StatelessCompoundCommit(
                    node.shardId,
                    termAndGen.generation(),
                    termAndGen.primaryTerm(),
                    node.node.getEphemeralId(),
                    Map.of(
                        "file",
                        new BlobLocation(
                            new BlobFile(StatelessCompoundCommit.blobNameFromGeneration(termAndGen.generation()), termAndGen),
                            offset,
                            fileLength
                        )
                    ),
                    0,
                    Set.of()
                );
                warmingService.warmCacheForShardRecovery(
                    randomAlphaOfLength(10),
                    indexShard,
                    commit,
                    node.indexingDirectory.getBlobStoreCacheDirectory()
                );
                final ArgumentCaptor<ActionListener<Boolean>> listener = ArgumentCaptor.forClass(ActionListener.class);

                assertBusy(() -> {
                    verify(cacheService).maybeFetchRange(
                        any(),
                        eq(region),
                        eq(ByteRange.of(0, rangeSize)),
                        anyLong(),
                        any(),
                        any(),
                        listener.capture()
                    );
                    listener.getValue().onResponse(true);
                });
            }
        }
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
