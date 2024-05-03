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

import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.FileCacheKey;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;
import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
