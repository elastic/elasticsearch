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
 *
 * This file was contributed to by generative AI
 */

package co.elastic.elasticsearch.stateless.lucene;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.StatelessSharedBlobCacheService;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.IndexingShardCacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreCacheBlobReader;

import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.stateless.TestUtils.newCacheService;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.pageAligned;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class SearchIndexInputTests extends ESIndexInputTestCase {

    public void testRandomReads() throws IOException {
        final ThreadPool threadPool = getThreadPool("testRandomReads");
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(randomLongBetween(0, 10_000_000));
        final var settings = sharedCacheSettings(cacheSize);
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            for (int i = 0; i < 100; i++) {
                final String fileName = randomAlphaOfLength(5) + randomFileExtension();
                final byte[] input = randomChecksumBytes(randomIntBetween(1, 100_000)).v2();
                final long primaryTerm = randomNonNegativeLong();
                final SearchIndexInput indexInput = new SearchIndexInput(
                    fileName,
                    sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, fileName), input.length),
                    randomIOContext(),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    input.length,
                    0
                );
                assertEquals(input.length, indexInput.length());
                assertEquals(0, indexInput.getFilePointer());
                byte[] output = randomReadAndSlice(indexInput, input.length);
                assertArrayEquals(input, output);
            }
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    /**
     * Test that clone copies the underlying cachefile object. Reads on cloned instances are checked by #testRandomReads
     * @throws IOException
     */
    public void testClone() throws IOException {
        final ThreadPool threadPool = getThreadPool("testRandomReads");
        final var settings = sharedCacheSettings(ByteSizeValue.ofBytes(randomLongBetween(0, 10_000_000)));
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final byte[] input = randomByteArrayOfLength(randomIntBetween(1, 100_000));
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final SearchIndexInput indexInput = new SearchIndexInput(
                fileName,
                sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, fileName), input.length),
                randomIOContext(),
                createBlobReader(fileName, input, sharedBlobCacheService),
                input.length,
                0
            );

            indexInput.seek(randomLongBetween(0, input.length - 1));
            SearchIndexInput clone = asInstanceOf(SearchIndexInput.class, indexInput.clone());
            assertThat(clone.cacheFile(), not(equalTo(indexInput.cacheFile())));
            assertThat(clone.getFilePointer(), equalTo(indexInput.getFilePointer()));
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
        }
    }

    private static CacheBlobReader createBlobReader(String fileName, byte[] input, StatelessSharedBlobCacheService sharedBlobCacheService) {

        ObjectStoreCacheBlobReader objectStore = new ObjectStoreCacheBlobReader(
            TestUtils.singleBlobContainer(fileName, input),
            fileName,
            sharedBlobCacheService.getRangeSize()
        );
        if (randomBoolean()) {
            return objectStore;
        }
        ByteSizeValue chunkSize = ByteSizeValue.ofKb(8);
        return new IndexingShardCacheBlobReader(null, null, null, chunkSize) {
            @Override
            public InputStream getRangeInputStream(long position, int length) throws IOException {
                // verifies that `getRange` does not exceed remaining file length except for padding, implicitly also
                // verifying that the remainingFileLength calculation in SearchIndexInput is correct too.
                assertThat(position + length, lessThanOrEqualTo(BlobCacheUtils.toPageAlignedSize(input.length)));
                return objectStore.getRangeInputStream(position, length);
            }
        };
    }

    private static Settings sharedCacheSettings(ByteSizeValue cacheSize) {
        return Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                pageAligned(new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB))
            )
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .build();
    }

    private static TestThreadPool getThreadPool(String name) {
        return new TestThreadPool(name, Stateless.statelessExecutorBuilders(Settings.EMPTY, true));
    }
}
