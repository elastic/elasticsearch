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
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.SwitchingCacheBlobReader;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static co.elastic.elasticsearch.stateless.TestUtils.newCacheService;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.pageAligned;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchIndexInputTests extends ESIndexInputTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = getThreadPool("SearchIndexInputTests");
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
    }

    public void testRandomReads() throws IOException {
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
        }
    }

    /**
     * Test that clone copies the underlying cachefile object. Reads on cloned instances are checked by #testRandomReads
     * @throws IOException
     */
    public void testClone() throws IOException {
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
        }
    }

    // The test explicitly set rangeSize > regionSize to trigger SharedBlobCacheService#readMultiRegions and ensure
    // things work correctly. The readSingleRegion path is more commonly tested by other tests and also by the first
    // part of the test where it reads from IndexingShardCacheBlobReader.
    public void testFillMultipleRegions() throws IOException {
        final ByteSizeValue cacheSize = ByteSizeValue.ofMb(40);
        final ByteSizeValue regionSize = ByteSizeValue.ofBytes(randomLongBetween(50, 256) * PAGE_SIZE);
        final var settings = Settings.builder()
            .put(sharedCacheSettings(cacheSize, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(between(2, 4) * regionSize.getKb()))
            .build();

        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final String blobName = StatelessCompoundCommit.blobNameFromGeneration(randomNonNegativeLong());
            // Create a blob with data span from 2 to 4 regions
            final int numberRegions = between(2, 4);
            final byte[] data = randomByteArrayOfLength(numberRegions * (int) regionSize.getBytes());

            final AtomicInteger objectStoreRequestCount = new AtomicInteger();
            final var objectStoreCacheBlobReader = new ObjectStoreCacheBlobReader(
                TestUtils.singleBlobContainer(blobName, data),
                blobName,
                sharedBlobCacheService.getRangeSize()
            ) {
                @Override
                public InputStream getRangeInputStream(long position, int length) throws IOException {
                    objectStoreRequestCount.incrementAndGet();
                    return super.getRangeInputStream(position, length);
                }
            };

            final ByteSizeValue chunkSize = ByteSizeValue.ofBytes(between(1, 8) * PAGE_SIZE);
            final var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(null, null, null, null, chunkSize) {
                @Override
                public InputStream getRangeInputStream(long position, int length) throws IOException {
                    // verifies that `getRange` does not exceed remaining file length except for padding, implicitly also
                    // verifying that the remainingFileLength calculation in SearchIndexInput is correct too.
                    assertThat(position + length, lessThanOrEqualTo(BlobCacheUtils.toPageAlignedSize(data.length)));
                    return objectStoreCacheBlobReader.getRangeInputStream(position, length);
                }
            };
            final var uploaded = new AtomicBoolean(false);
            final var cacheBlobReader = new SwitchingCacheBlobReader(new ObjectStoreUploadTracker.UploadInfo() {
                @Override
                public boolean isUploaded() {
                    return uploaded.get();
                }

                @Override
                public String preferredNodeId() {
                    return "node";
                }
            }, objectStoreCacheBlobReader, indexingShardCacheBlobReader);

            final long primaryTerm = randomNonNegativeLong();
            // Creating multiple gaps by reading small portion of files
            final int interval = (int) BlobCacheUtils.toPageAlignedSize(data.length / between(5, 10));
            for (int pos = interval; pos < data.length; pos += interval) {
                final String fileName = randomAlphaOfLength(5) + randomFileExtension();
                final int fileLength = (int) randomLongBetween(1, 2048);
                final SearchIndexInput indexInput = new SearchIndexInput(
                    fileName,
                    sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, blobName), pos + fileLength),
                    randomIOContext(),
                    cacheBlobReader,
                    fileLength,
                    pos
                );
                byte[] output = randomReadAndSlice(indexInput, fileLength);
                final byte[] input = Arrays.copyOfRange(data, pos, pos + fileLength);
                assertArrayEquals(input, output);
            }

            // Read all data from blob store to fill all gaps which exercise SharedBlobCacheService#readMultiRegions
            objectStoreRequestCount.set(0);
            uploaded.set(true);
            final SearchIndexInput indexInput = new SearchIndexInput(
                "everything",
                sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, blobName), data.length),
                randomIOContext(),
                cacheBlobReader,
                data.length,
                0
            );
            assertArrayEquals(randomReadAndSlice(indexInput, data.length), data);
            assertThat(objectStoreRequestCount.get(), equalTo(numberRegions));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSequentialRangeMissingHandlerPositionTracking() throws IOException {
        final CacheBlobReader cacheBlobReader = mock(CacheBlobReader.class);
        final SharedBlobCacheService.CacheFile cacheFile = mock(SharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        when(cacheFile.getCacheKey()).thenReturn(new FileCacheKey(shardId, randomNonNegativeLong(), randomIdentifier()));
        final SearchIndexInput indexInput = new SearchIndexInput(
            randomIdentifier(),
            cacheFile,
            randomIOContext(),
            cacheBlobReader,
            randomNonNegativeLong(),
            0
        );

        final long rangeToWriteStart = randomLongBetween(0, 1000);
        final long rangeLength = PAGE_SIZE * between(20, 40);
        final long rangeToWriteEnd = rangeToWriteStart + rangeLength;
        final ByteRange rangeToWrite = ByteRange.of(rangeToWriteStart, rangeToWriteEnd);
        final var sequentialRangeMissingHandler = indexInput.new SequentialRangeMissingHandler(rangeToWrite);

        // Fallback behaviour for a single gap
        {
            long start = randomNonNegativeInt();
            assertThat(
                sequentialRangeMissingHandler.sharedInputStreamFactory(List.of(mockGap(start, start + randomNonNegativeInt()))),
                nullValue()
            );
        }

        // Fill a list of disjoint gaps to ensure underlying input stream has position advanced correctly
        final ArrayList<SparseFileTracker.Gap> gaps = new ArrayList<>();
        for (var start = 0; start < rangeLength; start += 5 * PAGE_SIZE) {
            gaps.add(mockGap(start, Math.min(rangeLength, start + between(1, 5) * PAGE_SIZE)));
        }
        // Add one more gap that is beyond available data length
        final long lastGapStart = rangeLength + randomLongBetween(0, 100);
        gaps.add(mockGap(lastGapStart, lastGapStart + randomLongBetween(1, 100)));

        final byte[] input = randomByteArrayOfLength((int) rangeLength);
        class PosByteArrayInputStream extends ByteArrayInputStream {
            PosByteArrayInputStream(byte[] buf) {
                super(buf);
            }

            public int getPos() {
                return pos;
            }
        }
        final PosByteArrayInputStream inputStream = new PosByteArrayInputStream(input);
        final var totalGapLength = Math.toIntExact(gaps.get(gaps.size() - 1).end() - gaps.get(0).start());
        when(cacheBlobReader.getRangeInputStream(rangeToWriteStart + gaps.get(0).start(), totalGapLength)).thenReturn(inputStream);
        try (var streamFactory = sequentialRangeMissingHandler.sharedInputStreamFactory(gaps)) {
            for (int i = 0; i < gaps.size(); i++) {
                SparseFileTracker.Gap gap = gaps.get(i);
                try (var in = streamFactory.create((int) gap.start())) {
                    if (i == gaps.size() - 1) {
                        // Last gap is beyond available data and should get a nullInputstream with 0 available data
                        assertThat(in.available(), equalTo(0));
                    } else {
                        // Input stream returned by create should have its position set to the new gap start by skipping
                        assertThat(inputStream.getPos(), equalTo((int) gap.start()));
                        final int nbytes = (int) (gap.end() - gap.start());
                        final byte[] output = in.readNBytes(nbytes);
                        assertArrayEquals(Arrays.copyOfRange(input, (int) gap.start(), (int) gap.end()), output);
                    }
                }
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSequentialRangeMissingHandlerWithExceptions() throws IOException {
        final CacheBlobReader cacheBlobReader = mock(CacheBlobReader.class);
        final SharedBlobCacheService.CacheFile cacheFile = mock(SharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        when(cacheFile.getCacheKey()).thenReturn(new FileCacheKey(shardId, randomNonNegativeLong(), randomIdentifier()));
        final SearchIndexInput indexInput = new SearchIndexInput(
            randomIdentifier(),
            cacheFile,
            randomIOContext(),
            cacheBlobReader,
            randomNonNegativeLong(),
            0
        );

        final long rangeToWriteStart = randomLongBetween(0, 1000);
        final long rangeLength = PAGE_SIZE * between(20, 40);
        final long rangeToWriteEnd = rangeToWriteStart + rangeLength;
        final ByteRange rangeToWrite = ByteRange.of(rangeToWriteStart, rangeToWriteEnd);
        final var sequentialRangeMissingHandler = indexInput.new SequentialRangeMissingHandler(rangeToWrite);

        // Create a list of disjoint gaps
        final ArrayList<SparseFileTracker.Gap> gaps = new ArrayList<>();
        for (var start = 0; start < rangeLength; start += 5 * PAGE_SIZE) {
            gaps.add(mockGap(start, Math.min(rangeLength, start + between(1, 5) * PAGE_SIZE)));
        }

        // Go through the gaps to try to fill them unsuccessfully due to the noSuchFileException being thrown
        var noSuchFileException = new NoSuchFileException(cacheFile.getCacheKey().toString());
        when(cacheBlobReader.getRangeInputStream(anyLong(), anyInt())).thenThrow(noSuchFileException);
        try (var streamFactory = sequentialRangeMissingHandler.sharedInputStreamFactory(gaps)) {
            for (int i = 0; i < gaps.size(); i++) {
                SparseFileTracker.Gap gap = gaps.get(i);
                try (var in = streamFactory.create((int) gap.start())) {
                    assert false : "should throw exception";
                } catch (Exception e) {
                    assertThat(e, equalTo(noSuchFileException));
                }
            }
        }
    }

    private SparseFileTracker.Gap mockGap(long start, long end) {
        final SparseFileTracker.Gap gap = mock(SparseFileTracker.Gap.class);
        when(gap.start()).thenReturn(start);
        when(gap.end()).thenReturn(end);
        return gap;
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
        return new IndexingShardCacheBlobReader(null, null, null, null, chunkSize) {
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
        return sharedCacheSettings(cacheSize, pageAligned(new ByteSizeValue(randomIntBetween(4, 1024), ByteSizeUnit.KB)));
    }

    private static Settings sharedCacheSettings(ByteSizeValue cacheSize, ByteSizeValue regionSize) {
        return Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), regionSize)
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .putList(Environment.PATH_DATA_SETTING.getKey(), createTempDir().toAbsolutePath().toString())
            .build();
    }

    private static TestThreadPool getThreadPool(String name) {
        return new TestThreadPool(name, Stateless.statelessExecutorBuilders(Settings.EMPTY, true));
    }
}
