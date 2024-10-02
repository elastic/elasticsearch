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
import co.elastic.elasticsearch.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.CacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.CacheFileReader;
import co.elastic.elasticsearch.stateless.cache.reader.IndexingShardCacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.MutableObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreCacheBlobReader;
import co.elastic.elasticsearch.stateless.cache.reader.ObjectStoreUploadTracker;
import co.elastic.elasticsearch.stateless.cache.reader.SequentialRangeMissingHandler;
import co.elastic.elasticsearch.stateless.cache.reader.SwitchingCacheBlobReader;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.lucene.store.ESIndexInputTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.stateless.TestUtils.newCacheService;
import static co.elastic.elasticsearch.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheFile;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.pageAligned;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.intThat;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BlobCacheIndexInputTests extends ESIndexInputTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = getThreadPool("BlobCacheIndexInputTests");
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
                final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                    fileName,
                    randomIOContext(),
                    new CacheFileReader(
                        sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, fileName), input.length),
                        createBlobReader(fileName, input, sharedBlobCacheService),
                        createBlobFileRanges(primaryTerm, 0L, 0, input.length)
                    ),
                    null,
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

    public void testSwitchToBlobRegionSizeOnUpload() throws IOException {
        final var fileSize = ByteSizeValue.ofKb(64);
        final ByteSizeValue cacheSize = new ByteSizeValue(32, ByteSizeUnit.MB);
        final var settings = sharedCacheSettings(cacheSize);
        final var rangeSize = SHARED_CACHE_RANGE_SIZE_SETTING.get(settings);
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final byte[] input = randomChecksumBytes((int) fileSize.getBytes()).v2();
            final var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonNegativeLong());

            final var blobContainer = new FilterBlobContainer(TestUtils.singleBlobContainer(fileName, input)) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    return child;
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
                    if (blobName.contains(StatelessCompoundCommit.PREFIX)) {
                        assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                    }
                    return super.readBlob(purpose, blobName);
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    if (blobName.contains(StatelessCompoundCommit.PREFIX)) {
                        assert ThreadPool.assertCurrentThreadPool(Stateless.SHARD_READ_THREAD_POOL);
                    }
                    return super.readBlob(purpose, blobName, position, length);
                }
            };

            final ObjectStoreCacheBlobReader objectStoreReader = new ObjectStoreCacheBlobReader(
                blobContainer,
                fileName,
                sharedBlobCacheService.getRangeSize(),
                threadPool.executor(Stateless.SHARD_READ_THREAD_POOL)
            );
            final var indexShardReader = new IndexingShardCacheBlobReader(null, null, null, null, fileSize, threadPool) {
                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    listener.onFailure(new ResourceNotFoundException("VBCC not found"));
                }
            };
            final var tracker = new AtomicMutableObjectStoreUploadTracker();
            final var rangesGot = new AtomicLong(0);
            final var switchingReader = new SwitchingCacheBlobReader(tracker, termAndGen, objectStoreReader, indexShardReader) {
                @Override
                public ByteRange getRange(long position, int length, long remainingFileLength) {
                    final var range = super.getRange(position, length, remainingFileLength);
                    if (rangesGot.getAndIncrement() == 0) {
                        assertFalse(tracker.getLatestUploadInfo(termAndGen).isUploaded());
                        assertThat(range.length(), lessThan(rangeSize.getBytes()));
                    } else {
                        assertTrue(tracker.getLatestUploadInfo(termAndGen).isUploaded());
                        assertThat(range.length(), equalTo(rangeSize.getBytes()));
                    }
                    return range;
                }

                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    // Assert that it's the test thread trying to fetch and not some executor thread.
                    String threadName = Thread.currentThread().getName();
                    assert (threadName.startsWith("TEST-") || threadName.startsWith("LuceneTestCase")) : threadName;
                    super.getRangeInputStream(position, length, listener);
                }
            };
            assertFalse(tracker.getLatestUploadInfo(termAndGen).isUploaded());
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, termAndGen.primaryTerm(), fileName), input.length),
                    switchingReader,
                    createBlobFileRanges(termAndGen.primaryTerm(), termAndGen.generation(), 0, input.length)
                ),
                null,
                input.length,
                0
            );
            byte[] output = new byte[input.length];
            indexInput.readBytes(output, 0, output.length);
            assertArrayEquals(input, output);
            assertTrue(tracker.getLatestUploadInfo(termAndGen).isUploaded());
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
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, fileName), input.length),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length)
                ),
                null,
                input.length,
                0
            );

            indexInput.seek(randomLongBetween(0, input.length - 1));
            BlobCacheIndexInput clone = asInstanceOf(BlobCacheIndexInput.class, indexInput.clone());
            assertThat(getCacheFile(clone), not(equalTo(getCacheFile(indexInput))));
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
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(between(2, 4) * regionSize.getKb()))
            .build();

        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final long primaryTerm = randomNonNegativeLong();
            final long generation = randomNonNegativeLong();
            final String blobName = StatelessCompoundCommit.blobNameFromGeneration(generation);
            // Create a blob with data span from 2 to 4 regions
            final int numberRegions = between(2, 4);
            final byte[] data = randomByteArrayOfLength(numberRegions * (int) regionSize.getBytes());

            final AtomicInteger objectStoreRequestCount = new AtomicInteger();
            final var objectStoreCacheBlobReader = new ObjectStoreCacheBlobReader(
                TestUtils.singleBlobContainer(blobName, data),
                blobName,
                sharedBlobCacheService.getRangeSize(),
                threadPool.executor(Stateless.SHARD_READ_THREAD_POOL)
            ) {
                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    objectStoreRequestCount.incrementAndGet();
                    super.getRangeInputStream(position, length, listener);
                }
            };

            final ByteSizeValue chunkSize = ByteSizeValue.ofBytes(between(1, 8) * PAGE_SIZE);
            final var indexingShardCacheBlobReader = new IndexingShardCacheBlobReader(null, null, null, null, chunkSize, threadPool) {

                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    // verifies that `getRange` does not exceed remaining file length except for padding, implicitly also
                    // verifying that the remainingFileLength calculation in BlobCacheIndexInput is correct too.
                    assertThat(position + length, lessThanOrEqualTo(BlobCacheUtils.toPageAlignedSize(data.length)));
                    objectStoreCacheBlobReader.getRangeInputStream(position, length, listener);
                }
            };
            final var uploaded = new AtomicBoolean(false);
            final var cacheBlobReader = new SwitchingCacheBlobReader(new MutableObjectStoreUploadTracker() {
                @Override
                public void updateLatestUploadedBcc(PrimaryTermAndGeneration latestUploadedBccTermAndGen) {
                    assert false : "should not be called";
                }

                @Override
                public void updateLatestCommitInfo(PrimaryTermAndGeneration ccTermAndGen, String nodeId) {
                    assert false : "should not be called";
                }

                @Override
                public UploadInfo getLatestUploadInfo(PrimaryTermAndGeneration bccTermAndGen) {
                    return new ObjectStoreUploadTracker.UploadInfo() {
                        @Override
                        public boolean isUploaded() {
                            return uploaded.get();
                        }

                        @Override
                        public String preferredNodeId() {
                            return "node";
                        }
                    };
                }
            }, new PrimaryTermAndGeneration(primaryTerm, generation), objectStoreCacheBlobReader, indexingShardCacheBlobReader);

            // Creating multiple gaps by reading small portion of files
            final int interval = (int) BlobCacheUtils.toPageAlignedSize(data.length / between(5, 10));
            for (int pos = interval; pos < data.length; pos += interval) {
                final String fileName = randomAlphaOfLength(5) + randomFileExtension();
                final int fileLength = (int) randomLongBetween(1, 2048);
                final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                    fileName,
                    randomIOContext(),
                    new CacheFileReader(
                        sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, blobName), pos + fileLength),
                        cacheBlobReader,
                        createBlobFileRanges(primaryTerm, generation, pos, fileLength)
                    ),
                    null,
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
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                "everything",
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(new FileCacheKey(shardId, primaryTerm, blobName), data.length),
                    cacheBlobReader,
                    createBlobFileRanges(primaryTerm, generation, 0, data.length)
                ),
                null,
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

        final long rangeToWriteStart = randomLongBetween(0, 1000);
        final long rangeLength = PAGE_SIZE * between(20, 40);
        final long rangeToWriteEnd = rangeToWriteStart + rangeLength;
        final ByteRange rangeToWrite = ByteRange.of(rangeToWriteStart, rangeToWriteEnd);
        final var sequentialRangeMissingHandler = new SequentialRangeMissingHandler(
            "__test__",
            "__unknown__",
            rangeToWrite,
            cacheBlobReader,
            () -> null, // ignored
            copiedBytes -> {},
            Stateless.SHARD_READ_THREAD_POOL,
            Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        );

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
        final long firstGapStart = randomLongBetween(0, 10);
        for (var start = firstGapStart; start < rangeLength; start += 5 * PAGE_SIZE) {
            gaps.add(mockGap(start, Math.min(rangeLength, start + between(1, 5) * PAGE_SIZE)));
        }
        // Add one more gap that is beyond available data length
        final long lastGapStart = rangeLength + randomLongBetween(0, 100);
        gaps.add(mockGap(lastGapStart, lastGapStart + randomLongBetween(1, 100)));

        final byte[] input = randomByteArrayOfLength((int) (rangeLength - firstGapStart));
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
        final var calledOnce = ActionListener.assertOnce(ActionListener.noop());
        final var executor = randomBoolean()
            ? threadPool.executor(Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL)
            : EsExecutors.DIRECT_EXECUTOR_SERVICE;
        doAnswer(invocation -> {
            ActionListener<InputStream> argument = invocation.getArgument(2);
            executor.submit(ActionRunnable.run(calledOnce, () -> {
                // Make sure the input stream called only once and is returned with a delay
                safeSleep(randomIntBetween(0, 100));
                argument.onResponse(inputStream);
            }));
            return null;
        }).when(cacheBlobReader)
            .getRangeInputStream(
                longThat(l -> l.equals(rangeToWriteStart + firstGapStart)),
                intThat(i -> i.equals(totalGapLength)),
                any(ActionListener.class)
            );
        try (var streamFactory = sequentialRangeMissingHandler.sharedInputStreamFactory(gaps)) {
            CountDownLatch latch = new CountDownLatch(gaps.size());
            AtomicInteger expectedGapId = new AtomicInteger(0);
            for (int i = 0; i < gaps.size(); i++) {
                SparseFileTracker.Gap gap = gaps.get(i);
                int finalI = i;
                streamFactory.create((int) gap.start(), ActionListener.runAfter(ActionListener.wrap(in -> {
                    try (in) {
                        assertThat(finalI, equalTo(expectedGapId.getAndIncrement()));
                        if (finalI == gaps.size() - 1) {
                            // Last gap is beyond available data and should get a nullInputstream with 0 available data
                            assertThat(in.available(), equalTo(0));
                        } else {
                            // Input stream returned by create should have its position set to the new gap start by skipping
                            assertThat(inputStream.getPos(), equalTo((int) (gap.start() - firstGapStart)));
                            final int nbytes = (int) (gap.end() - gap.start());
                            final byte[] output = in.readNBytes(nbytes);
                            assertArrayEquals(
                                Arrays.copyOfRange(input, (int) (gap.start() - firstGapStart), (int) (gap.end() - firstGapStart)),
                                output
                            );
                        }
                    }
                }, e -> { assert false : e; }), () -> latch.countDown()));
            }
            safeAwait(latch);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testSequentialRangeMissingHandlerWithExceptions() throws IOException {
        final CacheBlobReader cacheBlobReader = mock(CacheBlobReader.class);
        final SharedBlobCacheService.CacheFile cacheFile = mock(SharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
        when(cacheFile.getCacheKey()).thenReturn(new FileCacheKey(shardId, randomNonNegativeLong(), randomIdentifier()));

        final long rangeToWriteStart = randomLongBetween(0, 1000);
        final long rangeLength = PAGE_SIZE * between(20, 40);
        final long rangeToWriteEnd = rangeToWriteStart + rangeLength;
        final ByteRange rangeToWrite = ByteRange.of(rangeToWriteStart, rangeToWriteEnd);
        final var sequentialRangeMissingHandler = new SequentialRangeMissingHandler(
            "__test__",
            "__unknown__",
            rangeToWrite,
            cacheBlobReader,
            () -> null, // ignored
            copiedBytes -> {},
            Stateless.SHARD_READ_THREAD_POOL,
            Stateless.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
        );

        // Create a list of disjoint gaps
        final ArrayList<SparseFileTracker.Gap> gaps = new ArrayList<>();
        for (var start = 0; start < rangeLength; start += 5 * PAGE_SIZE) {
            gaps.add(mockGap(start, Math.min(rangeLength, start + between(1, 5) * PAGE_SIZE)));
        }

        // Go through the gaps to try to fill them unsuccessfully due to the noSuchFileException being thrown
        var noSuchFileException = new NoSuchFileException(cacheFile.getCacheKey().toString());
        doAnswer(invocation -> { throw noSuchFileException; }).when(cacheBlobReader)
            .getRangeInputStream(anyLong(), anyInt(), any(ActionListener.class));
        CountDownLatch exceptionSeen = new CountDownLatch(1);
        try (var streamFactory = sequentialRangeMissingHandler.sharedInputStreamFactory(gaps)) {
            try (var refCountingListener = new RefCountingListener(new ActionListener<>() {
                @Override
                public void onResponse(Void ignored) {
                    assert false : "should have failed";
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(ExceptionsHelper.unwrap(e, NoSuchFileException.class), equalTo(noSuchFileException));
                    exceptionSeen.countDown();
                }
            })) {
                for (int i = 0; i < gaps.size(); i++) {
                    SparseFileTracker.Gap gap = gaps.get(i);
                    streamFactory.create((int) gap.start(), refCountingListener.acquire().map(ignored -> null));
                }
            }
        }
        safeAwait(exceptionSeen);
    }

    private SparseFileTracker.Gap mockGap(long start, long end) {
        final SparseFileTracker.Gap gap = mock(SparseFileTracker.Gap.class);
        when(gap.start()).thenReturn(start);
        when(gap.end()).thenReturn(end);
        return gap;
    }

    private CacheBlobReader createBlobReader(String fileName, byte[] input, StatelessSharedBlobCacheService sharedBlobCacheService) {

        ObjectStoreCacheBlobReader objectStore = new ObjectStoreCacheBlobReader(
            TestUtils.singleBlobContainer(fileName, input),
            fileName,
            sharedBlobCacheService.getRangeSize(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        if (randomBoolean()) {
            return objectStore;
        }
        ByteSizeValue chunkSize = ByteSizeValue.ofKb(8);
        return new IndexingShardCacheBlobReader(null, null, null, null, chunkSize, threadPool) {

            @Override
            public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                // verifies that `getRange` does not exceed remaining file length except for padding, implicitly also
                // verifying that the remainingFileLength calculation in BlobCacheIndexInput is correct too.
                assertThat(position + length, lessThanOrEqualTo(BlobCacheUtils.toPageAlignedSize(input.length)));
                objectStore.getRangeInputStream(position, length, listener);
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
