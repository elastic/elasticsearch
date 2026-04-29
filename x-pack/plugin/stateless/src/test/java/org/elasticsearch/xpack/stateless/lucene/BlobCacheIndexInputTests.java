/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.lucene;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyUploadedException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
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
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.cache.reader.AtomicMutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.CacheFileReader;
import org.elasticsearch.xpack.stateless.cache.reader.IndexingShardCacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.MutableObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.ObjectStoreCacheBlobReader;
import org.elasticsearch.xpack.stateless.cache.reader.ObjectStoreUploadTracker;
import org.elasticsearch.xpack.stateless.cache.reader.SequentialRangeMissingHandler;
import org.elasticsearch.xpack.stateless.cache.reader.SwitchingCacheBlobReader;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

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
import java.util.function.Supplier;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomChecksumBytes;
import static org.elasticsearch.xpack.searchablesnapshots.AbstractSearchableSnapshotsTestCase.randomIOContext;
import static org.elasticsearch.xpack.searchablesnapshots.cache.common.TestUtils.pageAligned;
import static org.elasticsearch.xpack.stateless.TestUtils.newCacheService;
import static org.elasticsearch.xpack.stateless.commits.BlobLocationTestUtils.createBlobFileRanges;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheFile;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.intThat;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
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
                        sharedBlobCacheService.getCacheFile(
                            new FileCacheKey(shardId, primaryTerm, fileName),
                            input.length,
                            SharedBlobCacheService.CacheMissHandler.NOOP
                        ),
                        createBlobReader(fileName, input, sharedBlobCacheService),
                        createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                        BlobCacheMetrics.NOOP,
                        System::currentTimeMillis
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
        final ByteSizeValue cacheSize = ByteSizeValue.of(32, ByteSizeUnit.MB);
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
                        assert ThreadPool.assertCurrentThreadPool(StatelessPlugin.SHARD_READ_THREAD_POOL);
                    }
                    return super.readBlob(purpose, blobName);
                }

                @Override
                public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
                    if (blobName.contains(StatelessCompoundCommit.PREFIX)) {
                        assert ThreadPool.assertCurrentThreadPool(StatelessPlugin.SHARD_READ_THREAD_POOL);
                    }
                    return super.readBlob(purpose, blobName, position, length);
                }
            };

            final ObjectStoreCacheBlobReader objectStoreReader = new ObjectStoreCacheBlobReader(
                blobContainer,
                fileName,
                sharedBlobCacheService.getRangeSize(),
                threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL)
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
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, termAndGen.primaryTerm(), fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    switchingReader,
                    createBlobFileRanges(termAndGen.primaryTerm(), termAndGen.generation(), 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
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

    // Regression test for #5890
    public void testAlreadyUploadedHandling() throws Exception {
        // How this reproduces #5890:
        // 1. schedule a read, which should try to get a cache region, fail, and then call getInputStream without
        // the cache region. It will go to the index shard because it doesn't know the object is uploaded,
        // but should block before it marks the object as uploaded
        // 2. schedule other reads, which should succeed at acquiring a cache region, and create gaps to fill.
        // Block them before they mark the object as uploaded.
        // 3. unblock the initial read. It will fail on the shard because it has been uploaded, then retry with
        // the object store listener and subscribe to the gaps.
        // 4. Once it has subscribed, it unblocks the other reads.
        // 5. The other reads have already decided to contact the index shard, so they will fail with already uploaded,
        // which will fail the first read's subscribed listener a second time.

        final int cachingReaders = randomIntBetween(1, 5);
        logger.info("running with {} caching readers", cachingReaders);
        final var fileSize = ByteSizeValue.ofKb(64);
        final var indexReaderChunkSize = ByteSizeValue.ofKb(4);
        final ByteSizeValue cacheSize = ByteSizeValue.of(32, ByteSizeUnit.MB);
        // The region size must exceed the total bytes read by all cacheable readers (cachingReaders * 4KB) so that
        // the uncacheable reader's retry can claim a new gap beyond the cacheable reads' range. This new gap gets
        // filled from the object store, which triggers objectStoreReadArrived and unblocks the cacheable threads.
        final var settings = sharedCacheSettings(
            cacheSize,
            pageAligned(ByteSizeValue.of(randomIntBetween(4 * (cachingReaders + 1), 1024), ByteSizeUnit.KB))
        );
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index(randomIndexName(), randomUUID()), 0);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final byte[] input = randomChecksumBytes((int) fileSize.getBytes()).v2();
            final var termAndGen = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonNegativeLong());
            final var tracker = new AtomicMutableObjectStoreUploadTracker();
            final var blobContainer = TestUtils.singleBlobContainer(fileName, input);

            // The first read will fail to claim a cache slot, then block until the other reads have claimed gaps
            final var uncacheableReadArrived = new CountDownLatch(1);
            final var uncacheableReadReleased = new CountDownLatch(cachingReaders);
            // the others read will claim cache slots and then unblock the first read, then wait for the first read to
            // have failed its first (unsubscribed) read before proceeding to have their own reads fail
            final var objectStoreReadArrived = new CountDownLatch(1);
            final var objectStoreReadReleased = new CountDownLatch(1);

            final var cacheFile = spy(
                sharedBlobCacheService.getCacheFile(
                    new FileCacheKey(shardId, termAndGen.generation(), fileName),
                    input.length,
                    SharedBlobCacheService.CacheMissHandler.NOOP
                )
            );
            // simulate eviction on only the first attempt to claim the cache
            doThrow(new AlreadyClosedException("evicted")).doCallRealMethod()
                .when(cacheFile)
                .populateAndRead(any(), any(), any(), any(), anyString());

            final ObjectStoreCacheBlobReader objectStoreReader = new ObjectStoreCacheBlobReader(
                blobContainer,
                fileName,
                sharedBlobCacheService.getRangeSize(),
                threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL)
            ) {
                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    logger.info("reading {}@{}+{} from object store", fileName, position, length);
                    objectStoreReadArrived.countDown();
                    safeAwait(objectStoreReadReleased);
                    super.getRangeInputStream(position, length, listener);
                }
            };
            final var numReads = new AtomicInteger();
            final var indexShardReader = new IndexingShardCacheBlobReader(null, null, null, null, indexReaderChunkSize, threadPool) {
                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    final var reads = numReads.getAndIncrement();
                    logger.info("read {}: reading {}@{}+{} from index shard", reads, fileName, position, length);
                    if (reads == 0) {
                        logger.debug("blocking uncacheable read");
                        uncacheableReadArrived.countDown();
                        // should not be released until the second reader has started its object store read
                        safeAwait(uncacheableReadReleased);
                        logger.debug("uncacheable read released");
                    } else {
                        logger.debug("counting down uncacheable read");
                        uncacheableReadReleased.countDown();
                        safeAwait(objectStoreReadArrived);
                        objectStoreReadReleased.countDown();
                    }
                    listener.onFailure(new ResourceAlreadyUploadedException("VBCC already uploaded: " + position + "+" + length));
                }
            };
            final var switchingReader = new SwitchingCacheBlobReader(tracker, termAndGen, objectStoreReader, indexShardReader);

            final Supplier<BlobCacheIndexInput> indexInputSupplier = () -> new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    cacheFile,
                    switchingReader,
                    createBlobFileRanges(termAndGen.primaryTerm(), termAndGen.generation(), 0, input.length),
                    null,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            final var threads = new Thread[cachingReaders + 1];

            // store uncaching reader at the end to make filling the array more natural
            threads[cachingReaders] = new Thread(null, () -> {
                final var totalSize = 4096 * cachingReaders;
                try (var indexInput = indexInputSupplier.get()) {
                    indexInput.readBytes(new byte[totalSize], 0, totalSize, false);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }, "THREAD-uncacheable");
            threads[cachingReaders].start();

            safeAwait(uncacheableReadArrived);

            for (int i = 0; i < cachingReaders; i++) {
                final var offset = i * 4096L;
                final var reader = new Thread(null, () -> {
                    try (var indexInput = indexInputSupplier.get()) {
                        indexInput.seek(offset);
                        indexInput.readBytes(new byte[4096], 0, 4096, false);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, "THREAD-cacheable-" + i);
                reader.start();
                threads[i] = reader;
            }

            for (int i = 0; i < cachingReaders + 1; i++) {
                safeJoin(threads[i]);
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
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
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
                threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL)
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
                        sharedBlobCacheService.getCacheFile(
                            new FileCacheKey(shardId, primaryTerm, blobName),
                            pos + fileLength,
                            SharedBlobCacheService.CacheMissHandler.NOOP
                        ),
                        cacheBlobReader,
                        createBlobFileRanges(primaryTerm, generation, pos, fileLength),
                        BlobCacheMetrics.NOOP,
                        System::currentTimeMillis
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
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, blobName),
                        data.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    cacheBlobReader,
                    createBlobFileRanges(primaryTerm, generation, 0, data.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
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
            StatelessPlugin.SHARD_READ_THREAD_POOL,
            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
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
            ? threadPool.executor(StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL)
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
            StatelessPlugin.SHARD_READ_THREAD_POOL,
            StatelessPlugin.FILL_VIRTUAL_BATCHED_COMPOUND_COMMIT_CACHE_THREAD_POOL
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

    public void testSlicing() throws IOException {
        final var settings = sharedCacheSettings(ByteSizeValue.ofBytes(randomLongBetween(0, 10_000_000)));
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            // More than one position to allow slicing
            final byte[] input = randomByteArrayOfLength(randomIntBetween(2, 100_000));
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            assertNull(indexInput.getSliceDescription());

            long pos = randomLongBetween(0, input.length - 1);
            IndexInput slice = indexInput.slice("fake", 0, pos);
            BlobCacheIndexInput blobCacheIndexInputSlice = asInstanceOf(BlobCacheIndexInput.class, slice);
            assertThat(getCacheFile(blobCacheIndexInputSlice), not(equalTo(getCacheFile(indexInput))));
            assertThat(blobCacheIndexInputSlice.getFilePointer(), equalTo(indexInput.getFilePointer()));
            assertEquals("fake", blobCacheIndexInputSlice.getSliceDescription());

            long secondPos = randomLongBetween(0, input.length - 1);
            IndexInput secondSlice = indexInput.slice("fake.nmv", 0, secondPos);
            BlobCacheIndexInput secondBlobCacheIndexInputSlice = asInstanceOf(BlobCacheIndexInput.class, secondSlice);
            assertThat(getCacheFile(secondBlobCacheIndexInputSlice), not(equalTo(getCacheFile(indexInput))));
            assertThat(secondBlobCacheIndexInputSlice.getFilePointer(), equalTo(indexInput.getFilePointer()));
            assertEquals("fake.nmv", secondBlobCacheIndexInputSlice.getSliceDescription());

            RandomAccessInput randomAccessSlice = indexInput.randomAccessSlice(0, pos);
            BlobCacheIndexInput randomAccessBlobCacheIndexInputSlice = asInstanceOf(BlobCacheIndexInput.class, randomAccessSlice);
            assertThat(getCacheFile(randomAccessBlobCacheIndexInputSlice), not(equalTo(getCacheFile(indexInput))));
            assertThat(randomAccessBlobCacheIndexInputSlice.getFilePointer(), equalTo(indexInput.getFilePointer()));
            assertEquals("randomaccess", randomAccessBlobCacheIndexInputSlice.getSliceDescription());

            RandomAccessInput randomAccessSliceSlice = secondSlice.randomAccessSlice(0, secondPos);
            BlobCacheIndexInput randomAccessSliceBlobCacheIndexInputSlice = asInstanceOf(BlobCacheIndexInput.class, randomAccessSliceSlice);
            assertThat(getCacheFile(randomAccessSliceBlobCacheIndexInputSlice), not(equalTo(getCacheFile(indexInput))));
            assertThat(randomAccessSliceBlobCacheIndexInputSlice.getFilePointer(), equalTo(indexInput.getFilePointer()));
            assertEquals("randomaccess", randomAccessSliceBlobCacheIndexInputSlice.getSliceDescription());
        }
    }

    // Verifies withByteBufferSlice returns correct data for full and sub-range reads.
    // Uses mmap-backed cache with 10 regions (4-64 KB each); file fits within a single region.
    public void testWithByteBufferSlice() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 64)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 10);
        final var settings = Settings.builder()
            .put(sharedCacheSettings(cacheSize, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            // Keep file small enough to fit in one region
            final int fileLength = randomIntBetween(1, (int) regionSize.getBytes() / 2);
            final byte[] input = randomByteArrayOfLength(fileLength);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            // Read all data to populate the cache
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);

            // Now verify withByteBufferSlice provides the correct data
            boolean available = indexInput.withByteBufferSlice(0, input.length, slice -> {
                assertTrue(slice.isReadOnly());
                assertEquals(input.length, slice.remaining());
                byte[] sliceBytes = new byte[input.length];
                slice.get(sliceBytes);
                assertArrayEquals(input, sliceBytes);
            });
            assertTrue("withByteBufferSlice(0, " + input.length + ") returned false; regionSize=" + regionSize, available);

            // Verify a sub-range works too
            if (input.length > 10) {
                int subOffset = randomIntBetween(1, input.length / 2);
                int subLength = randomIntBetween(1, input.length - subOffset);
                boolean subAvailable = indexInput.withByteBufferSlice(subOffset, subLength, slice -> {
                    byte[] subBytes = new byte[subLength];
                    slice.get(subBytes);
                    assertArrayEquals(Arrays.copyOfRange(input, subOffset, subOffset + subLength), subBytes);
                });
                assertTrue(subAvailable);
            }
        }
    }

    // Verifies withByteBufferSlice on a sliced BlobCacheIndexInput correctly translates the
    // slice offset. Uses doSlice to bypass the buffer-based fast path that returns ByteArrayIndexInput.
    public void testWithByteBufferSliceOnSlice() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 64)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 10);
        final var settings = Settings.builder()
            .put(sharedCacheSettings(cacheSize, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            // Keep file small enough to fit in one region
            final int fileLength = randomIntBetween(100, (int) regionSize.getBytes() / 2);
            final byte[] input = randomByteArrayOfLength(fileLength);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            // Read all data to populate the cache
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);

            // Use doSlice to bypass the buffer-based fast path (trySliceBuffer) that may return a ByteArrayIndexInput
            int sliceOffset = randomIntBetween(1, input.length / 2);
            int sliceLength = randomIntBetween(1, input.length - sliceOffset);
            BlobCacheIndexInput blobSlice = asInstanceOf(
                BlobCacheIndexInput.class,
                indexInput.doSlice("test-slice", sliceOffset, sliceLength)
            );

            // withByteBufferSlice(0, sliceLength) on the slice should return data starting at sliceOffset
            boolean available = blobSlice.withByteBufferSlice(0, sliceLength, slice -> {
                byte[] sliceBytes = new byte[sliceLength];
                slice.get(sliceBytes);
                assertArrayEquals(Arrays.copyOfRange(input, sliceOffset, sliceOffset + sliceLength), sliceBytes);
            });
            assertTrue(available);
        }
    }

    // Verifies withByteBufferSlice returns false when the cache is not mmap-backed,
    // since no direct byte buffer view is available. Uses 10 regions (4-64 KB each) without mmap.
    public void testWithByteBufferSliceNoMmapReturnsFalse() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 64)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 10);
        final var settings = sharedCacheSettings(cacheSize, regionSize);
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final int fileLength = randomIntBetween(1, (int) regionSize.getBytes() / 2);
            final byte[] input = randomByteArrayOfLength(fileLength);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            // Read all data to populate the cache
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);

            // withByteBufferSlice should return false when mmap is not enabled
            boolean available = indexInput.withByteBufferSlice(0, input.length, slice -> {
                fail("action should not be invoked when mmap is not enabled");
            });
            assertFalse(available);
        }
    }

    // Verifies withByteBufferSlice returns false after the backing region has been evicted.
    // Uses a small mmap-backed cache with only 3 regions (4-16 KB each); populating 4 additional
    // files forces eviction of file A's region, after which withByteBufferSlice must return false.
    public void testWithByteBufferSliceReturnsFalseAfterEviction() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 16)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 3);
        final var settings = Settings.builder()
            .put(sharedCacheSettings(cacheSize, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final long primaryTerm = randomNonNegativeLong();

            // File A: fits in one region
            final int fileLengthA = randomIntBetween(1, (int) regionSize.getBytes() / 2);
            final byte[] inputA = randomByteArrayOfLength(fileLengthA);
            final String fileNameA = "fileA_" + randomAlphaOfLength(5);
            final BlobCacheIndexInput indexInputA = new BlobCacheIndexInput(
                fileNameA,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileNameA),
                        inputA.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileNameA, inputA, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, inputA.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                inputA.length,
                0
            );

            // Populate cache with file A
            byte[] outputA = randomReadAndSlice(indexInputA, inputA.length);
            assertArrayEquals(inputA, outputA);

            // Verify buffer is available before eviction
            boolean availableBefore = indexInputA.withByteBufferSlice(0, inputA.length, slice -> {
                byte[] sliceBytes = new byte[inputA.length];
                slice.get(sliceBytes);
                assertArrayEquals(inputA, sliceBytes);
            });
            assertTrue("expected buffer to be available before eviction", availableBefore);

            // Fill the cache with other files to evict file A's region
            for (int i = 0; i < 4; i++) {
                int evictFileLength = (int) regionSize.getBytes() / 2;
                byte[] evictInput = randomByteArrayOfLength(evictFileLength);
                String evictFileName = "evict_" + i + "_" + randomAlphaOfLength(5);
                BlobCacheIndexInput evictIndexInput = new BlobCacheIndexInput(
                    evictFileName,
                    randomIOContext(),
                    new CacheFileReader(
                        sharedBlobCacheService.getCacheFile(
                            new FileCacheKey(shardId, primaryTerm, evictFileName),
                            evictInput.length,
                            SharedBlobCacheService.CacheMissHandler.NOOP
                        ),
                        createBlobReader(evictFileName, evictInput, sharedBlobCacheService),
                        createBlobFileRanges(primaryTerm, 0L, 0, evictInput.length),
                        BlobCacheMetrics.NOOP,
                        System::currentTimeMillis
                    ),
                    null,
                    evictInput.length,
                    0
                );
                byte[] evictOutput = randomReadAndSlice(evictIndexInput, evictInput.length);
                assertArrayEquals(evictInput, evictOutput);
            }

            // After eviction, withByteBufferSlice should return false for file A
            boolean availableAfter = indexInputA.withByteBufferSlice(
                0,
                inputA.length,
                slice -> { fail("action should not be invoked after eviction"); }
            );
            assertFalse("expected buffer to be unavailable after eviction", availableAfter);
        }
    }

    public void testBypassReadMetrics() throws Exception {
        final var recordingMeterRegistry = new RecordingMeterRegistry();
        final var regionSize = SharedBytes.PAGE_SIZE * between(1, 10);
        final var settings = sharedCacheSettings(ByteSizeValue.ofBytes(regionSize * 10L), ByteSizeValue.ofBytes(regionSize));
        try (
            var nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = newCacheService(nodeEnvironment, settings, threadPool, recordingMeterRegistry)
        ) {
            final var shardId = new ShardId(new Index(randomIndexName(), randomUUID()), 0);
            final var fileName = randomIdentifier() + randomFileExtension();
            // ensure it fits into a single region to get predictable metric counts
            final var input = randomByteArrayOfLength(between(1, regionSize));
            final long primaryTerm = randomNonNegativeLong();
            final var metrics = cacheService.getBlobCacheMetrics();

            // Spy on CacheFile: first call throws for bypass, rest calls real method
            final var cacheFile = spy(
                cacheService.getCacheFile(
                    new FileCacheKey(shardId, primaryTerm, fileName),
                    input.length,
                    SharedBlobCacheService.CacheMissHandler.NOOP
                )
            );
            doThrow(new AlreadyClosedException("evicted")).doCallRealMethod()
                .when(cacheFile)
                .populateAndRead(any(), any(), any(), any(), any());

            final var cacheFileReader = new CacheFileReader(
                cacheFile,
                createBlobReader(fileName, input, cacheService),
                createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                metrics,
                System::currentTimeMillis
            );

            // First read: bypass path — exactly 1 bypass, 1 read, 1 miss
            assertBypassReadMetrics(fileName, cacheFileReader, input, recordingMeterRegistry, metrics, 1, 1, 1);

            // Second read: normal path — 1 read, 1 miss (region needs filling since bypass didn't cache), no bypass
            assertBypassReadMetrics(fileName, cacheFileReader, input, recordingMeterRegistry, metrics, 1, 1, 0);

            // Third read: cache hit — 1 read (from tryRead), no miss, no bypass
            assertBypassReadMetrics(fileName, cacheFileReader, input, recordingMeterRegistry, metrics, 1, 0, 0);
        }
    }

    private void assertBypassReadMetrics(
        String fileName,
        CacheFileReader cacheFileReader,
        byte[] expectedData,
        RecordingMeterRegistry recordingMeterRegistry,
        BlobCacheMetrics metrics,
        long expectedReads,
        long expectedMisses,
        long expectedBypasses
    ) throws IOException {
        recordingMeterRegistry.getRecorder().resetCalls();
        final long initialReadCount = metrics.readCount();
        final long initialMissCount = metrics.missCount();

        // Create a fresh BlobCacheIndexInput each time to avoid internal buffer state from previous reads
        final var indexInput = new BlobCacheIndexInput(fileName, randomIOContext(), cacheFileReader, null, expectedData.length, 0);
        byte[] output = new byte[expectedData.length];
        indexInput.readBytes(output, 0, expectedData.length);
        assertArrayEquals(expectedData, output);

        assertThat(metrics.readCount(), equalTo(initialReadCount + expectedReads));
        assertThat(metrics.missCount(), equalTo(initialMissCount + expectedMisses));
        assertThat(
            recordingMeterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_COUNTER, BlobCacheMetrics.BLOB_CACHE_BYPASS_READ_TOTAL)
                .stream()
                .mapToLong(Measurement::getLong)
                .sum(),
            equalTo(expectedBypasses)
        );
    }

    // Verifies withByteBufferSlices returns correct data for multiple ranges within a single region.
    // Uses mmap-backed cache with 10 regions; file fits within a single region.
    public void testWithByteBufferSlices() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 64)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 10);
        final var settings = Settings.builder()
            .put(sharedCacheSettings(cacheSize, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final int fileLength = randomIntBetween(200, (int) regionSize.getBytes() / 2);
            final byte[] input = randomByteArrayOfLength(fileLength);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            // Populate the cache
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);

            // Multiple slices within the same region
            int sliceLen = randomIntBetween(1, fileLength / 4);
            long[] offsets = new long[3];
            offsets[0] = 0;
            offsets[1] = randomIntBetween(1, fileLength / 2 - sliceLen);
            offsets[2] = fileLength - sliceLen;
            boolean available = indexInput.withByteBufferSlices(offsets, sliceLen, 3, slices -> {
                assertEquals(3, slices.length);
                for (int i = 0; i < 3; i++) {
                    assertNotNull(slices[i]);
                    assertTrue(slices[i].isReadOnly());
                    assertEquals(sliceLen, slices[i].remaining());
                    byte[] sliceBytes = new byte[sliceLen];
                    slices[i].get(sliceBytes);
                    byte[] expected = Arrays.copyOfRange(input, (int) offsets[i], (int) offsets[i] + sliceLen);
                    assertArrayEquals("mismatch at offset " + offsets[i], expected, sliceBytes);
                }
            });
            assertTrue("withByteBufferSlices returned false; regionSize=" + regionSize, available);
        }
    }

    // Verifies withByteBufferSlices on a sliced BlobCacheIndexInput correctly translates offsets
    // by adding this.offset. Uses doSlice to bypass the buffer-based fast path.
    public void testWithByteBufferSlicesOnSlice() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 64)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 10);
        final var settings = Settings.builder()
            .put(sharedCacheSettings(cacheSize, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final int fileLength = randomIntBetween(200, (int) regionSize.getBytes() / 2);
            final byte[] input = randomByteArrayOfLength(fileLength);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            // Populate the cache
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);

            // Use doSlice to bypass trySliceBuffer
            int sliceOffset = randomIntBetween(1, fileLength / 3);
            int sliceLength = fileLength - sliceOffset - randomIntBetween(0, fileLength / 3);
            BlobCacheIndexInput blobSlice = asInstanceOf(
                BlobCacheIndexInput.class,
                indexInput.doSlice("test-slice", sliceOffset, sliceLength)
            );

            // Offsets are relative to the slice; the implementation must add this.offset
            int rangeLen = randomIntBetween(1, sliceLength / 3);
            long[] offsets = { 0, randomIntBetween(1, sliceLength - rangeLen) };
            boolean available = blobSlice.withByteBufferSlices(offsets, rangeLen, 2, slices -> {
                for (int i = 0; i < 2; i++) {
                    byte[] sliceBytes = new byte[rangeLen];
                    slices[i].get(sliceBytes);
                    byte[] expected = Arrays.copyOfRange(input, sliceOffset + (int) offsets[i], sliceOffset + (int) offsets[i] + rangeLen);
                    assertArrayEquals("mismatch at slice-relative offset " + offsets[i], expected, sliceBytes);
                }
            });
            assertTrue(available);
        }
    }

    // Verifies withByteBufferSlices returns false when the cache is not mmap-backed.
    public void testWithByteBufferSlicesNoMmapReturnsFalse() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 64)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 10);
        final var settings = sharedCacheSettings(cacheSize, regionSize);
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final int fileLength = randomIntBetween(200, (int) regionSize.getBytes() / 2);
            final byte[] input = randomByteArrayOfLength(fileLength);
            final String fileName = randomAlphaOfLength(5) + randomFileExtension();
            final long primaryTerm = randomNonNegativeLong();
            final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
                fileName,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileName),
                        input.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileName, input, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, input.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                input.length,
                0
            );

            // Populate the cache
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);

            int sliceLen = randomIntBetween(1, fileLength / 4);
            long[] offsets = { 0, randomIntBetween(1, fileLength - sliceLen) };
            boolean available = indexInput.withByteBufferSlices(offsets, sliceLen, 2, slices -> {
                fail("action should not be invoked when mmap is not enabled");
            });
            assertFalse(available);
        }
    }

    // Verifies withByteBufferSlices returns false after the backing region has been evicted.
    // Uses a small mmap-backed cache with only 3 regions; populating additional files forces
    // eviction of file A's region.
    public void testWithByteBufferSlicesReturnsFalseAfterEviction() throws IOException {
        final ByteSizeValue regionSize = pageAligned(ByteSizeValue.ofKb(randomIntBetween(4, 16)));
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(regionSize.getBytes() * 3);
        final var settings = Settings.builder()
            .put(sharedCacheSettings(cacheSize, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_MMAP.getKey(), true)
            .build();
        try (
            NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService sharedBlobCacheService = newCacheService(nodeEnvironment, settings, threadPool)
        ) {
            final ShardId shardId = new ShardId(new Index("_index_name", "_index_id"), 0);
            final long primaryTerm = randomNonNegativeLong();

            // File A: fits in one region
            final int fileLengthA = randomIntBetween(200, (int) regionSize.getBytes() / 2);
            final byte[] inputA = randomByteArrayOfLength(fileLengthA);
            final String fileNameA = "fileA_" + randomAlphaOfLength(5);
            final BlobCacheIndexInput indexInputA = new BlobCacheIndexInput(
                fileNameA,
                randomIOContext(),
                new CacheFileReader(
                    sharedBlobCacheService.getCacheFile(
                        new FileCacheKey(shardId, primaryTerm, fileNameA),
                        inputA.length,
                        SharedBlobCacheService.CacheMissHandler.NOOP
                    ),
                    createBlobReader(fileNameA, inputA, sharedBlobCacheService),
                    createBlobFileRanges(primaryTerm, 0L, 0, inputA.length),
                    BlobCacheMetrics.NOOP,
                    System::currentTimeMillis
                ),
                null,
                inputA.length,
                0
            );

            // Populate cache with file A
            byte[] outputA = randomReadAndSlice(indexInputA, inputA.length);
            assertArrayEquals(inputA, outputA);

            // Verify bulk access is available before eviction
            int sliceLen = randomIntBetween(1, fileLengthA / 4);
            long[] offsets = { 0, randomIntBetween(1, fileLengthA - sliceLen) };
            boolean availableBefore = indexInputA.withByteBufferSlices(offsets, sliceLen, 2, slices -> {
                for (int i = 0; i < 2; i++) {
                    byte[] sliceBytes = new byte[sliceLen];
                    slices[i].get(sliceBytes);
                    assertArrayEquals(Arrays.copyOfRange(inputA, (int) offsets[i], (int) offsets[i] + sliceLen), sliceBytes);
                }
            });
            assertTrue("expected buffers to be available before eviction", availableBefore);

            // Fill the cache with other files to evict file A's region
            for (int i = 0; i < 4; i++) {
                int evictFileLength = (int) regionSize.getBytes() / 2;
                byte[] evictInput = randomByteArrayOfLength(evictFileLength);
                String evictFileName = "evict_" + i + "_" + randomAlphaOfLength(5);
                BlobCacheIndexInput evictIndexInput = new BlobCacheIndexInput(
                    evictFileName,
                    randomIOContext(),
                    new CacheFileReader(
                        sharedBlobCacheService.getCacheFile(
                            new FileCacheKey(shardId, primaryTerm, evictFileName),
                            evictInput.length,
                            SharedBlobCacheService.CacheMissHandler.NOOP
                        ),
                        createBlobReader(evictFileName, evictInput, sharedBlobCacheService),
                        createBlobFileRanges(primaryTerm, 0L, 0, evictInput.length),
                        BlobCacheMetrics.NOOP,
                        System::currentTimeMillis
                    ),
                    null,
                    evictInput.length,
                    0
                );
                byte[] evictOutput = randomReadAndSlice(evictIndexInput, evictInput.length);
                assertArrayEquals(evictInput, evictOutput);
            }

            // After eviction, withByteBufferSlices should return false
            boolean availableAfter = indexInputA.withByteBufferSlices(offsets, sliceLen, 2, slices -> {
                fail("action should not be invoked after eviction");
            });
            assertFalse("expected buffers to be unavailable after eviction", availableAfter);
        }
    }

    // Verifies that prefetch on a sliced BlobCacheIndexInput correctly translates the offset
    // by adding this.offset before forwarding to CacheFileReader.tryPrefetch. Uses a mock
    // CacheFile to capture the actual offset argument, and doSlice to bypass the buffer-based
    // fast path (trySliceBuffer) that may return a ByteArrayIndexInput.
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testPrefetchOnSlice() throws IOException {
        final SharedBlobCacheService.CacheFile cacheFile = mock(SharedBlobCacheService.CacheFile.class);
        when(cacheFile.copy()).thenReturn(cacheFile);
        when(cacheFile.tryPrefetch(anyLong(), anyLong())).thenReturn(true);
        final CacheBlobReader cacheBlobReader = mock(CacheBlobReader.class);
        final long primaryTerm = randomNonNegativeLong();
        final long fileLength = randomLongBetween(200, 10_000);
        final CacheFileReader cacheFileReader = new CacheFileReader(
            cacheFile,
            cacheBlobReader,
            createBlobFileRanges(primaryTerm, 0L, 0, (int) fileLength),
            BlobCacheMetrics.NOOP,
            System::currentTimeMillis
        );
        final BlobCacheIndexInput indexInput = new BlobCacheIndexInput(
            "test-file",
            randomIOContext(),
            cacheFileReader,
            null,
            fileLength,
            0
        );

        // Prefetch on root input (this.offset == 0) — offset passed as-is
        long prefetchOffset = randomLongBetween(0, fileLength - 2);
        long prefetchLength = randomLongBetween(1, fileLength - prefetchOffset);
        indexInput.prefetch(prefetchOffset, prefetchLength);
        verify(cacheFile).tryPrefetch(prefetchOffset, prefetchLength);

        // Create a slice with non-zero offset (use doSlice to bypass trySliceBuffer)
        long sliceOffset = randomLongBetween(1, fileLength / 2);
        long sliceLength = randomLongBetween(2, fileLength - sliceOffset);
        BlobCacheIndexInput slice = asInstanceOf(BlobCacheIndexInput.class, indexInput.doSlice("test-slice", sliceOffset, sliceLength));

        // Prefetch on slice must translate the offset by adding sliceOffset
        clearInvocations(cacheFile);
        long slicePrefetchOffset = randomLongBetween(0, sliceLength - 2);
        long slicePrefetchLength = randomLongBetween(1, sliceLength - slicePrefetchOffset);
        slice.prefetch(slicePrefetchOffset, slicePrefetchLength);
        verify(cacheFile).tryPrefetch(sliceOffset + slicePrefetchOffset, slicePrefetchLength);
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
        return sharedCacheSettings(cacheSize, pageAligned(ByteSizeValue.of(randomIntBetween(4, 1024), ByteSizeUnit.KB)));
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
        return new TestThreadPool(name, StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
    }
}
