/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobstore.cache.BlobStoreCacheService;
import org.elasticsearch.blobstore.cache.CachedBlob;
import org.elasticsearch.blobstore.cache.CopyOnReadInputStream;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.BaseSearchableSnapshotIndexInput;
import org.elasticsearch.index.store.IndexInputStats;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.index.store.checksum.ChecksumBlobContainerIndexInput.checksumToBytesArray;

public class CachedBlobContainerIndexInput extends BaseSearchableSnapshotIndexInput {

    /**
     * Specific IOContext used for prewarming the cache. This context allows to write
     * a complete part of the {@link #fileInfo} at once in the cache and should not be
     * used for anything else than what the {@link #prefetchPart(int)} method does.
     */
    public static final IOContext CACHE_WARMING_CONTEXT = new IOContext();

    private static final Logger logger = LogManager.getLogger(CachedBlobContainerIndexInput.class);
    private static final int COPY_BUFFER_SIZE = 8192;

    private final SearchableSnapshotDirectory directory;
    private final CacheFileReference cacheFileReference;
    private final int defaultRangeSize;

    // last read position is kept around in order to detect (non)contiguous reads for stats
    private long lastReadPosition;
    // last seek position is kept around in order to detect forward/backward seeks for stats
    private long lastSeekPosition;

    public CachedBlobContainerIndexInput(
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        int rangeSize
    ) {
        this(
            "CachedBlobContainerIndexInput(" + fileInfo.physicalName() + ")",
            directory,
            fileInfo,
            context,
            stats,
            0L,
            fileInfo.length(),
            new CacheFileReference(directory, fileInfo.physicalName(), fileInfo.length()),
            rangeSize
        );
        stats.incrementOpenCount();
    }

    private CachedBlobContainerIndexInput(
        String resourceDesc,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long length,
        CacheFileReference cacheFileReference,
        int rangeSize
    ) {
        super(resourceDesc, directory.blobContainer(), fileInfo, context, stats, offset, length);
        this.directory = directory;
        this.cacheFileReference = cacheFileReference;
        this.lastReadPosition = this.offset;
        this.lastSeekPosition = this.offset;
        this.defaultRangeSize = rangeSize;
    }

    @Override
    public void innerClose() {
        if (isClone == false) {
            cacheFileReference.releaseOnClose();
        }
    }

    private void ensureContext(Predicate<IOContext> predicate) throws IOException {
        if (predicate.test(context) == false) {
            assert false : "this method should not be used with this context " + context;
            throw new IOException("Cannot read the index input using context [context=" + context + ", input=" + this + ']');
        }
    }

    private long getDefaultRangeSize() {
        return (context != CACHE_WARMING_CONTEXT) ? defaultRangeSize : fileInfo.partSize().getBytes();
    }

    private Tuple<Long, Long> computeRange(long position) {
        final long rangeSize = getDefaultRangeSize();
        long start = (position / rangeSize) * rangeSize;
        long end = Math.min(start + rangeSize, fileInfo.length());
        return Tuple.tuple(start, end);
    }

    private CacheFile getCacheFileSafe() throws Exception {
        final CacheFile cacheFile = cacheFileReference.get();
        if (cacheFile == null) {
            throw new AlreadyClosedException("Failed to acquire a non-evicted cache file");
        }
        return cacheFile;
    }

    @Override
    protected void readInternal(ByteBuffer b) throws IOException {
        ensureContext(ctx -> ctx != CACHE_WARMING_CONTEXT);
        assert assertCurrentThreadIsNotCacheFetchAsync();
        final long position = getFilePointer() + this.offset;
        final int length = b.remaining();

        logger.trace("readInternal: read [{}-{}] from [{}]", position, position + length, this);

        // We can detect that we're going to read the last 16 bytes (that contains the footer checksum) of the file. Such reads are often
        // executed when opening a Directory and since we have the checksum in the snapshot metadata we can use it to fill the ByteBuffer.
        if (length == CodecUtil.footerLength()) {
            // we're comparing the global position (file pointer + offset) with the total file length (not the length of the index input
            // which can be a slice) so the following code only applies when reading the footer of non-sliced index inputs (we're asserting
            // that we are not reading from a clone as it would be surprising that Lucene uses a slice to verify the footer of a .cfs file)
            if (position == fileInfo.length() - length) {
                logger.trace("reading footer of file [{}] at position [{}], bypassing all caches", fileInfo.physicalName(), position);
                b.put(checksumToBytesArray(fileInfo.checksum()));
                assert b.remaining() == 0L;
                assert isClone == false;
                return; // TODO we should add this to DirectBlobContainerIndexInput too.
            }
        }

        final List<Tuple<Long, Integer>> regions;

        // We prefer to use the index cache if the recovery is not done yet
        if (directory.isRecoveryDone() == false) {
            // We try to use the snapshot blob cache if:
            // - we're reading the first N bytes of the file
            final boolean isStartOfFile = (position + length <= BlobStoreCacheService.DEFAULT_SIZE);
            // - the file is small enough to be fully cached in the blob cache
            final boolean canBeFullyCached = (fileInfo.length() <= (BlobStoreCacheService.DEFAULT_SIZE));

            if (canBeFullyCached || isStartOfFile) {
                final CachedBlob cachedBlob = directory.getCachedBlob(fileInfo.physicalName(), 0L, length);
                if (cachedBlob != null) {
                    logger.trace(
                        "reading [{}] bytes of file [{}] at position [{}] using index cache",
                        length,
                        fileInfo.physicalName(),
                        position
                    );
                    b.put(BytesReference.toBytes(cachedBlob.bytes().slice(Math.toIntExact(position), length)));
                    return;
                }
            }

            // We would have liked to find a cached entry but we did not find anything: the cache on the disk will be requested so
            // we compute the regions of the file we would like to have the next time. The regions are expressed as tuple of
            // {position, length} where position is relative to the file.
            if (canBeFullyCached) {
                // if the index input is smaller than twice the size of the blob cache, it will be fully indexed
                regions = List.of(Tuple.tuple(0L, Math.toIntExact(fileInfo.length())));
            } else {
                regions = List.of(Tuple.tuple(0L, BlobStoreCacheService.DEFAULT_SIZE));
            }
            logger.trace("recovery cache miss for [{}], falling through with regions [{}]", this, regions);
        } else {
            regions = List.of();
        }

        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            final long pos = position + totalBytesRead;
            final int len = length - totalBytesRead;
            int bytesRead = 0;
            try {
                final CacheFile cacheFile = getCacheFileSafe();
                try (Releasable ignored = cacheFile.fileLock()) {
                    final Tuple<Long, Long> rangeToWrite = computeRange(pos);
                    final Tuple<Long, Long> rangeToRead = Tuple.tuple(pos, Math.min(pos + len, rangeToWrite.v2()));

                    bytesRead = cacheFile.fetchAsync(rangeToWrite, rangeToRead, (channel) -> {
                        final int read;
                        if ((rangeToRead.v2() - rangeToRead.v1()) < b.remaining()) {
                            final ByteBuffer duplicate = b.duplicate();
                            duplicate.limit(duplicate.position() + Math.toIntExact(rangeToRead.v2() - rangeToRead.v1()));
                            read = readCacheFile(channel, pos, duplicate);
                            assert duplicate.position() <= b.limit();
                            b.position(duplicate.position());
                        } else {
                            read = readCacheFile(channel, pos, b);
                        }
                        return read;
                    },
                        (channel, from, to, progressUpdater) -> writeCacheFile(channel, from, to, progressUpdater, regions, logger),
                        directory.cacheFetchAsyncExecutor()
                    ).get();
                }
            } catch (final Exception e) {
                if (e instanceof AlreadyClosedException || (e.getCause() != null && e.getCause() instanceof AlreadyClosedException)) {
                    try {
                        // cache file was evicted during the range fetching, read bytes directly from source
                        bytesRead = readDirectly(pos, pos + len, b);
                        continue;
                    } catch (Exception inner) {
                        e.addSuppressed(inner);
                    }
                }
                throw new IOException("Fail to read data from cache", e);

            } finally {
                totalBytesRead += bytesRead;
            }
        }
        assert totalBytesRead == length : "partial read operation, read [" + totalBytesRead + "] bytes of [" + length + "]";
        stats.incrementBytesRead(lastReadPosition, position, totalBytesRead);
        lastReadPosition = position + totalBytesRead;
        lastSeekPosition = lastReadPosition;
    }

    /**
     * Prefetches a complete part and writes it in cache. This method is used to prewarm the cache.
     */
    public void prefetchPart(final int part) throws IOException {
        ensureContext(ctx -> ctx == CACHE_WARMING_CONTEXT);
        if (part >= fileInfo.numberOfParts()) {
            throw new IllegalArgumentException("Unexpected part number [" + part + "]");
        }
        final Tuple<Long, Long> partRange = computeRange(IntStream.range(0, part).mapToLong(fileInfo::partBytes).sum());
        assert assertRangeIsAlignedWithPart(partRange);

        try {
            final CacheFile cacheFile = getCacheFileSafe();
            try (Releasable ignored = cacheFile.fileLock()) {

                final Tuple<Long, Long> range = cacheFile.getAbsentRangeWithin(partRange.v1(), partRange.v2());
                if (range == null) {
                    logger.trace(
                        "prefetchPart: part [{}] bytes [{}-{}] is already fully available for cache file [{}]",
                        part,
                        partRange.v1(),
                        partRange.v2(),
                        cacheFileReference
                    );
                    return;
                }

                final long rangeStart = range.v1();
                final long rangeEnd = range.v2();
                final long rangeLength = rangeEnd - rangeStart;

                logger.trace(
                    "prefetchPart: prewarming part [{}] bytes [{}-{}] by fetching bytes [{}-{}] for cache file [{}]",
                    part,
                    partRange.v1(),
                    partRange.v2(),
                    rangeStart,
                    rangeEnd,
                    cacheFileReference
                );

                final FileChannel fc = cacheFile.getChannel();
                assert assertFileChannelOpen(fc);
                final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, rangeLength))];

                long totalBytesRead = 0L;
                final AtomicLong totalBytesWritten = new AtomicLong();
                long remainingBytes = rangeEnd - rangeStart;
                final long startTimeNanos = stats.currentTimeNanos();
                try (InputStream input = openInputStream(rangeStart, rangeLength)) {
                    while (remainingBytes > 0L) {
                        assert totalBytesRead + remainingBytes == rangeLength;
                        final int bytesRead = readSafe(input, copyBuffer, rangeStart, rangeEnd, remainingBytes, cacheFileReference);

                        // The range to prewarm in cache
                        final long readStart = rangeStart + totalBytesRead;
                        final Tuple<Long, Long> rangeToWrite = Tuple.tuple(readStart, readStart + bytesRead);

                        cacheFile.fetchAsync(rangeToWrite, rangeToWrite, (channel) -> bytesRead, (channel, start, end, progressUpdater) -> {
                            final ByteBuffer byteBuffer = ByteBuffer.wrap(
                                copyBuffer,
                                Math.toIntExact(start - readStart),
                                Math.toIntExact(end - start)
                            );
                            final int writtenBytes = positionalWrite(channel, start, byteBuffer);
                            logger.trace(
                                "prefetchPart: writing range [{}-{}] of file [{}], [{}] bytes written",
                                start,
                                end,
                                fileInfo.physicalName(),
                                writtenBytes
                            );
                            totalBytesWritten.addAndGet(writtenBytes);
                            progressUpdater.accept(start + writtenBytes);
                        }, directory.cacheFetchAsyncExecutor()).get();
                        totalBytesRead += bytesRead;
                        remainingBytes -= bytesRead;
                    }
                    final long endTimeNanos = stats.currentTimeNanos();
                    stats.addCachedBytesWritten(totalBytesWritten.get(), endTimeNanos - startTimeNanos);
                }

                assert totalBytesRead == rangeLength;
            }
        } catch (final Exception e) {
            throw new IOException("Failed to prefetch file part in cache", e);
        }
    }

    @SuppressForbidden(reason = "Use positional writes on purpose")
    private static int positionalWrite(FileChannel fc, long start, ByteBuffer byteBuffer) throws IOException {
        assert assertCurrentThreadMayWriteCacheFile();
        return fc.write(byteBuffer, start);
    }

    /**
     * Perform a single {@code read()} from {@code inputStream} into {@code copyBuffer}, handling an EOF by throwing an {@link EOFException}
     * rather than returning {@code -1}. Returns the number of bytes read, which is always positive.
     *
     * Most of its arguments are there simply to make the message of the {@link EOFException} more informative.
     */
    private static int readSafe(
        InputStream inputStream,
        byte[] copyBuffer,
        long rangeStart,
        long rangeEnd,
        long remaining,
        CacheFileReference cacheFileReference
    ) throws IOException {
        final int len = (remaining < copyBuffer.length) ? Math.toIntExact(remaining) : copyBuffer.length;
        final int bytesRead = inputStream.read(copyBuffer, 0, len);
        if (bytesRead == -1) {
            throw new EOFException(
                String.format(
                    Locale.ROOT,
                    "unexpected EOF reading [%d-%d] ([%d] bytes remaining) from %s",
                    rangeStart,
                    rangeEnd,
                    remaining,
                    cacheFileReference
                )
            );
        }
        assert bytesRead > 0 : bytesRead;
        return bytesRead;
    }

    /**
     * Asserts that the range of bytes to warm in cache is aligned with {@link #fileInfo}'s part size.
     */
    private boolean assertRangeIsAlignedWithPart(Tuple<Long, Long> range) {
        if (fileInfo.numberOfParts() == 1L) {
            final long length = fileInfo.length();
            assert range.v1() == 0L : "start of range [" + range.v1() + "] is not aligned with zero";
            assert range.v2() == length : "end of range [" + range.v2() + "] is not aligned with file length [" + length + ']';
        } else {
            final long length = fileInfo.partSize().getBytes();
            assert range.v1() % length == 0L : "start of range [" + range.v1() + "] is not aligned with part start";
            assert range.v2() % length == 0L || (range.v2() == fileInfo.length()) : "end of range ["
                + range.v2()
                + "] is not aligned with part end or with file length";
        }
        return true;
    }

    private int readCacheFile(final FileChannel fc, final long position, final ByteBuffer buffer) throws IOException {
        assert assertFileChannelOpen(fc);
        final int bytesRead = Channels.readFromFileChannel(fc, position, buffer);
        if (bytesRead == -1) {
            throw new EOFException(
                String.format(
                    Locale.ROOT,
                    "unexpected EOF reading [%d-%d] from %s",
                    position,
                    position + buffer.remaining(),
                    cacheFileReference
                )
            );
        }
        stats.addCachedBytesRead(bytesRead);
        return bytesRead;
    }

    private void writeCacheFile(
        final FileChannel fc,
        final long start,
        final long end,
        final Consumer<Long> progressUpdater,
        final List<Tuple<Long, Integer>> cacheableRegions,
        final Logger logger
    ) throws IOException {
        assert assertFileChannelOpen(fc);
        assert assertCurrentThreadMayWriteCacheFile();
        final long length = end - start;
        final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> new ParameterizedMessage("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference));

        long bytesCopied = 0L;
        long remaining = end - start;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStream(start, length, cacheableRegions, directory::putCachedBlob, logger)) {
            while (remaining > 0L) {
                final int bytesRead = readSafe(input, copyBuffer, start, end, remaining, cacheFileReference);
                positionalWrite(fc, start + bytesCopied, ByteBuffer.wrap(copyBuffer, 0, bytesRead));
                bytesCopied += bytesRead;
                remaining -= bytesRead;
                progressUpdater.accept(start + bytesCopied);
            }
            final long endTimeNanos = stats.currentTimeNanos();
            stats.addCachedBytesWritten(bytesCopied, endTimeNanos - startTimeNanos);
        }
    }

    private InputStream openInputStream(
        final long position,
        final long length,
        final List<Tuple<Long, Integer>> regions,
        final TriConsumer<String, Long, ReleasableBytesReference> blobStoreCacher,
        final Logger logger
    ) throws IOException {
        final InputStream stream = openInputStream(position, length);
        if (regions == null || regions.isEmpty()) {
            logger.trace("returning bare stream for [{}]", fileInfo);
            return stream;
        }

        //
        // TODO I'm so sorry. This should be done differently, maybe using a smarter CopyOnReadInputStream
        //
        // The idea is to build a SequenceInputStream that wraps the stream from the blob store repository
        // into multiple limited streams that copy over the bytes of the regions.
        //
        // If while reading the stream we saw interesting regions to cache, we index them. It means that
        // we first have to sort the regions and exclude the ones that we're not going to see anyway.
        //
        // TODO we should check overlapping regions too

        logger.trace("returning caching stream for [{}] with regions [{}]", this, regions);

        final Iterator<Tuple<Long, Integer>> sortedRegions = regions.stream()
            .filter(region -> position <= region.v1())
            .filter(region -> region.v1() + region.v2() <= position + length)
            .sorted(Comparator.comparing(Tuple::v1))
            .collect(Collectors.toList())
            .iterator();

        final List<InputStream> streams = new ArrayList<>();
        for (long p = position; p < position + length;) {
            if (sortedRegions.hasNext()) {
                final Tuple<Long, Integer> nextRegion = sortedRegions.next();
                if (p < nextRegion.v1()) {
                    long limit = nextRegion.v1() - p;
                    streams.add(Streams.limitStream(Streams.noCloseStream(stream), limit));
                    p += limit;
                }
                assert p == nextRegion.v1();

                long limit = nextRegion.v2();
                streams.add(
                    new CopyOnReadInputStream(
                        Streams.limitStream(p + limit < length ? Streams.noCloseStream(stream) : stream, limit),
                        BigArrays.NON_RECYCLING_INSTANCE.newByteArray(limit),
                            // TODO use proper BigArrays, also let the CopyOnReadInputStream allocate this
                        new ActionListener<>() {
                            @Override
                            public void onResponse(ReleasableBytesReference releasableBytesReference) {
                                logger.trace(
                                    () -> new ParameterizedMessage(
                                        "indexing bytes of file [{}] for region [{}-{}] in blob cache index",
                                        this,
                                        nextRegion.v1(),
                                        nextRegion.v1() + nextRegion.v2()
                                    )
                                );
                                blobStoreCacher.apply(fileInfo.physicalName(), nextRegion.v1(), releasableBytesReference);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.trace(
                                    () -> new ParameterizedMessage(
                                        "fail to index bytes of file [{}] for region [{}-{}] in blob cache index",
                                        this,
                                        nextRegion.v1(),
                                        nextRegion.v1() + nextRegion.v2()
                                    ),
                                    e
                                );
                            }
                        }
                    )
                );
                p += limit;
                assert p == nextRegion.v1() + nextRegion.v2();
            } else if (p < position + length) {
                long limit = position + length - p;
                streams.add(Streams.limitStream(stream, limit));
                p += limit;
            }
        }
        if (streams.size() == 1) {
            return streams.get(0);
        }
        return new SequenceInputStream(Collections.enumeration(streams));
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("Reading past end of file [position=" + pos + ", length=" + length() + "] for " + toString());
        } else if (pos < 0L) {
            throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
        }
        final long position = pos + this.offset;
        stats.incrementSeeks(lastSeekPosition, position);
        lastSeekPosition = position;
    }

    @Override
    public CachedBlobContainerIndexInput clone() {
        return (CachedBlobContainerIndexInput) super.clone();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) {
        if (offset < 0 || length < 0 || offset + length > length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceDescription
                    + " out of bounds: offset="
                    + offset
                    + ",length="
                    + length
                    + ",fileLength="
                    + length()
                    + ": "
                    + this
            );
        }
        final CachedBlobContainerIndexInput slice = new CachedBlobContainerIndexInput(
            getFullSliceDescription(sliceDescription),
            directory,
            fileInfo,
            context,
            stats,
            this.offset + offset,
            length,
            cacheFileReference,
            defaultRangeSize
        );
        slice.isClone = true;
        return slice;
    }

    @Override
    public String toString() {
        return "CachedBlobContainerIndexInput{"
            + "cacheFileReference="
            + cacheFileReference
            + ", offset="
            + offset
            + ", length="
            + length()
            + ", position="
            + getFilePointer()
            + ", rangeSize="
            + getDefaultRangeSize()
            + ", directory="
            + directory
            + '}';
    }

    private int readDirectly(long start, long end, ByteBuffer b) throws IOException {
        final long length = end - start;
        final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> new ParameterizedMessage("direct reading of range [{}-{}] for cache file [{}]", start, end, cacheFileReference));

        int bytesCopied = 0;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStream(start, length)) {
            long remaining = end - start;
            while (remaining > 0) {
                final int len = (remaining < copyBuffer.length) ? (int) remaining : copyBuffer.length;
                int bytesRead = input.read(copyBuffer, 0, len);
                if (bytesRead == -1) {
                    throw new EOFException(
                        String.format(
                            Locale.ROOT,
                            "unexpected EOF reading [%d-%d] ([%d] bytes remaining) from %s",
                            start,
                            end,
                            remaining,
                            cacheFileReference
                        )
                    );
                }
                b.put(copyBuffer, 0, bytesRead);
                bytesCopied += bytesRead;
                remaining -= bytesRead;
            }
            final long endTimeNanos = stats.currentTimeNanos();
            stats.addDirectBytesRead(bytesCopied, endTimeNanos - startTimeNanos);
        }
        return bytesCopied;
    }

    private static class CacheFileReference implements CacheFile.EvictionListener {

        private final long fileLength;
        private final CacheKey cacheKey;
        private final SearchableSnapshotDirectory directory;
        private final AtomicReference<CacheFile> cacheFile = new AtomicReference<>(); // null if evicted or not yet acquired

        private CacheFileReference(SearchableSnapshotDirectory directory, String fileName, long fileLength) {
            this.cacheKey = directory.createCacheKey(fileName);
            this.fileLength = fileLength;
            this.directory = directory;
        }

        @Nullable
        CacheFile get() throws Exception {
            CacheFile currentCacheFile = cacheFile.get();
            if (currentCacheFile != null) {
                return currentCacheFile;
            }

            final CacheFile newCacheFile = directory.getCacheFile(cacheKey, fileLength);
            synchronized (this) {
                currentCacheFile = cacheFile.get();
                if (currentCacheFile != null) {
                    return currentCacheFile;
                }
                if (newCacheFile.acquire(this)) {
                    final CacheFile previousCacheFile = cacheFile.getAndSet(newCacheFile);
                    assert previousCacheFile == null;
                    return newCacheFile;
                }
            }
            return null;
        }

        @Override
        public void onEviction(final CacheFile evictedCacheFile) {
            synchronized (this) {
                if (cacheFile.compareAndSet(evictedCacheFile, null)) {
                    evictedCacheFile.release(this);
                }
            }
        }

        void releaseOnClose() {
            synchronized (this) {
                final CacheFile currentCacheFile = cacheFile.getAndSet(null);
                if (currentCacheFile != null) {
                    currentCacheFile.release(this);
                }
            }
        }

        @Override
        public String toString() {
            return "CacheFileReference{"
                + "cacheKey='"
                + cacheKey
                + '\''
                + ", fileLength="
                + fileLength
                + ", acquired="
                + (cacheFile.get() != null)
                + '}';
        }
    }

    private static boolean assertFileChannelOpen(FileChannel fileChannel) {
        assert fileChannel != null;
        assert fileChannel.isOpen();
        return true;
    }

    private static boolean isCacheFetchAsyncThread(final String threadName) {
        return threadName.contains('[' + SearchableSnapshotsConstants.CACHE_FETCH_ASYNC_THREAD_POOL_NAME + ']');
    }

    private static boolean assertCurrentThreadMayWriteCacheFile() {
        final String threadName = Thread.currentThread().getName();
        assert isCacheFetchAsyncThread(threadName) : "expected the current thread ["
            + threadName
            + "] to belong to the cache fetch async thread pool";
        return true;
    }

    private static boolean assertCurrentThreadIsNotCacheFetchAsync() {
        final String threadName = Thread.currentThread().getName();
        assert false == isCacheFetchAsyncThread(threadName) : "expected the current thread ["
            + threadName
            + "] to belong to the cache fetch async thread pool";
        return true;
    }
}
