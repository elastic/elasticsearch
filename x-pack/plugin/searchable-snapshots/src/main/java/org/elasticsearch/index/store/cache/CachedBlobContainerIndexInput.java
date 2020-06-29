/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.store.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.BaseSearchableSnapshotIndexInput;
import org.elasticsearch.index.store.IndexInputStats;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.IntStream;

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
        final long position = getFilePointer() + this.offset;
        final int length = b.remaining();

        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            final long pos = position + totalBytesRead;
            final int len = length - totalBytesRead;
            int bytesRead = 0;
            try {
                final CacheFile cacheFile = getCacheFileSafe();
                try (Releasable ignored = cacheFile.fileLock()) {
                    final Tuple<Long, Long> range = computeRange(pos);
                    bytesRead = cacheFile.fetchRange(
                        range.v1(),
                        range.v2(),
                        (start, end) -> readCacheFile(cacheFile.getChannel(), end, pos, b, len),
                        (start, end) -> writeCacheFile(cacheFile.getChannel(), start, end)
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
                        final long readStart = rangeStart + totalBytesRead;
                        cacheFile.fetchRange(readStart, readStart + bytesRead, (start, end) -> {
                            logger.trace(
                                "prefetchPart: range [{}-{}] of file [{}] is available in cache",
                                start,
                                end,
                                fileInfo.physicalName()
                            );
                            return Math.toIntExact(end - start);
                        }, (start, end) -> {
                            final ByteBuffer byteBuffer = ByteBuffer.wrap(
                                copyBuffer,
                                Math.toIntExact(start - readStart),
                                Math.toIntExact(end - start)
                            );
                            final int writtenBytes = positionalWrite(fc, start, byteBuffer);
                            logger.trace(
                                "prefetchPart: writing range [{}-{}] of file [{}], [{}] bytes written",
                                start,
                                end,
                                fileInfo.physicalName(),
                                writtenBytes
                            );
                            totalBytesWritten.addAndGet(writtenBytes);
                        });
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

    private int readCacheFile(FileChannel fc, long end, long position, ByteBuffer b, long length) throws IOException {
        assert assertFileChannelOpen(fc);
        final int bytesRead;

        assert b.remaining() == length;
        if (end - position < b.remaining()) {
            final ByteBuffer duplicate = b.duplicate();
            duplicate.limit(b.position() + Math.toIntExact(end - position));
            bytesRead = Channels.readFromFileChannel(fc, position, duplicate);
            assert duplicate.position() < b.limit();
            b.position(duplicate.position());
        } else {
            bytesRead = Channels.readFromFileChannel(fc, position, b);
        }
        if (bytesRead == -1) {
            throw new EOFException(
                String.format(Locale.ROOT, "unexpected EOF reading [%d-%d] from %s", position, position + length, cacheFileReference)
            );
        }
        stats.addCachedBytesRead(bytesRead);
        return bytesRead;
    }

    private void writeCacheFile(FileChannel fc, long start, long end) throws IOException {
        assert assertFileChannelOpen(fc);
        final long length = end - start;
        final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> new ParameterizedMessage("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference));

        long bytesCopied = 0L;
        long remaining = end - start;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStream(start, length)) {
            while (remaining > 0L) {
                final int bytesRead = readSafe(input, copyBuffer, start, end, remaining, cacheFileReference);
                positionalWrite(fc, start + bytesCopied, ByteBuffer.wrap(copyBuffer, 0, bytesRead));
                bytesCopied += bytesRead;
                remaining -= bytesRead;
            }
            final long endTimeNanos = stats.currentTimeNanos();
            stats.addCachedBytesWritten(bytesCopied, endTimeNanos - startTimeNanos);
        }
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
}
