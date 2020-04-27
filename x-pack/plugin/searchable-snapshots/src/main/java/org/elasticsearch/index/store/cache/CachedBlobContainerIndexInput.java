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
import org.elasticsearch.common.util.concurrent.ReleasableLock;
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
    protected void readInternal(final byte[] buffer, final int offset, final int length) throws IOException {
        ensureContext(ctx -> ctx != CACHE_WARMING_CONTEXT);
        final long position = getFilePointer() + this.offset;

        int totalBytesRead = 0;
        while (totalBytesRead < length) {
            final long pos = position + totalBytesRead;
            final int off = offset + totalBytesRead;
            final int len = length - totalBytesRead;

            int bytesRead = 0;
            try {
                final CacheFile cacheFile = getCacheFileSafe();
                try (ReleasableLock ignored = cacheFile.fileLock()) {
                    final Tuple<Long, Long> range = computeRange(pos);
                    bytesRead = cacheFile.fetchRange(
                        range.v1(),
                        range.v2(),
                        (start, end) -> readCacheFile(cacheFile.getChannel(), end, pos, buffer, off, len),
                        (start, end) -> writeCacheFile(cacheFile.getChannel(), start, end)
                    ).get();
                }
            } catch (final Exception e) {
                if (e instanceof AlreadyClosedException || (e.getCause() != null && e.getCause() instanceof AlreadyClosedException)) {
                    try {
                        // cache file was evicted during the range fetching, read bytes directly from source
                        bytesRead = readDirectly(pos, pos + len, buffer, off);
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
    public int prefetchPart(final int part) throws IOException {
        ensureContext(ctx -> ctx == CACHE_WARMING_CONTEXT);
        if (part >= fileInfo.numberOfParts()) {
            throw new IllegalArgumentException("Unexpected part number [" + part + "]");
        }
        final Tuple<Long, Long> range = computeRange(IntStream.range(0, part).mapToLong(fileInfo::partBytes).sum());
        assert assertRangeIsAlignedWithPart(range);
        try {
            final CacheFile cacheFile = getCacheFileSafe();
            try (ReleasableLock ignored = cacheFile.fileLock()) {
                final int bytesRead = cacheFile.fetchRange(range.v1(), range.v2(), (start, end) -> {
                    logger.trace("range [{}-{}] of file [{}] is now available in cache", start, end, fileInfo.physicalName());
                    return Math.toIntExact(end - start);
                }, (start, end) -> writeCacheFile(cacheFile.getChannel(), start, end)).get();

                assert bytesRead == (range.v2() - range.v1());
                return bytesRead;
            }
        } catch (final Exception e) {
            throw new IOException("Failed to prefetch file part in cache", e);
        }
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

    private int readCacheFile(FileChannel fc, long end, long position, byte[] buffer, int offset, long length) throws IOException {
        assert assertFileChannelOpen(fc);
        int bytesRead = Channels.readFromFileChannel(fc, position, buffer, offset, Math.toIntExact(Math.min(length, end - position)));
        stats.addCachedBytesRead(bytesRead);
        return bytesRead;
    }

    @SuppressForbidden(reason = "Use positional writes on purpose")
    private void writeCacheFile(FileChannel fc, long start, long end) throws IOException {
        assert assertFileChannelOpen(fc);
        final long length = end - start;
        final byte[] copyBuffer = new byte[Math.toIntExact(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> new ParameterizedMessage("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference));

        int bytesCopied = 0;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStream(start, length)) {
            long remaining = end - start;
            while (remaining > 0) {
                final int len = (remaining < copyBuffer.length) ? Math.toIntExact(remaining) : copyBuffer.length;
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
                fc.write(ByteBuffer.wrap(copyBuffer, 0, bytesRead), start + bytesCopied);
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

    private int readDirectly(long start, long end, byte[] buffer, int offset) throws IOException {
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
                System.arraycopy(copyBuffer, 0, buffer, offset + bytesCopied, bytesRead);
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
