/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.store.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.BaseSearchableSnapshotIndexInput;
import org.elasticsearch.index.store.IndexInputStats;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.ByteRange;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

public class CachedBlobContainerIndexInput extends BaseSearchableSnapshotIndexInput {

    /**
     * Specific IOContext used for prewarming the cache. This context allows to write
     * a complete part of the {@link #fileInfo} at once in the cache and should not be
     * used for anything else than what the {@link #prefetchPart(int)} method does.
     */
    public static final IOContext CACHE_WARMING_CONTEXT = new IOContext();

    private static final Logger logger = LogManager.getLogger(CachedBlobContainerIndexInput.class);
    private static final int COPY_BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(8);

    private final CacheFileReference cacheFileReference;
    private final int defaultRangeSize;
    private final int recoveryRangeSize;

    // last read position is kept around in order to detect (non)contiguous reads for stats
    private long lastReadPosition;
    // last seek position is kept around in order to detect forward/backward seeks for stats
    private long lastSeekPosition;

    public CachedBlobContainerIndexInput(
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        int rangeSize,
        int recoveryRangeSize
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
            rangeSize,
            recoveryRangeSize,
            blobCacheByteRanges(fileInfo.physicalName(), fileInfo.length())
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
        int rangeSize,
        int recoveryRangeSize,
        List<ByteRange> blobCacheByteRanges
    ) {
        super(logger, resourceDesc, directory, fileInfo, context, stats, offset, length, blobCacheByteRanges);
        this.cacheFileReference = cacheFileReference;
        this.lastReadPosition = this.offset;
        this.lastSeekPosition = this.offset;
        this.defaultRangeSize = rangeSize;
        this.recoveryRangeSize = recoveryRangeSize;
    }

    @Override
    public void doClose() {
        if (isClone == false) {
            cacheFileReference.releaseOnClose();
        }
    }

    private long getDefaultRangeSize() {
        return (context != CACHE_WARMING_CONTEXT)
            ? (directory.isRecoveryFinalized() ? defaultRangeSize : recoveryRangeSize)
            : fileInfo.partSize().getBytes();
    }

    private ByteRange computeRange(long position) {
        final long rangeSize = getDefaultRangeSize();
        long start = (position / rangeSize) * rangeSize;
        long end = Math.min(start + rangeSize, fileInfo.length());
        return ByteRange.of(start, end);
    }

    @Override
    protected void doReadInternal(ByteBuffer b) throws IOException {
        ensureContext(ctx -> ctx != CACHE_WARMING_CONTEXT);
        final long position = getFilePointer() + this.offset;
        final int length = b.remaining();

        logger.trace("readInternal: read [{}-{}] ([{}] bytes) from [{}]", position, position + length, length, this);
        try {
            final CacheFile cacheFile = cacheFileReference.get();

            // Are we trying to read data that should be present in blob cache index?
            final ByteRange indexCacheMiss; // null if not a miss
            if (tryReadFromBlobCache()) {
                indexCacheMiss = getBlobCacheByteRange(position, length);
            } else {
                // requested range is not eligible for caching
                indexCacheMiss = null;

                // Can we serve the read directly from disk? If so, do so and don't worry about anything else.
                final Future<Integer> waitingForRead = cacheFile.readIfAvailableOrPending(
                    ByteRange.of(position, position + length),
                    chan -> {
                        final int read = readCacheFile(chan, position, b);
                        assert read == length : read + " vs " + length;
                        return read;
                    }
                );

                if (waitingForRead != null) {
                    final Integer read = waitingForRead.get();
                    assert read == length;
                    onReadComplete(position, length);
                    return;
                }
            }

            // Requested data is also not in the cache index, so we must visit the blob store to satisfy both the target range and any
            // miss in the cache index.

            final ByteRange startRangeToWrite = computeRange(position);
            final ByteRange endRangeToWrite = computeRange(position + length - 1);
            assert startRangeToWrite.end() <= endRangeToWrite.end() : startRangeToWrite + " vs " + endRangeToWrite;
            final ByteRange rangeToWrite = startRangeToWrite.minEnvelope(endRangeToWrite).minEnvelope(indexCacheMiss);

            final ByteRange rangeToRead = ByteRange.of(position, position + length);
            assert rangeToRead.isSubRangeOf(rangeToWrite) : rangeToRead + " vs " + rangeToWrite;
            assert rangeToRead.length() == b.remaining() : b.remaining() + " vs " + rangeToRead;

            final Future<Integer> populateCacheFuture = cacheFile.populateAndRead(
                rangeToWrite,
                rangeToRead,
                channel -> readCacheFile(channel, position, b),
                this::writeCacheFile,
                directory.cacheFetchAsyncExecutor()
            );

            if (indexCacheMiss != null) {
                final Releasable onCacheFillComplete = stats.addIndexCacheFill();
                final Future<Integer> readFuture = cacheFile.readIfAvailableOrPending(indexCacheMiss, channel -> {
                    final int indexCacheMissLength = toIntBytes(indexCacheMiss.length());

                    // Revisit this:
                    // We assume that we only cache small portions of blobs so that we do not need to:
                    // - use a BigArrays for allocation
                    // - use an intermediate copy buffer to read the file in sensibly-sized chunks
                    // - release the buffer once the indexing operation is complete
                    // assert indexCacheMissLength <= COPY_BUFFER_SIZE : indexCacheMiss;

                    final ByteBuffer byteBuffer = ByteBuffer.allocate(indexCacheMissLength);
                    Channels.readFromFileChannelWithEofException(channel, indexCacheMiss.start(), byteBuffer);
                    // NB use Channels.readFromFileChannelWithEofException not readCacheFile() to avoid counting this in the stats
                    byteBuffer.flip();
                    final BytesReference content = BytesReference.fromByteBuffer(byteBuffer);
                    directory.putCachedBlob(fileInfo.physicalName(), indexCacheMiss.start(), content, new ActionListener<>() {
                        @Override
                        public void onResponse(Void response) {
                            onCacheFillComplete.close();
                        }

                        @Override
                        public void onFailure(Exception e1) {
                            onCacheFillComplete.close();
                        }
                    });
                    return indexCacheMissLength;
                });

                if (readFuture == null) {
                    // Normally doesn't happen, we're already obtaining a range covering all cache misses above, but theoretically
                    // possible in the case that the real populateAndRead call already failed to obtain this range of the file. In that
                    // case, simply move on.
                    onCacheFillComplete.close();
                }
            }

            final int bytesRead = populateCacheFuture.get();
            assert bytesRead == length : bytesRead + " vs " + length;
        } catch (final Exception e) {
            // may have partially filled the buffer before the exception was thrown, so try and get the remainder directly.
            final int alreadyRead = length - b.remaining();
            final int bytesRead = readDirectlyIfAlreadyClosed(position + alreadyRead, b, e);
            assert alreadyRead + bytesRead == length : alreadyRead + " + " + bytesRead + " vs " + length;

            // In principle we could handle an index cache miss here too, ensuring that the direct read was large enough, but this is
            // already a rare case caused by an overfull/undersized cache.
        }

        onReadComplete(position, length);
    }

    @Override
    protected void onReadComplete(long position, int length) {
        stats.incrementBytesRead(lastReadPosition, position, length);
        lastReadPosition = position + length;
        lastSeekPosition = lastReadPosition;
    }

    private int readDirectlyIfAlreadyClosed(long position, ByteBuffer b, Exception e) throws IOException {
        if (e instanceof AlreadyClosedException || (e.getCause() != null && e.getCause() instanceof AlreadyClosedException)) {
            try {
                // cache file was evicted during the range fetching, read bytes directly from blob container
                final long length = b.remaining();
                final byte[] copyBuffer = new byte[toIntBytes(Math.min(COPY_BUFFER_SIZE, length))];
                logger.trace(
                    () -> new ParameterizedMessage(
                        "direct reading of range [{}-{}] for cache file [{}]",
                        position,
                        position + length,
                        cacheFileReference
                    )
                );

                int bytesCopied = 0;
                final long startTimeNanos = stats.currentTimeNanos();
                try (InputStream input = openInputStreamFromBlobStore(position, length)) {
                    long remaining = length;
                    while (remaining > 0) {
                        final int len = (remaining < copyBuffer.length) ? (int) remaining : copyBuffer.length;
                        int bytesRead = input.read(copyBuffer, 0, len);
                        if (bytesRead == -1) {
                            throw new EOFException(
                                String.format(
                                    Locale.ROOT,
                                    "unexpected EOF reading [%d-%d] ([%d] bytes remaining) from %s",
                                    position,
                                    position + length,
                                    remaining,
                                    cacheFileReference
                                )
                            );
                        }
                        b.put(copyBuffer, 0, bytesRead);
                        bytesCopied += bytesRead;
                        remaining -= bytesRead;
                        assert remaining == b.remaining() : remaining + " vs " + b.remaining();
                    }
                    final long endTimeNanos = stats.currentTimeNanos();
                    stats.addDirectBytesRead(bytesCopied, endTimeNanos - startTimeNanos);
                }
                return bytesCopied;
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
        }
        throw new IOException("failed to read data from cache", e);
    }

    /**
     * Prefetches a complete part and writes it in cache. This method is used to prewarm the cache.
     * @return a tuple with {@code Tuple<Persistent Cache Length, Prefetched Length>} values
     */
    public Tuple<Long, Long> prefetchPart(final int part) throws IOException {
        ensureContext(ctx -> ctx == CACHE_WARMING_CONTEXT);
        if (part >= fileInfo.numberOfParts()) {
            throw new IllegalArgumentException("Unexpected part number [" + part + "]");
        }
        final ByteRange partRange = computeRange(IntStream.range(0, part).mapToLong(fileInfo::partBytes).sum());
        assert assertRangeIsAlignedWithPart(partRange);

        try {
            final CacheFile cacheFile = cacheFileReference.get();

            final ByteRange range = cacheFile.getAbsentRangeWithin(partRange);
            if (range == null) {
                logger.trace(
                    "prefetchPart: part [{}] bytes [{}-{}] is already fully available for cache file [{}]",
                    part,
                    partRange.start(),
                    partRange.end(),
                    cacheFileReference
                );
                return Tuple.tuple(cacheFile.getInitialLength(), 0L);
            }

            logger.trace(
                "prefetchPart: prewarming part [{}] bytes [{}-{}] by fetching bytes [{}-{}] for cache file [{}]",
                part,
                partRange.start(),
                partRange.end(),
                range.start(),
                range.end(),
                cacheFileReference
            );

            final byte[] copyBuffer = new byte[toIntBytes(Math.min(COPY_BUFFER_SIZE, range.length()))];

            long totalBytesRead = 0L;
            final AtomicLong totalBytesWritten = new AtomicLong();
            long remainingBytes = range.length();
            final long startTimeNanos = stats.currentTimeNanos();
            try (InputStream input = openInputStreamFromBlobStore(range.start(), range.length())) {
                while (remainingBytes > 0L) {
                    assert totalBytesRead + remainingBytes == range.length();
                    final int bytesRead = readSafe(input, copyBuffer, range.start(), range.end(), remainingBytes, cacheFileReference);

                    // The range to prewarm in cache
                    final long readStart = range.start() + totalBytesRead;
                    final ByteRange rangeToWrite = ByteRange.of(readStart, readStart + bytesRead);

                    // We do not actually read anything, but we want to wait for the write to complete before proceeding.
                    // noinspection UnnecessaryLocalVariable
                    final ByteRange rangeToRead = rangeToWrite;
                    cacheFile.populateAndRead(rangeToWrite, rangeToRead, (channel) -> bytesRead, (channel, start, end, progressUpdater) -> {
                        final ByteBuffer byteBuffer = ByteBuffer.wrap(copyBuffer, toIntBytes(start - readStart), toIntBytes(end - start));
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
            assert totalBytesRead == range.length();
            return Tuple.tuple(cacheFile.getInitialLength(), range.length());
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
        final int len = (remaining < copyBuffer.length) ? toIntBytes(remaining) : copyBuffer.length;
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
    private boolean assertRangeIsAlignedWithPart(ByteRange range) {
        if (fileInfo.numberOfParts() == 1L) {
            final long length = fileInfo.length();
            assert range.start() == 0L : "start of range [" + range.start() + "] is not aligned with zero";
            assert range.end() == length : "end of range [" + range.end() + "] is not aligned with file length [" + length + ']';
        } else {
            final long length = fileInfo.partSize().getBytes();
            assert range.start() % length == 0L : "start of range [" + range.start() + "] is not aligned with part start";
            assert range.end() % length == 0L || (range.end() == fileInfo.length()) : "end of range ["
                + range.end()
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

    private void writeCacheFile(final FileChannel fc, final long start, final long end, final Consumer<Long> progressUpdater)
        throws IOException {
        assert assertFileChannelOpen(fc);
        assert assertCurrentThreadMayWriteCacheFile();
        final long length = end - start;
        final byte[] copyBuffer = new byte[toIntBytes(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> new ParameterizedMessage("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference));

        long bytesCopied = 0L;
        long remaining = end - start;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStreamFromBlobStore(start, length)) {
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
            defaultRangeSize,
            recoveryRangeSize,
            getBlobCacheByteRangesForSlice(sliceDescription, offset, length)
        );
        slice.isClone = true;
        return slice;
    }

    @Override
    public String toString() {
        final CacheFile cacheFile = cacheFileReference.cacheFile.get();
        return super.toString()
            + "[cache file="
            + (cacheFile != null
                ? String.join(
                    "/",
                    directory.getShardId().getIndex().getUUID(),
                    String.valueOf(directory.getShardId().getId()),
                    "snapshot_cache",
                    directory.getSnapshotId().getUUID(),
                    cacheFile.getFile().getFileName().toString()
                )
                : null)
            + ']';
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
                newCacheFile.acquire(this);
                final CacheFile previousCacheFile = cacheFile.getAndSet(newCacheFile);
                assert previousCacheFile == null;
                return newCacheFile;
            }
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

    private static boolean assertCurrentThreadMayWriteCacheFile() {
        final String threadName = Thread.currentThread().getName();
        assert isCacheFetchAsyncThread(threadName) : "expected the current thread ["
            + threadName
            + "] to belong to the cache fetch async thread pool";
        return true;
    }
}
