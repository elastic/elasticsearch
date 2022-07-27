/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.CachedBlob;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Locale;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

/**
 * Searchable snapshots index input that supports fully caching metadata files as well as header/footer information for each file,
 * consisting of a two-level cache (BlobStoreCacheService and CacheService).
 */
public abstract class MetadataCachingIndexInput extends BaseSearchableSnapshotIndexInput {

    protected static final int COPY_BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(8);

    protected final CacheFileReference cacheFileReference;

    /**
     * If > 0, represents a logical file within a compound (CFS) file or is a slice thereof represents the offset of the logical
     * compound file within the physical CFS file
     */
    protected final long compoundFileOffset;

    protected final int defaultRangeSize;
    protected final int recoveryRangeSize;

    // last read position is kept around in order to detect (non)contiguous reads for stats
    protected long lastReadPosition;
    // last seek position is kept around in order to detect forward/backward seeks for stats
    protected long lastSeekPosition;

    public MetadataCachingIndexInput(
        Logger logger,
        String name,
        SearchableSnapshotDirectory directory,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long compoundFileOffset,
        long length,
        CacheFileReference cacheFileReference,
        int defaultRangeSize,
        int recoveryRangeSize,
        ByteRange headerBlobCacheByteRange,
        ByteRange footerBlobCacheByteRange
    ) {
        super(logger, name, directory, fileInfo, context, stats, offset, length, headerBlobCacheByteRange, footerBlobCacheByteRange);
        this.cacheFileReference = cacheFileReference;
        this.compoundFileOffset = compoundFileOffset;
        this.defaultRangeSize = defaultRangeSize;
        this.recoveryRangeSize = recoveryRangeSize;
        this.lastReadPosition = offset;
        this.lastSeekPosition = offset;
        assert offset >= compoundFileOffset;
        assert getBufferSize() <= BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE; // must be able to cache at least one buffer's worth
    }

    @Override
    protected void doReadInternal(ByteBuffer b) throws IOException {
        final long position = getAbsolutePosition();
        final int length = b.remaining();

        logger.trace("readInternal: read [{}-{}] ([{}] bytes) from [{}]", position, position + length, length, this);

        try {
            final ByteRange blobCacheByteRange = rangeToReadFromBlobCache(position, length);
            if (blobCacheByteRange.isEmpty()) {
                readWithoutBlobCache(b);
            } else {
                readWithBlobCache(b, blobCacheByteRange);
            }
        } catch (final Exception e) {
            // may have partially filled the buffer before the exception was thrown, so try and get the remainder directly.
            final int alreadyRead = length - b.remaining();
            final int bytesRead = readDirectlyIfAlreadyClosed(position + alreadyRead, b, e);
            assert alreadyRead + bytesRead == length : alreadyRead + " + " + bytesRead + " vs " + length;
        }

        readComplete(position, length);
    }

    protected abstract void readWithoutBlobCache(ByteBuffer b) throws Exception;

    private void readWithBlobCache(ByteBuffer b, ByteRange blobCacheByteRange) throws Exception {
        final long position = getAbsolutePosition();
        final int length = b.remaining();

        final CacheFile cacheFile = cacheFileReference.get();

        // Can we serve the read directly from disk? If so, do so and don't worry about anything else.
        final Future<Integer> waitingForRead = cacheFile.readIfAvailableOrPending(ByteRange.of(position, position + length), chan -> {
            final int read = readCacheFile(chan, position, b);
            assert read == length : read + " vs " + length;
            return read;
        });

        if (waitingForRead != null) {
            final Integer read = waitingForRead.get();
            assert read == length;
            return;
        }

        final CachedBlob cachedBlob = directory.getCachedBlob(fileInfo.physicalName(), blobCacheByteRange);
        assert cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY || cachedBlob.from() <= position;
        assert cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY || length <= cachedBlob.length();

        if (cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY) {
            // We would have liked to find a cached entry but we did not find anything: the cache on the disk will be requested
            // so we compute the region of the file we would like to have the next time. The region is expressed as a tuple of
            // {start, end} where positions are relative to the whole file.

            // We must fill in a cache miss even if CACHE_NOT_READY since the cache index is only created on the first put.
            // TODO TBD use a different trigger for creating the cache index and avoid a put in the CACHE_NOT_READY case.
            final ByteRange rangeToWrite = blobCacheByteRange;
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

            fillIndexCache(cacheFile, blobCacheByteRange);
            if (compoundFileOffset > 0L
                && blobCacheByteRange.equals(headerBlobCacheByteRange)
                && footerBlobCacheByteRange.isEmpty() == false) {
                fillIndexCache(cacheFile, footerBlobCacheByteRange);
            }

            final int bytesRead = populateCacheFuture.get();
            assert bytesRead == length : bytesRead + " vs " + length;
        } else {
            final int sliceOffset = toIntBytes(position - cachedBlob.from());
            assert sliceOffset + length <= cachedBlob.to()
                : "reading " + length + " bytes from " + sliceOffset + " exceed cached blob max position " + cachedBlob.to();

            logger.trace("reading [{}] bytes of file [{}] at position [{}] using cache index", length, fileInfo.physicalName(), position);
            final BytesRefIterator cachedBytesIterator = cachedBlob.bytes().slice(sliceOffset, length).iterator();
            BytesRef bytesRef;
            int copiedBytes = 0;
            while ((bytesRef = cachedBytesIterator.next()) != null) {
                b.put(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                copiedBytes += bytesRef.length;
            }
            assert copiedBytes == length : "copied " + copiedBytes + " but expected " + length;
            stats.addIndexCacheBytesRead(cachedBlob.length());

            try {
                final ByteRange cachedRange = ByteRange.of(cachedBlob.from(), cachedBlob.to());
                cacheFile.populateAndRead(
                    cachedRange,
                    cachedRange,
                    channel -> cachedBlob.length(),
                    (channel, from, to, progressUpdater) -> {
                        final long startTimeNanos = stats.currentTimeNanos();
                        final BytesRefIterator iterator = cachedBlob.bytes()
                            .slice(toIntBytes(from - cachedBlob.from()), toIntBytes(to - from))
                            .iterator();
                        long writePosition = from;
                        BytesRef current;
                        while ((current = iterator.next()) != null) {
                            final ByteBuffer byteBuffer = ByteBuffer.wrap(current.bytes, current.offset, current.length);
                            while (byteBuffer.remaining() > 0) {
                                writePosition += positionalWrite(channel, writePosition, byteBuffer);
                                progressUpdater.accept(writePosition);
                            }
                        }
                        assert writePosition == to : writePosition + " vs " + to;
                        final long endTimeNanos = stats.currentTimeNanos();
                        stats.addCachedBytesWritten(to - from, endTimeNanos - startTimeNanos);
                        logger.trace("copied bytes [{}-{}] of file [{}] from cache index to disk", from, to, fileInfo);
                    },
                    directory.cacheFetchAsyncExecutor()
                );
            } catch (Exception e) {
                logger.debug(
                    () -> format(
                        "failed to store bytes [%s-%s] of file [%s] obtained from index cache",
                        cachedBlob.from(),
                        cachedBlob.to(),
                        fileInfo
                    ),
                    e
                );
                // oh well, no big deal, at least we can return them to the caller.
            }
        }
    }

    private void readComplete(long position, int length) {
        stats.incrementBytesRead(lastReadPosition, position, length);
        lastReadPosition = position + length;
        lastSeekPosition = lastReadPosition;
    }

    protected int readCacheFile(final FileChannel fc, final long position, final ByteBuffer buffer) throws IOException {
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

    protected void writeCacheFile(final FileChannel fc, final long start, final long end, final Consumer<Long> progressUpdater)
        throws IOException {
        assert assertFileChannelOpen(fc);
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        final long length = end - start;
        final byte[] copyBuffer = new byte[toIntBytes(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> format("writing range [%s-%s] to cache file [%s]", start, end, cacheFileReference));

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

    private void fillIndexCache(CacheFile cacheFile, ByteRange indexCacheMiss) {
        final Releasable onCacheFillComplete = stats.addIndexCacheFill();
        final Future<Integer> readFuture = cacheFile.readIfAvailableOrPending(indexCacheMiss, channel -> {
            final int indexCacheMissLength = toIntBytes(indexCacheMiss.length());

            // We assume that we only cache small portions of blobs so that we do not need to:
            // - use a BigArrays for allocation
            // - use an intermediate copy buffer to read the file in sensibly-sized chunks
            // - release the buffer once the indexing operation is complete

            final ByteBuffer byteBuffer = ByteBuffer.allocate(indexCacheMissLength);
            Channels.readFromFileChannelWithEofException(channel, indexCacheMiss.start(), byteBuffer);
            // NB use Channels.readFromFileChannelWithEofException not readCacheFile() to avoid counting this in the stats
            byteBuffer.flip();
            final BytesReference content = BytesReference.fromByteBuffer(byteBuffer);
            directory.putCachedBlob(fileInfo.physicalName(), indexCacheMiss, content, new ActionListener<>() {
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

    /**
     * Perform a single {@code read()} from {@code inputStream} into {@code copyBuffer}, handling an EOF by throwing an {@link EOFException}
     * rather than returning {@code -1}. Returns the number of bytes read, which is always positive.
     *
     * Most of its arguments are there simply to make the message of the {@link EOFException} more informative.
     */
    protected static int readSafe(
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

    protected static boolean assertFileChannelOpen(FileChannel fileChannel) {
        assert fileChannel != null;
        assert fileChannel.isOpen();
        return true;
    }

    @SuppressForbidden(reason = "Use positional writes on purpose")
    protected static int positionalWrite(FileChannel fc, long start, ByteBuffer byteBuffer) throws IOException {
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        return fc.write(byteBuffer, start);
    }

    protected int readDirectlyIfAlreadyClosed(long position, ByteBuffer b, Exception e) throws IOException {
        if (e instanceof AlreadyClosedException || (e.getCause() != null && e.getCause() instanceof AlreadyClosedException)) {
            try {
                // cache file was evicted during the range fetching, read bytes directly from blob container
                final long length = b.remaining();
                final byte[] copyBuffer = new byte[toIntBytes(Math.min(COPY_BUFFER_SIZE, length))];
                logger.trace(
                    () -> format("direct reading of range [%s-%s] for cache file [%s]", position, position + length, cacheFileReference)
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

    protected abstract long getDefaultRangeSize();

    protected ByteRange computeRange(long position) {
        final long rangeSize = getDefaultRangeSize();
        long start = (position / rangeSize) * rangeSize;
        long end = Math.min(start + rangeSize, fileInfo.length());
        return ByteRange.of(start, end);
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
    public void doClose() {
        if (isClone == false) {
            cacheFileReference.releaseOnClose();
        }
    }

    static class CacheFileReference implements CacheFile.EvictionListener {

        private final long fileLength;
        private final CacheKey cacheKey;
        private final SearchableSnapshotDirectory directory;
        final AtomicReference<CacheFile> cacheFile = new AtomicReference<>(); // null if evicted or not yet acquired

        CacheFileReference(SearchableSnapshotDirectory directory, String fileName, long fileLength) {
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
}
