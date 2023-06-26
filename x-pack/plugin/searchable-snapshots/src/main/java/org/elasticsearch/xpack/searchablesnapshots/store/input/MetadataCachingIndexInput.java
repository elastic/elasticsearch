/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.BlobStoreCacheService;
import org.elasticsearch.xpack.searchablesnapshots.cache.blob.CachedBlob;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import static org.elasticsearch.blobcache.BlobCacheUtils.throwEOF;
import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;

/**
 * Searchable snapshots index input that supports fully caching metadata files as well as header/footer information for each file,
 * consisting of a two-level cache (BlobStoreCacheService and CacheService).
 */
public abstract class MetadataCachingIndexInput extends BaseSearchableSnapshotIndexInput {

    protected static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

    protected final CacheFileReference cacheFileReference;

    protected final SearchableSnapshotDirectory directory;

    /**
     * If > 0, represents a logical file within a compound (CFS) file or is a slice thereof represents the offset of the logical
     * compound file within the physical CFS file
     */
    protected final long compoundFileOffset;

    protected final int defaultRangeSize;
    protected final int recoveryRangeSize;

    // last read position is kept around in order to detect (non)contiguous reads for stats
    private long lastReadPosition;
    // last seek position is kept around in order to detect forward/backward seeks for stats
    private long lastSeekPosition;

    /**
     * Range of bytes that should be cached in the blob cache for the current index input's header.
     */
    protected final ByteRange headerBlobCacheByteRange;

    /**
     * Range of bytes that should be cached in the blob cache for the current index input's footer. This footer byte range should only be
     * required for slices of CFS files; regular files already have their footers extracted from the
     * {@link BlobStoreIndexShardSnapshot.FileInfo} (see method {@link BaseSearchableSnapshotIndexInput#maybeReadChecksumFromFileInfo}).
     */
    protected final ByteRange footerBlobCacheByteRange;

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
        super(logger, name, directory.blobContainer(), fileInfo, context, stats, offset, length);
        this.directory = Objects.requireNonNull(directory);
        this.cacheFileReference = cacheFileReference;
        this.compoundFileOffset = compoundFileOffset;
        this.defaultRangeSize = defaultRangeSize;
        this.recoveryRangeSize = recoveryRangeSize;
        this.lastReadPosition = offset;
        this.lastSeekPosition = offset;
        this.headerBlobCacheByteRange = Objects.requireNonNull(headerBlobCacheByteRange);
        this.footerBlobCacheByteRange = Objects.requireNonNull(footerBlobCacheByteRange);
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

    private ByteRange rangeToReadFromBlobCache(long position, int readLength) {
        final long end = position + readLength;
        if (headerBlobCacheByteRange.contains(position, end)) {
            return headerBlobCacheByteRange;
        } else if (footerBlobCacheByteRange.contains(position, end)) {
            return footerBlobCacheByteRange;
        }
        return ByteRange.EMPTY;
    }

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
            final Future<Integer> populateCacheFuture = populateAndRead(b, position, length, cacheFile, blobCacheByteRange);

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

    protected Future<Integer> populateAndRead(ByteBuffer b, long position, int length, CacheFile cacheFile, ByteRange rangeToWrite) {
        final ByteRange rangeToRead = ByteRange.of(position, position + length);
        assert rangeToRead.isSubRangeOf(rangeToWrite) : rangeToRead + " vs " + rangeToWrite;
        assert rangeToRead.length() == b.remaining() : b.remaining() + " vs " + rangeToRead;

        return cacheFile.populateAndRead(
            rangeToWrite,
            rangeToRead,
            channel -> readCacheFile(channel, position, b),
            this::writeCacheFile,
            directory.cacheFetchAsyncExecutor()
        );
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
            throwEOF(position, buffer.remaining(), cacheFileReference);
        }
        stats.addCachedBytesRead(bytesRead);
        return bytesRead;
    }

    protected void writeCacheFile(final FileChannel fc, final long start, final long end, final LongConsumer progressUpdater)
        throws IOException {
        assert assertFileChannelOpen(fc);
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        final long length = end - start;
        final ByteBuffer copyBuffer = writeBuffer.get().clear();
        logger.trace("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference);

        long bytesCopied = 0L;
        long remaining = end - start;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStreamFromBlobStore(start, length)) {
            while (remaining > 0L) {
                final int bytesRead = BlobCacheUtils.readSafe(input, copyBuffer, start, remaining, cacheFileReference);
                positionalWrite(fc, start + bytesCopied, copyBuffer.flip());
                copyBuffer.clear();
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
            directory.putCachedBlob(fileInfo.physicalName(), indexCacheMiss, content, ActionListener.releasing(onCacheFillComplete));
            return indexCacheMissLength;
        });

        if (readFuture == null) {
            // Normally doesn't happen, we're already obtaining a range covering all cache misses above, but theoretically
            // possible in the case that the real populateAndRead call already failed to obtain this range of the file. In that
            // case, simply move on.
            onCacheFillComplete.close();
        }
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
                final int length = b.remaining();
                logger.trace("direct reading of range [{}-{}] for cache file [{}]", position, position + length, cacheFileReference);

                final long startTimeNanos = stats.currentTimeNanos();
                try (InputStream input = openInputStreamFromBlobStore(position, length)) {
                    final int bytesRead = Streams.read(input, b, length);
                    if (bytesRead < length) {
                        throwEOF(position, length - bytesRead, cacheFileReference);
                    }
                    final long endTimeNanos = stats.currentTimeNanos();
                    stats.addDirectBytesRead(bytesRead, endTimeNanos - startTimeNanos);
                    return bytesRead;
                }
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
        }
        throw new IOException("failed to read data from cache", e);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        BlobCacheUtils.ensureSeek(pos, this);
        final long position = pos + this.offset;
        stats.incrementSeeks(lastSeekPosition, position);
        lastSeekPosition = position;
    }

    @Override
    public IndexInput slice(String sliceName, long sliceOffset, long sliceLength) {
        BlobCacheUtils.ensureSlice(sliceName, sliceOffset, sliceLength, this);

        // Are we creating a slice from a CFS file?
        final boolean sliceCompoundFile = IndexFileNames.matchesExtension(name, "cfs")
            && IndexFileNames.getExtension(sliceName) != null
            && compoundFileOffset == 0L // not already a compound file
            && isClone == false; // tests aggressively clone and slice

        final ByteRange sliceHeaderByteRange;
        final ByteRange sliceFooterByteRange;
        final long sliceCompoundFileOffset;

        if (sliceCompoundFile) {
            sliceCompoundFileOffset = this.offset + sliceOffset;
            sliceHeaderByteRange = directory.getBlobCacheByteRange(sliceName, sliceLength).shift(sliceCompoundFileOffset);
            if (sliceHeaderByteRange.isEmpty() == false && sliceHeaderByteRange.length() < sliceLength) {
                sliceFooterByteRange = ByteRange.of(sliceLength - CodecUtil.footerLength(), sliceLength).shift(sliceCompoundFileOffset);
            } else {
                sliceFooterByteRange = ByteRange.EMPTY;
            }
        } else {
            sliceCompoundFileOffset = this.compoundFileOffset;
            sliceHeaderByteRange = ByteRange.EMPTY;
            sliceFooterByteRange = ByteRange.EMPTY;
        }
        final MetadataCachingIndexInput slice = doSlice(
            sliceName,
            sliceOffset,
            sliceLength,
            sliceHeaderByteRange,
            sliceFooterByteRange,
            sliceCompoundFileOffset
        );
        slice.isClone = true;
        return slice;
    }

    protected abstract MetadataCachingIndexInput doSlice(
        String sliceName,
        long sliceOffset,
        long sliceLength,
        ByteRange sliceHeaderByteRange,
        ByteRange sliceFooterByteRange,
        long sliceCompoundFileOffset
    );

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
