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
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.BlobCacheBufferedIndexInput;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.blobcache.BlobCacheUtils.throwEOF;
import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.blobcache.shared.SharedBytes.MAX_BYTES_PER_WRITE;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.searchablesnapshots.store.input.ChecksumBlobContainerIndexInput.checksumToBytesArray;

/**
 * Searchable snapshots index input that supports fully caching metadata files as well as header/footer information for each file,
 * consisting of a two-level cache (BlobStoreCacheService and CacheService).
 */
public abstract class MetadataCachingIndexInput extends BlobCacheBufferedIndexInput {

    protected static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

    protected final CacheFileReference cacheFileReference;

    protected final SearchableSnapshotDirectory directory;

    /**
     * If > 0, represents a logical file within a compound (CFS) file or is a slice thereof represents the offset of the logical
     * compound file within the physical CFS file
     */
    private final long compoundFileOffset;

    protected final int defaultRangeSize;
    protected final int recoveryRangeSize;

    // last read position is kept around in order to detect (non)contiguous reads for stats
    private long lastReadPosition;
    // last seek position is kept around in order to detect forward/backward seeks for stats
    private long lastSeekPosition;

    /**
     * Range of bytes that should be cached in the blob cache for the current index input's header.
     */
    private final ByteRange headerBlobCacheByteRange;

    /**
     * Range of bytes that should be cached in the blob cache for the current index input's footer. This footer byte range should only be
     * required for slices of CFS files; regular files already have their footers extracted from the
     * {@link BlobStoreIndexShardSnapshot.FileInfo} (see method {@link #maybeReadChecksumFromFileInfo}).
     */
    private final ByteRange footerBlobCacheByteRange;

    private final Logger logger;
    private final boolean isCfs;
    protected final BlobStoreIndexShardSnapshot.FileInfo fileInfo;
    protected final IOContext context;
    protected final IndexInputStats stats;
    private final long offset;

    // the following are only mutable so they can be adjusted after cloning/slicing
    private volatile boolean isClone;
    private AtomicBoolean closed;

    protected MetadataCachingIndexInput(
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
        super(name, context, length);
        this.isCfs = IndexFileNames.matchesExtension(name, "cfs");
        this.logger = Objects.requireNonNull(logger);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.context = Objects.requireNonNull(context);
        assert fileInfo.metadata().hashEqualsContents() == false
            : "this method should only be used with blobs that are NOT stored in metadata's hash field " + "(fileInfo: " + fileInfo + ')';
        this.stats = Objects.requireNonNull(stats);
        this.offset = offset;
        this.closed = new AtomicBoolean(false);
        this.isClone = false;
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

    /**
     * Clone constructor, will mark this as cloned.
     */
    protected MetadataCachingIndexInput(MetadataCachingIndexInput input) {
        this(
            input.logger,
            "(clone of) " + input,
            input.directory,
            input.fileInfo,
            input.context,
            input.stats,
            input.offset,
            input.compoundFileOffset,
            input.length(),
            input.cacheFileReference,
            input.defaultRangeSize,
            input.recoveryRangeSize,
            input.headerBlobCacheByteRange,
            input.footerBlobCacheByteRange
        );
        this.isClone = true;
        try {
            seek(input.getFilePointer());
        } catch (IOException e) {
            assert false : e;
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Detects read operations that are executed on the last 16 bytes of the index input which is where Lucene stores the footer checksum
     * of Lucene files. If such a read is detected this method tries to complete the read operation by reading the checksum from the
     * {@link BlobStoreIndexShardSnapshot.FileInfo} in memory rather than reading the bytes from the {@link BufferedIndexInput} because
     * that could trigger more cache operations.
     *
     * @return true if the footer checksum has been read from the {@link BlobStoreIndexShardSnapshot.FileInfo}
     */
    public static boolean maybeReadChecksumFromFileInfo(
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        long absolutePosition,
        boolean isClone,
        ByteBuffer b
    ) throws IOException {
        final int remaining = b.remaining();
        if (remaining > CodecUtil.footerLength()) {
            return false;
        }
        final long checksumPosition = fileInfo.length() - CodecUtil.footerLength();
        if (absolutePosition < checksumPosition) {
            return false;
        }
        if (isClone) {
            return false;
        }
        boolean success = false;
        try {
            final int checksumOffset = toIntBytes(Math.subtractExact(absolutePosition, checksumPosition));
            assert checksumOffset <= CodecUtil.footerLength() : checksumOffset;
            assert 0 <= checksumOffset : checksumOffset;

            final byte[] checksum = checksumToBytesArray(fileInfo.checksum());
            b.put(checksum, checksumOffset, remaining);
            success = true;
        } catch (NumberFormatException e) {
            // tests disable this optimisation by passing an invalid checksum
        } finally {
            assert b.remaining() == (success ? 0L : remaining) : b.remaining() + " remaining bytes but success is " + success;
        }
        return success;
    }

    public static boolean assertCurrentThreadMayAccessBlobStore() {
        return ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SNAPSHOT,
            ThreadPool.Names.GENERIC,
            ThreadPool.Names.SEARCH,
            ThreadPool.Names.SEARCH_THROTTLED,

            // Cache asynchronous fetching runs on a dedicated thread pool.
            SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME,

            // Cache prewarming also runs on a dedicated thread pool.
            SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME
        );
    }

    public static boolean assertCurrentThreadIsNotCacheFetchAsync() {
        final String threadName = Thread.currentThread().getName();
        assert false == threadName.contains('[' + SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME + ']')
            : "expected the current thread [" + threadName + "] to belong to the cache fetch async thread pool";
        return true;
    }

    protected long getAbsolutePosition() {
        final long position = getFilePointer() + this.offset;
        assert position >= 0L : "absolute position is negative: " + position;
        assert position <= fileInfo.length() : position + " vs " + fileInfo.length();
        return position;
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
            // yes, serving directly from disk because we had the bytes on disk already or had an ongoing download running for them
            final Integer read = waitingForRead.get();
            assert read == length;
            return;
        }

        // we did not find the bytes on disk, try the cache index before falling back to the blob store
        final CachedBlob cachedBlob = directory.getCachedBlob(fileInfo.physicalName(), blobCacheByteRange);
        if (cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY) {
            fillBlobCacheIndex(b, blobCacheByteRange, position, cacheFile);
        } else {
            readAndCopyFromBlobCacheIndex(b, position, cacheFile, cachedBlob);
        }
    }

    /**
     * Fills {@code b} starting at {@code position} the same way {@link #readAndCopyFromBlobCacheIndex} does but reads directly from the
     * blob store.
     * Caches the requested {@code blobCacheByteRange} containing {@code position} from the blob-store and stores it in the internal cache
     * index as well as writes it to local disk into a {@link CacheFile}. This is used for ranges that should be stored in the internal
     * cache index but haven't been found during a read attempt.
     *
     * @param b buffer to fill with the request byte range
     * @param blobCacheByteRange byte range to cache in the internal index
     * @param position position in the file to start reading from
     * @param cacheFile cache file for this operation
     * @throws Exception on failure
     */
    private void fillBlobCacheIndex(ByteBuffer b, ByteRange blobCacheByteRange, long position, CacheFile cacheFile) throws Exception {
        // We would have liked to find a cached entry, but we did not find anything: the cache on the disk will be requested,
        // so we compute the region of the file we would like to have the next time. The region is expressed as a tuple of
        // {start, end} where positions are relative to the whole file.
        final int length = b.remaining();
        // We must fill in a cache miss even if CACHE_NOT_READY since the cache index is only created on the first put.
        // TODO TBD use a different trigger for creating the cache index and avoid a put in the CACHE_NOT_READY case.
        final Future<Integer> populateCacheFuture = populateAndRead(b, position, cacheFile, blobCacheByteRange);

        fillIndexCache(cacheFile, blobCacheByteRange);
        if (compoundFileOffset > 0L && blobCacheByteRange.equals(headerBlobCacheByteRange) && footerBlobCacheByteRange.isEmpty() == false) {
            fillIndexCache(cacheFile, footerBlobCacheByteRange);
        }

        final int bytesRead = populateCacheFuture.get();
        assert bytesRead == length : bytesRead + " vs " + length;
    }

    /**
     * Copies a byte range found in the internal cache index to a local {@link CacheFile} and reads the section starting from
     * {@code position} into {@code b}, reading as many bytes as are remaining in {code b}. This is used whenever a range that should be
     * stored in a local cache file wasn't found in the cache file but could be read from the cache index without having to reach out to
     * the blob store itself.
     *
     * @param b buffer to fill with the request byte range
     * @param position position in the file to start reading from
     * @param cacheFile cache file for this operation
     * @param cachedBlob cached blob found in the internal cache index
     * @throws IOException on failure
     */
    private void readAndCopyFromBlobCacheIndex(ByteBuffer b, long position, CacheFile cacheFile, CachedBlob cachedBlob) throws IOException {
        final int length = b.remaining();
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

        copyToCacheFile(cacheFile, cachedBlob);
    }

    /**
     * Copy a {@code cachedBlob} from internal cache index to the given {@code cacheFile}.
     * @param cacheFile file to copy to
     * @param cachedBlob cache blob to copy from
     */
    private void copyToCacheFile(CacheFile cacheFile, CachedBlob cachedBlob) {
        try {
            final ByteRange cachedRange = ByteRange.of(cachedBlob.from(), cachedBlob.to());
            cacheFile.populateAndRead(cachedRange, cachedRange, channel -> cachedBlob.length(), (channel, from, to, progressUpdater) -> {
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
            }, directory.cacheFetchAsyncExecutor());
        } catch (Exception e) {
            // ignore exceptions during copying, we already read the bytes on another thread so failing to copy to local disk does not
            // break anything functionally
            logger.debug(
                () -> format(
                    "failed to store bytes [%s-%s] of file [%s] obtained from index cache",
                    cachedBlob.from(),
                    cachedBlob.to(),
                    fileInfo
                ),
                e
            );
        }
    }

    /**
     * Read from given {@code cacheFile} into {@code b}, starting at the given {@code position} and reading as many bytes as are remaining
     * {@code b} while storing {@code rangeToWrite} in the cache file.
     *
     * @param b buffer to read into
     * @param position position in the file to start reading from
     * @param cacheFile file to read from
     * @param rangeToWrite range to read from the blob store and store in the cache file
     * @return future that resolves to the number of bytes read
     */
    protected Future<Integer> populateAndRead(ByteBuffer b, long position, CacheFile cacheFile, ByteRange rangeToWrite) {
        final ByteRange rangeToRead = ByteRange.of(position, position + b.remaining());
        assert rangeToRead.isSubRangeOf(rangeToWrite) : rangeToRead + " vs " + rangeToWrite;

        return cacheFile.populateAndRead(
            rangeToWrite,
            rangeToRead,
            channel -> readCacheFile(channel, position, b),
            (fc, start, end, progressUpdater) -> {
                assert assertFileChannelOpen(fc);
                assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
                final ByteBuffer copyBuffer = writeBuffer.get().clear();
                logger.trace("writing range [{}-{}] to cache file [{}]", start, end, cacheFileReference);

                long bytesCopied = 0L;
                long remaining = end - start;
                final long startTimeNanos = stats.currentTimeNanos();
                try (InputStream input = openInputStreamFromBlobStore(start, end - start)) {
                    while (remaining > 0L) {
                        final int bytesRead = BlobCacheUtils.readSafe(input, copyBuffer, start, remaining);
                        positionalWrite(fc, start + bytesCopied, copyBuffer.flip());
                        copyBuffer.clear();
                        bytesCopied += bytesRead;
                        remaining -= bytesRead;
                        progressUpdater.accept(start + bytesCopied);
                    }
                    final long endTimeNanos = stats.currentTimeNanos();
                    stats.addCachedBytesWritten(bytesCopied, endTimeNanos - startTimeNanos);
                }
            },
            directory.cacheFetchAsyncExecutor()
        );
    }

    private void readComplete(long position, int length) {
        stats.incrementBytesRead(lastReadPosition, position, length);
        lastReadPosition = position + length;
        lastSeekPosition = lastReadPosition;
    }

    private int readCacheFile(final FileChannel fc, final long position, final ByteBuffer buffer) throws IOException {
        assert assertFileChannelOpen(fc);
        final int bytesRead = Channels.readFromFileChannel(fc, position, buffer);
        if (bytesRead == -1) {
            throwEOF(position, buffer.remaining());
        }
        stats.addCachedBytesRead(bytesRead);
        return bytesRead;
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

    private static boolean assertFileChannelOpen(FileChannel fileChannel) {
        assert fileChannel != null;
        assert fileChannel.isOpen();
        return true;
    }

    @SuppressForbidden(reason = "Use positional writes on purpose")
    protected static int positionalWrite(FileChannel fc, long start, ByteBuffer byteBuffer) throws IOException {
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        return fc.write(byteBuffer, start);
    }

    private int readDirectlyIfAlreadyClosed(long position, ByteBuffer b, Exception e) throws IOException {
        if (e instanceof AlreadyClosedException || (e.getCause() != null && e.getCause() instanceof AlreadyClosedException)) {
            try {
                // cache file was evicted during the range fetching, read bytes directly from blob container
                final int length = b.remaining();
                logger.trace("direct reading of range [{}-{}] for cache file [{}]", position, position + length, cacheFileReference);

                final long startTimeNanos = stats.currentTimeNanos();
                try (InputStream input = openInputStreamFromBlobStore(position, length)) {
                    final int bytesRead = Streams.read(input, b, length);
                    if (bytesRead < length) {
                        throwEOF(position, length - bytesRead);
                    }
                    final long endTimeNanos = stats.currentTimeNanos();
                    stats.addDirectBytesRead(bytesRead, endTimeNanos - startTimeNanos);
                    return bytesRead;
                }
            } catch (Exception inner) {
                e.addSuppressed(inner);
            }
        }
        throw new IOException("failed to read data from cache for [" + cacheFileReference + "]", e);
    }

    /**
     * Opens an {@link InputStream} for the given range of bytes which reads the data directly from the blob store. If the requested range
     * spans multiple blobs then this stream will request them in turn using.
     *
     * @param position The start of the range of bytes to read, relative to the start of the corresponding Lucene file.
     * @param readLength The number of bytes to read
     */
    protected InputStream openInputStreamFromBlobStore(final long position, final long readLength) throws IOException {
        assert assertCurrentThreadMayAccessBlobStore();
        if (fileInfo.numberOfParts() == 1L) {
            assert position + readLength <= fileInfo.length()
                : "cannot read [" + position + "-" + (position + readLength) + "] from [" + fileInfo + "]";
            stats.addBlobStoreBytesRequested(readLength);
            return directory.blobContainer().readBlob(OperationPurpose.SNAPSHOT_DATA, fileInfo.name(), position, readLength);
        }
        return openInputStreamMultipleParts(position, readLength);
    }

    /**
     * Used by {@link #openInputStreamFromBlobStore} when reading a range that is split across multiple file parts/blobs.
     * See {@link BlobStoreRepository#chunkSize()}.
     */
    private SlicedInputStream openInputStreamMultipleParts(long position, long readLength) {
        final int startPart = getPartNumberForPosition(position);
        final int endPart = getPartNumberForPosition(position + readLength - 1);
        return new SlicedInputStream(endPart - startPart + 1) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                final int currentPart = startPart + slice;
                final long startInPart = (currentPart == startPart) ? getRelativePositionInPart(position) : 0L;
                final long endInPart;
                endInPart = currentPart == endPart
                    ? getRelativePositionInPart(position + readLength - 1) + 1
                    : fileInfo.partBytes(currentPart);
                final long length = endInPart - startInPart;
                stats.addBlobStoreBytesRequested(length);
                return directory.blobContainer()
                    .readBlob(OperationPurpose.SNAPSHOT_DATA, fileInfo.partName(currentPart), startInPart, length);
            }

            @Override
            public boolean markSupported() {
                return false;
            }
        };
    }

    /**
     * Compute the part number that contains the byte at the given position in the corresponding Lucene file.
     */
    private int getPartNumberForPosition(long position) {
        ensureValidPosition(position);
        final int part = fileInfo.numberOfParts() == 1 ? 0 : Math.toIntExact(position / fileInfo.partSize().getBytes());
        assert part <= fileInfo.numberOfParts() : "part number [" + part + "] exceeds number of parts: " + fileInfo.numberOfParts();
        assert part >= 0 : "part number [" + part + "] is negative";
        return part;
    }

    /**
     * Compute the position of the given byte relative to the start of its part.
     * @param position the position of the required byte (within the corresponding Lucene file)
     */
    private long getRelativePositionInPart(long position) {
        ensureValidPosition(position);
        final long pos = position % fileInfo.partSize().getBytes();
        assert pos < fileInfo.partBytes(getPartNumberForPosition(pos)) : "position in part [" + pos + "] exceeds part's length";
        assert pos >= 0L : "position in part [" + pos + "] is negative";
        return pos;
    }

    private void ensureValidPosition(long position) {
        assert position >= 0L && position < fileInfo.length() : position + " vs " + fileInfo.length();
        // noinspection ConstantConditions in case assertions are disabled
        if (position < 0L || position >= fileInfo.length()) {
            throw new IllegalArgumentException("Position [" + position + "] is invalid for a file of length [" + fileInfo.length() + "]");
        }
    }

    @Override
    protected final void readInternal(ByteBuffer b) throws IOException {
        assert assertCurrentThreadIsNotCacheFetchAsync();

        final int bytesToRead = b.remaining();
        // We can detect that we're going to read the last 16 bytes (that contains the footer checksum) of the file. Such reads are often
        // executed when opening a Directory and since we have the checksum in the snapshot metadata we can use it to fill the ByteBuffer.
        if (maybeReadChecksumFromFileInfo(fileInfo, getAbsolutePosition(), isClone, b)) {
            logger.trace("read footer of file [{}], bypassing all caches", fileInfo.physicalName());
        } else {
            final long position = getAbsolutePosition();
            if (logger.isTraceEnabled()) {
                logger.trace("readInternal: read [{}-{}] ([{}] bytes) from [{}]", position, position + bytesToRead, bytesToRead, this);
            }

            try {
                final ByteRange blobCacheByteRange = rangeToReadFromBlobCache(position, bytesToRead);
                if (blobCacheByteRange.isEmpty()) {
                    readWithoutBlobCache(b);
                } else {
                    readWithBlobCache(b, blobCacheByteRange);
                }
            } catch (final Exception e) {
                // may have partially filled the buffer before the exception was thrown, so try and get the remainder directly.
                final int alreadyRead = bytesToRead - b.remaining();
                final int bytesRead = readDirectlyIfAlreadyClosed(position + alreadyRead, b, e);
                assert alreadyRead + bytesRead == bytesToRead : alreadyRead + " + " + bytesRead + " vs " + bytesToRead;
            }

            readComplete(position, bytesToRead);
        }
        assert b.remaining() == 0L : b.remaining();
        stats.addLuceneBytesRead(bytesToRead);
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        BlobCacheUtils.ensureSeek(pos, this);
        final long position = pos + this.offset;
        stats.incrementSeeks(lastSeekPosition, position);
        lastSeekPosition = position;
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true) && isClone == false) {
            stats.incrementCloseCount();
            cacheFileReference.releaseOnClose();
        }
    }

    @Override
    public IndexInput clone() {
        var bufferClone = tryCloneBuffer();
        if (bufferClone != null) {
            return bufferClone;
        }
        final MetadataCachingIndexInput clone = (MetadataCachingIndexInput) super.clone();
        clone.closed = new AtomicBoolean(false);
        clone.isClone = true;
        return clone;
    }

    @Override
    public String toString() {
        return super.toString() + "[length=" + length() + ", file pointer=" + getFilePointer() + ", offset=" + offset + ']';
    }

    @Override
    protected String getFullSliceDescription(String sliceDescription) {
        final String resourceDesc = super.toString();
        if (sliceDescription != null) {
            return "slice(" + sliceDescription + ") of " + resourceDesc;
        }
        return resourceDesc;
    }

    @Override
    public IndexInput slice(String sliceName, long sliceOffset, long sliceLength) {
        var bufferSlice = trySliceBuffer(sliceName, sliceOffset, sliceLength);
        if (bufferSlice != null) {
            return bufferSlice;
        }
        BlobCacheUtils.ensureSlice(sliceName, sliceOffset, sliceLength, this);

        // Are we creating a slice from a CFS file?
        final boolean sliceCompoundFile = isCfs
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
            this.offset + sliceOffset,
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
