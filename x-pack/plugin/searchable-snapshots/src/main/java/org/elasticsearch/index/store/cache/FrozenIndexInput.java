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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.blobstore.cache.BlobStoreCacheService;
import org.elasticsearch.blobstore.cache.CachedBlob;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.store.BaseSearchableSnapshotIndexInput;
import org.elasticsearch.index.store.IndexInputStats;
import org.elasticsearch.index.store.SearchableSnapshotDirectory;
import org.elasticsearch.xpack.searchablesnapshots.cache.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.FrozenCacheFile;
import org.elasticsearch.xpack.searchablesnapshots.cache.SharedBytes;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

public class FrozenIndexInput extends BaseSearchableSnapshotIndexInput {

    public static final IOContext CACHE_WARMING_CONTEXT = new IOContext();

    private static final Logger logger = LogManager.getLogger(FrozenIndexInput.class);
    private static final int COPY_BUFFER_SIZE = ByteSizeUnit.KB.toIntBytes(8);

    private final SearchableSnapshotDirectory directory;
    private final FrozenCacheFile frozenCacheFile;
    private final int defaultRangeSize;
    private final int recoveryRangeSize;

    // last read position is kept around in order to detect (non)contiguous reads for stats
    private long lastReadPosition;
    // last seek position is kept around in order to detect forward/backward seeks for stats
    private long lastSeekPosition;

    public FrozenIndexInput(
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        int rangeSize,
        int recoveryRangeSize
    ) {
        this(
            "FrozenIndexInput(" + fileInfo.physicalName() + ")",
            directory,
            fileInfo,
            context,
            stats,
            0L,
            fileInfo.length(),
            directory.getFrozenCacheFile(fileInfo.physicalName(), fileInfo.length()),
            rangeSize,
            recoveryRangeSize
        );
        assert getBufferSize() <= BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE; // must be able to cache at least one buffer's worth
        stats.incrementOpenCount();
    }

    private FrozenIndexInput(
        String resourceDesc,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long length,
        FrozenCacheFile frozenCacheFile,
        int rangeSize,
        int recoveryRangeSize
    ) {
        super(logger, resourceDesc, directory.blobContainer(), fileInfo, context, stats, offset, length);
        this.directory = directory;
        this.frozenCacheFile = frozenCacheFile;
        this.lastReadPosition = this.offset;
        this.lastSeekPosition = this.offset;
        this.defaultRangeSize = rangeSize;
        this.recoveryRangeSize = recoveryRangeSize;
    }

    @Override
    public void doClose() {
        // nothing needed to be done here
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

        final ReentrantReadWriteLock luceneByteBufLock = new ReentrantReadWriteLock();
        final AtomicBoolean stopAsyncReads = new AtomicBoolean();
        // Runnable that, when called, ensures that async callbacks (such as those used by readCacheFile) are not
        // accessing the byte buffer anymore that was passed to readInternal
        // In particular, it's important to call this method before adapting the ByteBuffer's offset
        final Runnable preventAsyncBufferChanges = () -> {
            luceneByteBufLock.writeLock().lock();
            try {
                stopAsyncReads.set(true);
            } finally {
                luceneByteBufLock.writeLock().unlock();
            }
        };

        logger.trace("readInternal: read [{}-{}] ([{}] bytes) from [{}]", position, position + length, length, this);

        try {
            // Can we serve the read directly from disk? If so, do so and don't worry about anything else.

            final StepListener<Integer> waitingForRead = frozenCacheFile.readIfAvailableOrPending(
                ByteRange.of(position, position + length),
                (channel, pos, relativePos, len) -> {
                    final int read = readCacheFile(channel, pos, relativePos, len, b, position, true, luceneByteBufLock, stopAsyncReads);
                    assert read <= length : read + " vs " + length;
                    return read;
                }
            );

            if (waitingForRead != null) {
                final Integer read = waitingForRead.asFuture().get();
                assert read == length;
                assert luceneByteBufLock.getReadHoldCount() == 0;
                preventAsyncBufferChanges.run();
                b.position(read); // mark all bytes as accounted for
                readComplete(position, length);
                return;
            }

            // Requested data is not on disk, so try the cache index next.

            final ByteRange indexCacheMiss; // null if not a miss

            // We try to use the cache index if:
            // - the file is small enough to be fully cached
            final boolean canBeFullyCached = fileInfo.length() <= BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE * 2;
            // - we're reading the first N bytes of the file
            final boolean isStartOfFile = (position + length <= BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE);

            if (canBeFullyCached || isStartOfFile) {
                final CachedBlob cachedBlob = directory.getCachedBlob(fileInfo.physicalName(), 0L, length);

                if (cachedBlob == CachedBlob.CACHE_MISS || cachedBlob == CachedBlob.CACHE_NOT_READY) {
                    // We would have liked to find a cached entry but we did not find anything: the cache on the disk will be requested
                    // so we compute the region of the file we would like to have the next time. The region is expressed as a tuple of
                    // {start, end} where positions are relative to the whole file.

                    if (canBeFullyCached) {
                        // if the index input is smaller than twice the size of the blob cache, it will be fully indexed
                        indexCacheMiss = ByteRange.of(0L, fileInfo.length());
                    } else {
                        // the index input is too large to fully cache, so just cache the initial range
                        indexCacheMiss = ByteRange.of(0L, (long) BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE);
                    }

                    // We must fill in a cache miss even if CACHE_NOT_READY since the cache index is only created on the first put.
                    // TODO TBD use a different trigger for creating the cache index and avoid a put in the CACHE_NOT_READY case.
                } else {
                    logger.trace(
                        "reading [{}] bytes of file [{}] at position [{}] using cache index",
                        length,
                        fileInfo.physicalName(),
                        position
                    );
                    stats.addIndexCacheBytesRead(cachedBlob.length());

                    preventAsyncBufferChanges.run();

                    final BytesRefIterator cachedBytesIterator = cachedBlob.bytes().slice(toIntBytes(position), length).iterator();
                    int copiedBytes = 0;
                    BytesRef bytesRef;
                    while ((bytesRef = cachedBytesIterator.next()) != null) {
                        b.put(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                        copiedBytes += bytesRef.length;
                    }
                    assert copiedBytes == length : "copied " + copiedBytes + " but expected " + length;

                    try {
                        final ByteRange cachedRange = ByteRange.of(cachedBlob.from(), cachedBlob.to());
                        frozenCacheFile.populateAndRead(
                            cachedRange,
                            cachedRange,
                            (channel, channelPos, relativePos, len) -> Math.toIntExact(len),
                            (channel, channelPos, relativePos, len, progressUpdater) -> {
                                assert len <= cachedBlob.to() - cachedBlob.from();
                                final long startTimeNanos = stats.currentTimeNanos();
                                final BytesRefIterator iterator = cachedBlob.bytes()
                                    .slice(toIntBytes(relativePos), toIntBytes(len))
                                    .iterator();
                                long writePosition = channelPos;
                                long bytesCopied = 0L;
                                BytesRef current;
                                while ((current = iterator.next()) != null) {
                                    final ByteBuffer byteBuffer = ByteBuffer.wrap(current.bytes, current.offset, current.length);
                                    while (byteBuffer.remaining() > 0) {
                                        final long bytesWritten = positionalWrite(channel, writePosition, byteBuffer);
                                        bytesCopied += bytesWritten;
                                        writePosition += bytesWritten;
                                        progressUpdater.accept(bytesCopied);
                                    }
                                }
                                long channelTo = channelPos + len;
                                assert writePosition == channelTo : writePosition + " vs " + channelTo;
                                final long endTimeNanos = stats.currentTimeNanos();
                                stats.addCachedBytesWritten(len, endTimeNanos - startTimeNanos);
                                logger.trace(
                                    "copied bytes [{}-{}] of file [{}] from cache index to disk",
                                    relativePos,
                                    relativePos + len,
                                    fileInfo
                                );
                            },
                            directory.cacheFetchAsyncExecutor()
                        );
                    } catch (Exception e) {
                        logger.debug(
                            new ParameterizedMessage(
                                "failed to store bytes [{}-{}] of file [{}] obtained from index cache",
                                cachedBlob.from(),
                                cachedBlob.to(),
                                fileInfo
                            ),
                            e
                        );
                        // oh well, no big deal, at least we can return them to the caller.
                    }

                    readComplete(position, length);

                    return;
                }
            } else {
                // requested range is not eligible for caching
                indexCacheMiss = null;
            }

            // Requested data is also not in the cache index, so we must visit the blob store to satisfy both the target range and any
            // miss in the cache index.

            final ByteRange startRangeToWrite = computeRange(position);
            final ByteRange endRangeToWrite = computeRange(position + length - 1);
            assert startRangeToWrite.end() <= endRangeToWrite.end() : startRangeToWrite + " vs " + endRangeToWrite;
            final ByteRange rangeToWrite = startRangeToWrite.minEnvelope(endRangeToWrite).minEnvelope(indexCacheMiss);

            assert rangeToWrite.start() <= position && position + length <= rangeToWrite.end() : "["
                + position
                + "-"
                + (position + length)
                + "] vs "
                + rangeToWrite;
            final ByteRange rangeToRead = ByteRange.of(position, position + length);

            final StepListener<Integer> populateCacheFuture = frozenCacheFile.populateAndRead(
                rangeToWrite,
                rangeToRead,
                (channel, pos, relativePos, len) -> readCacheFile(
                    channel,
                    pos,
                    relativePos,
                    len,
                    b,
                    rangeToRead.start(),
                    false,
                    luceneByteBufLock,
                    stopAsyncReads
                ),
                (channel, channelPos, relativePos, len, progressUpdater) -> this.writeCacheFile(
                    channel,
                    channelPos,
                    relativePos,
                    len,
                    rangeToWrite.start(),
                    progressUpdater
                ),
                directory.cacheFetchAsyncExecutor()
            );

            if (indexCacheMiss != null) {
                final Releasable onCacheFillComplete = stats.addIndexCacheFill();
                final int indexCacheMissLength = toIntBytes(indexCacheMiss.length());
                // We assume that we only cache small portions of blobs so that we do not need to:
                // - use a BigArrays for allocation
                // - use an intermediate copy buffer to read the file in sensibly-sized chunks
                // - release the buffer once the indexing operation is complete
                assert indexCacheMissLength <= COPY_BUFFER_SIZE : indexCacheMiss;

                final ByteBuffer byteBuffer = ByteBuffer.allocate(indexCacheMissLength);

                final StepListener<Integer> readListener = frozenCacheFile.readIfAvailableOrPending(
                    indexCacheMiss,
                    (channel, channelPos, relativePos, len) -> {
                        assert len <= indexCacheMissLength;

                        if (len == 0) {
                            return 0;
                        }

                        // create slice that is positioned to read the given values
                        final ByteBuffer dup = byteBuffer.duplicate();
                        final int newPosition = dup.position() + Math.toIntExact(relativePos);
                        assert newPosition <= dup.limit() : "newpos " + newPosition + " limit " + dup.limit();
                        assert newPosition + len <= byteBuffer.limit();
                        dup.position(newPosition);
                        dup.limit(newPosition + Math.toIntExact(len));

                        final int read = channel.read(dup, channelPos);
                        if (read < 0) {
                            throw new EOFException("read past EOF. pos [" + relativePos + "] length: [" + len + "]");
                        }
                        // NB use Channels.readFromFileChannelWithEofException not readCacheFile() to avoid counting this in the stats
                        assert read == len;
                        return read;
                    }
                );

                if (readListener == null) {
                    // Normally doesn't happen, we're already obtaining a range covering all cache misses above, but theoretically
                    // possible in the case that the real populateAndRead call already failed to obtain this range of the file. In that
                    // case, simply move on.
                    onCacheFillComplete.close();
                } else {
                    readListener.whenComplete(read -> {
                        assert read == indexCacheMissLength;
                        byteBuffer.position(read); // mark all bytes as accounted for
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
                    }, e -> onCacheFillComplete.close());
                }
            }

            final int bytesRead = populateCacheFuture.asFuture().get();
            assert bytesRead == length : bytesRead + " vs " + length;
            assert luceneByteBufLock.getReadHoldCount() == 0;

            preventAsyncBufferChanges.run();
            b.position(bytesRead); // mark all bytes as accounted for
        } catch (final Exception e) {
            preventAsyncBufferChanges.run();

            // may have partially filled the buffer before the exception was thrown, so try and get the remainder directly.
            final int alreadyRead = length - b.remaining();
            final int bytesRead = readDirectlyIfAlreadyClosed(position + alreadyRead, b, e);
            assert alreadyRead + bytesRead == length : alreadyRead + " + " + bytesRead + " vs " + length;

            // In principle we could handle an index cache miss here too, ensuring that the direct read was large enough, but this is
            // already a rare case caused by an overfull/undersized cache.
        }

        readComplete(position, length);
    }

    private void readComplete(long position, int length) {
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
                        frozenCacheFile
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
                                    frozenCacheFile
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

    private static int positionalWrite(SharedBytes.IO fc, long start, ByteBuffer byteBuffer) throws IOException {
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
        FrozenCacheFile frozenCacheFile
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
                    frozenCacheFile
                )
            );
        }
        assert bytesRead > 0 : bytesRead;
        return bytesRead;
    }

    private int readCacheFile(
        final SharedBytes.IO fc,
        long channelPos,
        long relativePos,
        long length,
        final ByteBuffer buffer,
        long logicalPos,
        boolean cached,
        ReentrantReadWriteLock luceneByteBufLock,
        AtomicBoolean stopAsyncReads
    ) throws IOException {
        logger.trace(
            "{}: reading cached {} logical {} channel {} pos {} length {} (details: {})",
            fileInfo.physicalName(),
            cached,
            logicalPos,
            channelPos,
            relativePos,
            length,
            frozenCacheFile
        );
        if (length == 0L) {
            return 0;
        }
        final int bytesRead;
        if (luceneByteBufLock.readLock().tryLock()) {
            try {
                boolean shouldStopReading = stopAsyncReads.get();
                if (shouldStopReading) {
                    // return fake response
                    return Math.toIntExact(length);
                }
                // create slice that is positioned to read the given values
                final ByteBuffer dup = buffer.duplicate();
                final int newPosition = dup.position() + Math.toIntExact(relativePos);
                assert newPosition <= dup.limit() : "newpos " + newPosition + " limit " + dup.limit();
                assert newPosition + length <= buffer.limit() : "oldpos "
                    + dup.position()
                    + " newpos "
                    + newPosition
                    + " length "
                    + length
                    + " limit "
                    + buffer.limit();
                dup.position(newPosition);
                dup.limit(newPosition + Math.toIntExact(length));
                bytesRead = fc.read(dup, channelPos);
                if (bytesRead == -1) {
                    throw new EOFException(
                        String.format(
                            Locale.ROOT,
                            "unexpected EOF reading [%d-%d] from %s",
                            channelPos,
                            channelPos + dup.remaining(),
                            this.frozenCacheFile
                        )
                    );
                }
            } finally {
                luceneByteBufLock.readLock().unlock();
            }
        } else {
            // return fake response
            return Math.toIntExact(length);
        }
        stats.addCachedBytesRead(bytesRead);
        return bytesRead;
    }

    private void writeCacheFile(
        final SharedBytes.IO fc,
        long fileChannelPos,
        long relativePos,
        long length,
        long logicalPos,
        final Consumer<Long> progressUpdater
    ) throws IOException {
        assert assertCurrentThreadMayWriteCacheFile();
        logger.trace(
            "{}: writing logical {} channel {} pos {} length {} (details: {})",
            fileInfo.physicalName(),
            logicalPos,
            fileChannelPos,
            relativePos,
            length,
            frozenCacheFile
        );
        final long end = relativePos + length;
        final byte[] copyBuffer = new byte[toIntBytes(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> new ParameterizedMessage("writing range [{}-{}] to cache file [{}]", relativePos, end, frozenCacheFile));

        long bytesCopied = 0L;
        long remaining = length;
        final long startTimeNanos = stats.currentTimeNanos();
        try (InputStream input = openInputStreamFromBlobStore(logicalPos + relativePos, length)) {
            while (remaining > 0L) {
                final int bytesRead = readSafe(input, copyBuffer, relativePos, end, remaining, frozenCacheFile);
                positionalWrite(fc, fileChannelPos + bytesCopied, ByteBuffer.wrap(copyBuffer, 0, bytesRead));
                bytesCopied += bytesRead;
                remaining -= bytesRead;
                progressUpdater.accept(bytesCopied);
            }
            final long endTimeNanos = stats.currentTimeNanos();
            assert bytesCopied == length;
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
    public FrozenIndexInput clone() {
        return (FrozenIndexInput) super.clone();
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
        final FrozenIndexInput slice = new FrozenIndexInput(
            getFullSliceDescription(sliceDescription),
            directory,
            fileInfo,
            context,
            stats,
            this.offset + offset,
            length,
            frozenCacheFile,
            defaultRangeSize,
            recoveryRangeSize
        );
        slice.isClone = true;
        return slice;
    }

    @Override
    public String toString() {
        return "CachedBlobContainerIndexInput{"
            + "sharedCacheFile="
            + frozenCacheFile
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

    private static boolean assertCurrentThreadMayWriteCacheFile() {
        final String threadName = Thread.currentThread().getName();
        assert isCacheFetchAsyncThread(threadName) : "expected the current thread ["
            + threadName
            + "] to belong to the cache fetch async thread pool";
        return true;
    }
}
