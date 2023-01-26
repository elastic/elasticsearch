/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;
import static org.elasticsearch.core.Strings.format;

public class FrozenIndexInput extends MetadataCachingIndexInput {

    private static final Logger logger = LogManager.getLogger(FrozenIndexInput.class);

    private final SharedBlobCacheService<CacheKey>.CacheFile cacheFile;

    public FrozenIndexInput(
        String name,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        int rangeSize,
        int recoveryRangeSize
    ) {
        this(
            name,
            directory,
            fileInfo,
            context,
            stats,
            0L,
            0L,
            fileInfo.length(),
            new CacheFileReference(directory, fileInfo.physicalName(), fileInfo.length()),
            directory.getFrozenCacheFile(name, fileInfo.length()),
            rangeSize,
            recoveryRangeSize,
            directory.getBlobCacheByteRange(name, fileInfo.length()),
            ByteRange.EMPTY
        );
        stats.incrementOpenCount();
    }

    private FrozenIndexInput(
        String name,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long compoundFileOffset,
        long length,
        CacheFileReference cacheFileReference,
        SharedBlobCacheService<CacheKey>.CacheFile cacheFile,
        int defaultRangeSize,
        int recoveryRangeSize,
        ByteRange headerBlobCacheByteRange,
        ByteRange footerBlobCacheByteRange
    ) {
        super(
            logger,
            name,
            directory,
            fileInfo,
            context,
            stats,
            offset,
            compoundFileOffset,
            length,
            cacheFileReference,
            defaultRangeSize,
            recoveryRangeSize,
            headerBlobCacheByteRange,
            footerBlobCacheByteRange
        );
        this.cacheFile = cacheFile;
    }

    @Override
    protected long getDefaultRangeSize() {
        return directory.isRecoveryFinalized() ? defaultRangeSize : recoveryRangeSize;
    }

    @Override
    protected void readWithoutBlobCache(ByteBuffer b) throws Exception {
        final long position = getAbsolutePosition();
        final int length = b.remaining();
        final int originalByteBufPosition = b.position();

        final ReentrantReadWriteLock luceneByteBufLock = new ReentrantReadWriteLock();
        final AtomicBoolean stopAsyncReads = new AtomicBoolean();
        // Runnable that, when called, ensures that async callbacks (such as those used by readCacheFile) are not
        // accessing the byte buffer anymore that was passed to readWithoutBlobCache
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
            final ByteRange startRangeToWrite = computeRange(position);
            final ByteRange endRangeToWrite = computeRange(position + length - 1);
            assert startRangeToWrite.end() <= endRangeToWrite.end() : startRangeToWrite + " vs " + endRangeToWrite;
            final ByteRange rangeToWrite = startRangeToWrite.minEnvelope(endRangeToWrite);

            assert rangeToWrite.start() <= position && position + length <= rangeToWrite.end()
                : "[" + position + "-" + (position + length) + "] vs " + rangeToWrite;
            final ByteRange rangeToRead = ByteRange.of(position, position + length);

            final int bytesRead = cacheFile.populateAndRead(
                rangeToWrite,
                rangeToRead,
                (channel, pos, relativePos, len) -> readCacheFile(
                    channel,
                    pos,
                    relativePos,
                    len,
                    b,
                    rangeToRead.start(),
                    luceneByteBufLock,
                    stopAsyncReads
                ),
                (channel, channelPos, relativePos, len, progressUpdater) -> {
                    final long startTimeNanos = stats.currentTimeNanos();
                    final long streamStartPosition = rangeToWrite.start() + relativePos;

                    try (InputStream input = openInputStreamFromBlobStore(streamStartPosition, len)) {
                        writeCacheFile(channel, input, channelPos, relativePos, len, progressUpdater, startTimeNanos);
                    }
                },
                SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME
            );
            assert bytesRead == length : bytesRead + " vs " + length;
            assert luceneByteBufLock.getReadHoldCount() == 0;

            preventAsyncBufferChanges.run();
            b.position(originalByteBufPosition + bytesRead); // mark all bytes as accounted for
        } finally {
            preventAsyncBufferChanges.run();
        }
    }

    private static int positionalWrite(SharedBytes.IO fc, long start, ByteBuffer byteBuffer) throws IOException {
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        byteBuffer.flip();
        int written = fc.write(byteBuffer, start);
        assert byteBuffer.hasRemaining() == false;
        byteBuffer.clear();
        return written;
    }

    private int readCacheFile(
        final SharedBytes.IO fc,
        long channelPos,
        long relativePos,
        long length,
        final ByteBuffer buffer,
        long logicalPos,
        ReentrantReadWriteLock luceneByteBufLock,
        AtomicBoolean stopAsyncReads
    ) throws IOException {
        logger.trace(
            "{}: reading cached {} logical {} channel {} pos {} length {} (details: {})",
            fileInfo.physicalName(),
            false,
            logicalPos,
            channelPos,
            relativePos,
            length,
            cacheFile
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
                assert newPosition + length <= buffer.limit()
                    : "oldpos " + dup.position() + " newpos " + newPosition + " length " + length + " limit " + buffer.limit();
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
                            this.cacheFile
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

    /**
     * Thread local direct byte buffer to aggregate multiple positional writes to the cache file.
     */
    private static final int MAX_BYTES_PER_WRITE = StrictMath.toIntExact(
        ByteSizeValue.parseBytesSizeValue(
            System.getProperty("es.searchable.snapshot.shared_cache.write_buffer.size", "2m"),
            "es.searchable.snapshot.shared_cache.write_buffer.size"
        ).getBytes()
    );

    private static final ThreadLocal<ByteBuffer> writeBuffer = ThreadLocal.withInitial(
        () -> ByteBuffer.allocateDirect(MAX_BYTES_PER_WRITE)
    );

    private void writeCacheFile(
        final SharedBytes.IO fc,
        final InputStream input,
        final long fileChannelPos,
        final long relativePos,
        final long length,
        final Consumer<Long> progressUpdater,
        final long startTimeNanos
    ) throws IOException {
        assert ThreadPool.assertCurrentThreadPool(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        logger.trace(
            "{}: writing channel {} pos {} length {} (details: {})",
            fileInfo.physicalName(),
            fileChannelPos,
            relativePos,
            length,
            cacheFile
        );
        final long end = relativePos + length;
        final byte[] copyBuffer = new byte[toIntBytes(Math.min(COPY_BUFFER_SIZE, length))];
        logger.trace(() -> format("writing range [%s-%s] to cache file [%s]", relativePos, end, cacheFile));

        long bytesCopied = 0L;
        long remaining = length;
        final ByteBuffer buf = writeBuffer.get();
        buf.clear();
        while (remaining > 0L) {
            final int bytesRead = MetadataCachingIndexInput.readSafe(input, copyBuffer, relativePos, end, remaining, cacheFile);
            if (bytesRead > buf.remaining()) {
                // always fill up buf to the max
                final int bytesToAdd = buf.remaining();
                buf.put(copyBuffer, 0, bytesToAdd);
                assert buf.remaining() == 0;
                long bytesWritten = positionalWrite(fc, fileChannelPos + bytesCopied, buf);
                bytesCopied += bytesWritten;
                progressUpdater.accept(bytesCopied);
                // add the remaining bytes to buf
                buf.put(copyBuffer, bytesToAdd, bytesRead - bytesToAdd);
            } else {
                buf.put(copyBuffer, 0, bytesRead);
            }
            remaining -= bytesRead;
        }
        // ensure that last write is aligned on 4k boundaries (= page size)
        final int remainder = buf.position() % SharedBytes.PAGE_SIZE;
        final int adjustment = remainder == 0 ? 0 : SharedBytes.PAGE_SIZE - remainder;
        buf.position(buf.position() + adjustment);
        long bytesWritten = positionalWrite(fc, fileChannelPos + bytesCopied, buf);
        bytesCopied += bytesWritten;
        final long adjustedBytesCopied = bytesCopied - adjustment; // adjust to not break RangeFileTracker
        assert adjustedBytesCopied == length;
        progressUpdater.accept(adjustedBytesCopied);
        final long endTimeNanos = stats.currentTimeNanos();
        stats.addCachedBytesWritten(adjustedBytesCopied, endTimeNanos - startTimeNanos);
    }

    @Override
    public FrozenIndexInput clone() {
        return (FrozenIndexInput) super.clone();
    }

    @Override
    protected MetadataCachingIndexInput doSlice(
        String sliceName,
        long sliceOffset,
        long sliceLength,
        ByteRange sliceHeaderByteRange,
        ByteRange sliceFooterByteRange,
        long sliceCompoundFileOffset
    ) {
        return new FrozenIndexInput(
            sliceName,
            directory,
            fileInfo,
            context,
            stats,
            this.offset + sliceOffset,
            sliceCompoundFileOffset,
            sliceLength,
            cacheFileReference,
            cacheFile,
            defaultRangeSize,
            recoveryRangeSize,
            sliceHeaderByteRange,
            sliceFooterByteRange
        );
    }

}
