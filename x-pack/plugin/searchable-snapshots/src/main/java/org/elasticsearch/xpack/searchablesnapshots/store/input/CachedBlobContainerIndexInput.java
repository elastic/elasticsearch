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
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheFile;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.elasticsearch.blobcache.BlobCacheUtils.readSafe;
import static org.elasticsearch.blobcache.BlobCacheUtils.toIntBytes;

public class CachedBlobContainerIndexInput extends MetadataCachingIndexInput {

    /**
     * Specific IOContext used for prewarming the cache. This context allows to write
     * a complete part of the {@link #fileInfo} at once in the cache and should not be
     * used for anything else than what the {@link #prefetchPart(int, Supplier)} method does.
     */
    public static final IOContext CACHE_WARMING_CONTEXT = new IOContext();

    private static final Logger logger = LogManager.getLogger(CachedBlobContainerIndexInput.class);

    public CachedBlobContainerIndexInput(
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
            rangeSize,
            recoveryRangeSize,
            directory.getBlobCacheByteRange(name, fileInfo.length()),
            ByteRange.EMPTY
        );
        stats.incrementOpenCount();
    }

    private CachedBlobContainerIndexInput(
        String name,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
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
    }

    @Override
    protected void readWithoutBlobCache(ByteBuffer b) throws Exception {
        ensureContext(ctx -> ctx != CACHE_WARMING_CONTEXT);
        final long position = getAbsolutePosition();
        final int length = b.remaining();

        final CacheFile cacheFile = cacheFileReference.get();

        final ByteRange rangeToWrite = BlobCacheUtils.computeRange(
            directory.isRecoveryFinalized() ? defaultRangeSize : recoveryRangeSize,
            position,
            length,
            fileInfo.length()
        );
        final ByteRange rangeToRead = ByteRange.of(position, position + length);
        assert rangeToRead.isSubRangeOf(rangeToWrite) : rangeToRead + " vs " + rangeToWrite;
        assert rangeToRead.length() == b.remaining() : b.remaining() + " vs " + rangeToRead;

        final Future<Integer> populateCacheFuture = populateAndRead(b, position, length, cacheFile, rangeToWrite);
        final int bytesRead = populateCacheFuture.get();
        assert bytesRead == length : bytesRead + " vs " + length;
    }

    /**
     * @return Returns the number of bytes already cached for the file in the cold persistent cache
     */
    public long getPersistentCacheInitialLength() throws Exception {
        return cacheFileReference.get().getInitialLength();
    }

    /**
     * Prefetches a complete part and writes it in cache. This method is used to prewarm the cache.
     *
     * @param part the index of the part to prewarm
     * @param isCancelled a {@link Supplier<Boolean>} that allows to check if prewarming must be cancelled
     *
     * @return the number of bytes that has been read from the blob store when prewarming the part,
     * or {@code -1} if the prewarming was cancelled
     */
    public long prefetchPart(final int part, Supplier<Boolean> isCancelled) throws IOException {
        ensureContext(ctx -> ctx == CACHE_WARMING_CONTEXT);
        if (part >= fileInfo.numberOfParts()) {
            throw new IllegalArgumentException("Unexpected part number [" + part + "]");
        }
        if (isCancelled.get()) {
            return -1L;
        }
        final ByteRange partRange;
        if (fileInfo.numberOfParts() == 1) {
            partRange = ByteRange.of(0, fileInfo.length());
        } else {
            long rangeSize = fileInfo.partSize().getBytes();
            long rangeStart = (IntStream.range(0, part).mapToLong(fileInfo::partBytes).sum() / rangeSize) * rangeSize;
            partRange = ByteRange.of(rangeStart, Math.min(rangeStart + rangeSize, fileInfo.length()));
        }
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
                return 0L;
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
            final ByteBuffer copyBuffer = writeBuffer.get();
            long totalBytesRead = 0L;
            final AtomicLong totalBytesWritten = new AtomicLong();
            long remainingBytes = range.length();
            final long startTimeNanos = stats.currentTimeNanos();
            try (InputStream input = openInputStreamFromBlobStore(range.start(), range.length())) {
                while (remainingBytes > 0L) {
                    assert totalBytesRead + remainingBytes == range.length();
                    copyBuffer.clear();
                    if (isCancelled.get()) {
                        return -1L;
                    }
                    final int bytesRead = readSafe(input, copyBuffer, range.start(), remainingBytes, cacheFileReference);
                    // The range to prewarm in cache
                    final long readStart = range.start() + totalBytesRead;
                    final ByteRange rangeToWrite = ByteRange.of(readStart, readStart + bytesRead);

                    // We do not actually read anything, but we want to wait for the write to complete before proceeding.
                    // noinspection UnnecessaryLocalVariable
                    final ByteRange rangeToRead = rangeToWrite;
                    cacheFile.populateAndRead(rangeToWrite, rangeToRead, (channel) -> bytesRead, (channel, start, end, progressUpdater) -> {
                        final int writtenBytes = positionalWrite(
                            channel,
                            start,
                            copyBuffer.slice(toIntBytes(start - readStart), toIntBytes(end - start))
                        );
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
            return totalBytesRead;
        } catch (final Exception e) {
            throw new IOException("Failed to prefetch file part in cache", e);
        }
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
            assert range.end() % length == 0L || (range.end() == fileInfo.length())
                : "end of range [" + range.end() + "] is not aligned with part end or with file length";
        }
        return true;
    }

    @Override
    public CachedBlobContainerIndexInput clone() {
        return (CachedBlobContainerIndexInput) super.clone();
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
        return new CachedBlobContainerIndexInput(
            sliceName,
            directory,
            fileInfo,
            context,
            stats,
            this.offset + sliceOffset,
            sliceCompoundFileOffset,
            sliceLength,
            cacheFileReference,
            defaultRangeSize,
            recoveryRangeSize,
            sliceHeaderByteRange,
            sliceFooterByteRange
        );
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
}
