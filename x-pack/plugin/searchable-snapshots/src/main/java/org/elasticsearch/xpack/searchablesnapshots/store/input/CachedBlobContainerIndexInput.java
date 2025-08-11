/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.store.input;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
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

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

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
    protected long getDefaultRangeSize() {
        return (context != CACHE_WARMING_CONTEXT)
            ? (directory.isRecoveryFinalized() ? defaultRangeSize : recoveryRangeSize)
            : fileInfo.partSize().getBytes();
    }

    @Override
    protected void readWithoutBlobCache(ByteBuffer b) throws Exception {
        ensureContext(ctx -> ctx != CACHE_WARMING_CONTEXT);
        final long position = getAbsolutePosition();
        final int length = b.remaining();

        final CacheFile cacheFile = cacheFileReference.get();

        final ByteRange startRangeToWrite = computeRange(position);
        final ByteRange endRangeToWrite = computeRange(position + length - 1);
        assert startRangeToWrite.end() <= endRangeToWrite.end() : startRangeToWrite + " vs " + endRangeToWrite;
        final ByteRange rangeToWrite = startRangeToWrite.minEnvelope(endRangeToWrite);

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

        final int bytesRead = populateCacheFuture.get();
        assert bytesRead == length : bytesRead + " vs " + length;
    }

    /**
     * Prefetches a complete part and writes it in cache. This method is used to prewarm the cache.
     * @return a tuple with {@code Tuple<Persistent Cache Length, Prefetched Length>} values
     */
    public Tuple<Long, Long> prefetchPart(final int part, Supplier<Boolean> isCancelled) throws IOException {
        ensureContext(ctx -> ctx == CACHE_WARMING_CONTEXT);
        if (part >= fileInfo.numberOfParts()) {
            throw new IllegalArgumentException("Unexpected part number [" + part + "]");
        }
        if (isCancelled.get()) {
            return Tuple.tuple(0L, 0L);
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
                    if (isCancelled.get()) {
                        return Tuple.tuple(cacheFile.getInitialLength(), totalBytesRead);
                    }
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
    public IndexInput slice(String sliceName, long sliceOffset, long sliceLength) {
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > length()) {
            throw new IllegalArgumentException(
                "slice() "
                    + sliceName
                    + " out of bounds: offset="
                    + sliceOffset
                    + ",length="
                    + sliceLength
                    + ",fileLength="
                    + length()
                    + ": "
                    + this
            );
        }

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

        final CachedBlobContainerIndexInput slice = new CachedBlobContainerIndexInput(
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
}
