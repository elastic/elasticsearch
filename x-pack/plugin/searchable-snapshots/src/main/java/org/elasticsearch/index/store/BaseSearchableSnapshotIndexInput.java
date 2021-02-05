/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BaseSearchableSnapshotIndexInput extends BufferedIndexInput {

    protected final BlobContainer blobContainer;
    protected final FileInfo fileInfo;
    protected final IOContext context;
    protected final IndexInputStats stats;
    protected final long offset;
    protected final long length;

    // the following are only mutable so they can be adjusted after cloning/slicing
    protected volatile boolean isClone;
    private AtomicBoolean closed;

    public BaseSearchableSnapshotIndexInput(
        String resourceDesc,
        BlobContainer blobContainer,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long length
    ) {
        super(resourceDesc, context);
        this.blobContainer = Objects.requireNonNull(blobContainer);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.context = Objects.requireNonNull(context);
        assert fileInfo.metadata().hashEqualsContents() == false
            : "this method should only be used with blobs that are NOT stored in metadata's hash field " + "(fileInfo: " + fileInfo + ')';
        this.stats = Objects.requireNonNull(stats);
        this.offset = offset;
        this.length = length;
        this.closed = new AtomicBoolean(false);
        this.isClone = false;
    }

    public BaseSearchableSnapshotIndexInput(
        String resourceDesc,
        BlobContainer blobContainer,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long offset,
        long length,
        int bufferSize
    ) {
        this(resourceDesc, blobContainer, fileInfo, context, stats, offset, length);
        setBufferSize(bufferSize);
    }

    @Override
    public final long length() {
        return length;
    }

    /**
     * Opens an {@link InputStream} for the given range of bytes which reads the data directly from the blob store. If the requested range
     * spans multiple blobs then this stream will request them in turn.
     *
     * @param position The start of the range of bytes to read, relative to the start of the corresponding Lucene file.
     * @param length The number of bytes to read
     */
    protected InputStream openInputStreamFromBlobStore(final long position, final long length) throws IOException {
        assert assertCurrentThreadMayAccessBlobStore();
        if (fileInfo.numberOfParts() == 1L) {
            assert position + length <= fileInfo.partBytes(0) : "cannot read ["
                + position
                + "-"
                + (position + length)
                + "] from ["
                + fileInfo
                + "]";
            stats.addBlobStoreBytesRequested(length);
            return blobContainer.readBlob(fileInfo.partName(0), position, length);
        } else {
            final int startPart = getPartNumberForPosition(position);
            final int endPart = getPartNumberForPosition(position + length - 1);

            for (int currentPart = startPart; currentPart <= endPart; currentPart++) {
                final long startInPart = (currentPart == startPart) ? getRelativePositionInPart(position) : 0L;
                final long endInPart = (currentPart == endPart)
                    ? getRelativePositionInPart(position + length - 1) + 1
                    : getLengthOfPart(currentPart);
                stats.addBlobStoreBytesRequested(endInPart - startInPart);
            }

            return new SlicedInputStream(endPart - startPart + 1) {
                @Override
                protected InputStream openSlice(int slice) throws IOException {
                    final int currentPart = startPart + slice;
                    final long startInPart = (currentPart == startPart) ? getRelativePositionInPart(position) : 0L;
                    final long endInPart = (currentPart == endPart)
                        ? getRelativePositionInPart(position + length - 1) + 1
                        : getLengthOfPart(currentPart);
                    return blobContainer.readBlob(fileInfo.partName(currentPart), startInPart, endInPart - startInPart);
                }
            };
        }
    }

    /**
     * Compute the part number that contains the byte at the given position in the corresponding Lucene file.
     */
    private int getPartNumberForPosition(long position) {
        ensureValidPosition(position);
        final int part = Math.toIntExact(position / fileInfo.partSize().getBytes());
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

    protected long getLengthOfPart(int part) {
        return fileInfo.partBytes(part);
    }

    private void ensureValidPosition(long position) {
        assert position >= 0L && position < fileInfo.length() : position + " vs " + fileInfo.length();
        // noinspection ConstantConditions in case assertions are disabled
        if (position < 0L || position >= fileInfo.length()) {
            throw new IllegalArgumentException("Position [" + position + "] is invalid for a file of length [" + fileInfo.length() + "]");
        }
    }

    @Override
    public BaseSearchableSnapshotIndexInput clone() {
        final BaseSearchableSnapshotIndexInput clone = (BaseSearchableSnapshotIndexInput) super.clone();
        clone.closed = new AtomicBoolean(false);
        clone.isClone = true;
        return clone;
    }

    protected void ensureOpen() throws IOException {
        if (closed.get()) {
            throw new IOException(toString() + " is closed");
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (isClone == false) {
                stats.incrementCloseCount();
            }
            innerClose();
        }
    }

    public abstract void innerClose() throws IOException;

    protected final boolean assertCurrentThreadMayAccessBlobStore() {
        final String threadName = Thread.currentThread().getName();
        assert threadName.contains('[' + ThreadPool.Names.SNAPSHOT + ']')
            || threadName.contains('[' + ThreadPool.Names.GENERIC + ']')
            || threadName.contains('[' + ThreadPool.Names.SEARCH + ']')
            || threadName.contains('[' + ThreadPool.Names.SEARCH_THROTTLED + ']')

            // Cache asynchronous fetching runs on a dedicated thread pool.
            || threadName.contains('[' + SearchableSnapshotsConstants.CACHE_FETCH_ASYNC_THREAD_POOL_NAME + ']')

            // Cache prewarming also runs on a dedicated thread pool.
            || threadName.contains('[' + SearchableSnapshotsConstants.CACHE_PREWARMING_THREAD_POOL_NAME + ']')

            // Unit tests access the blob store on the main test thread; simplest just to permit this rather than have them override this
            // method somehow.
            || threadName.startsWith("TEST-")
            || threadName.startsWith("LuceneTestCase") : "current thread [" + Thread.currentThread() + "] may not read " + fileInfo;
        return true;
    }

}
