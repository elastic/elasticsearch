/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public abstract class BaseSearchableSnapshotIndexInput extends BufferedIndexInput {

    protected final BlobContainer blobContainer;
    protected final FileInfo fileInfo;
    protected final IOContext context;

    public BaseSearchableSnapshotIndexInput(String resourceDesc, BlobContainer blobContainer, FileInfo fileInfo, IOContext context) {
        super(resourceDesc, context);
        this.blobContainer = Objects.requireNonNull(blobContainer);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.context = Objects.requireNonNull(context);
        assert fileInfo.metadata().hashEqualsContents() == false
            : "this method should only be used with blobs that are NOT stored in metadata's hash field (fileInfo: " + fileInfo + ')';
    }

    public BaseSearchableSnapshotIndexInput(
        String resourceDesc,
        BlobContainer blobContainer,
        FileInfo fileInfo,
        IOContext context,
        int bufferSize
    ) {
        this(resourceDesc, blobContainer, fileInfo, context);
        setBufferSize(bufferSize);
    }

    protected InputStream openInputStream(final long position, final long length) throws IOException {
        assert assertCurrentThreadMayAccessBlobStore();
        final long startPart = getPartNumberForPosition(position);
        final long endPart = getPartNumberForPosition(position + length);
        if ((startPart == endPart) || fileInfo.numberOfParts() == 1L) {
            return blobContainer.readBlob(fileInfo.partName(startPart), getRelativePositionInPart(position), length);
        } else {
            return new SlicedInputStream(endPart - startPart + 1L) {
                @Override
                protected InputStream openSlice(long slice) throws IOException {
                    final long currentPart = startPart + slice;
                    final long startInPart = (currentPart == startPart) ? getRelativePositionInPart(position) : 0L;
                    final long endInPart
                        = (currentPart == endPart) ? getRelativePositionInPart(position + length) : getLengthOfPart(currentPart);
                    return blobContainer.readBlob(
                        fileInfo.partName(currentPart),
                        startInPart,
                        endInPart - startInPart
                    );
                }
            };
        }
    }

    protected final boolean assertCurrentThreadMayAccessBlobStore() {
        final String threadName = Thread.currentThread().getName();
        assert threadName.contains('[' + ThreadPool.Names.SNAPSHOT + ']')
            || threadName.contains('[' + ThreadPool.Names.GENERIC + ']')
            || threadName.contains('[' + ThreadPool.Names.SEARCH + ']')
            || threadName.contains('[' + ThreadPool.Names.SEARCH_THROTTLED + ']')

            // Today processExistingRecoveries considers all shards and constructs a shard store snapshot on this thread, this needs
            // addressing. TODO NORELEASE
            || threadName.contains('[' + ThreadPool.Names.FETCH_SHARD_STORE + ']')

            // Today for as-yet-unknown reasons we sometimes try and compute the snapshot size on the cluster applier thread, which needs
            // addressing. TODO NORELEASE
            || threadName.contains('[' + ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME + ']')

            // Unit tests access the blob store on the main test thread; simplest just to permit this rather than have them override this
            // method somehow.
            || threadName.startsWith("TEST-")
            : "current thread [" + Thread.currentThread() + "] may not read " + fileInfo;
        return true;
    }

    private long getPartNumberForPosition(long position) {
        ensureValidPosition(position);
        final long part = position / fileInfo.partSize().getBytes();
        assert part <= fileInfo.numberOfParts() : "part number [" + part + "] exceeds number of parts: " + fileInfo.numberOfParts();
        assert part >= 0L : "part number [" + part + "] is negative";
        return part;
    }

    private long getRelativePositionInPart(long position) {
        ensureValidPosition(position);
        final long pos = position % fileInfo.partSize().getBytes();
        assert pos < fileInfo.partBytes((int) getPartNumberForPosition(pos)) : "position in part [" + pos + "] exceeds part's length";
        assert pos >= 0L : "position in part [" + pos + "] is negative";
        return pos;
    }

    private long getLengthOfPart(long part) {
        return fileInfo.partBytes(Math.toIntExact(part));
    }

    private void ensureValidPosition(long position) {
        if (position < 0L || position > fileInfo.length()) {
            throw new IllegalArgumentException("Position [" + position + "] is invalid");
        }
    }
}
