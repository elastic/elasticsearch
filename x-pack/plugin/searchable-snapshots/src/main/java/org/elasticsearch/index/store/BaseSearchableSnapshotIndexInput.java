/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;

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
        final long startPart = getPartNumberForPosition(position);
        final long endPart = getPartNumberForPosition(position + length);
        if ((startPart == endPart) || fileInfo.numberOfParts() == 1L) {
            return blobContainer.readBlob(fileInfo.partName(startPart), getRelativePositionInPart(position), length);
        } else {
            return new SlicedInputStream(endPart - startPart + 1L) {
                @Override
                protected InputStream openSlice(long slice) throws IOException {
                    final long currentPart = startPart + slice;
                    return blobContainer.readBlob(
                        fileInfo.partName(currentPart),
                        (currentPart == startPart) ? getRelativePositionInPart(position) : 0L,
                        (currentPart == endPart) ? getRelativePositionInPart(length) : getLengthOfPart(currentPart)
                    );
                }
            };
        }
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
