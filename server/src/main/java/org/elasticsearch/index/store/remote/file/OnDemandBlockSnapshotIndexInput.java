/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store.remote.file;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.remote.utils.BlobFetchRequest;
import org.elasticsearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OnDemandBlockSnapshotIndexInput extends OnDemandBlockIndexInput {
    /**
     * Where this class fetches IndexInput parts from
     */
    final TransferManager transferManager;

    /**
     * FileInfo contains snapshot metadata references for this IndexInput
     */
    protected final BlobStoreIndexShardSnapshot.FileInfo fileInfo;

    /**
     * Underlying lucene directory to open blocks and for caching
     */
    protected final FSDirectory directory;
    /**
     * File Name
     */
    protected final String fileName;

    /**
     * Maximum size in bytes of snapshot file parts.
     */
    protected final long partSize;

    /**
     * Size of the file, larger than length if it's a slice
     */
    protected final long originalFileSize;

    public OnDemandBlockSnapshotIndexInput(BlobStoreIndexShardSnapshot.FileInfo fileInfo, FSDirectory directory, TransferManager transferManager) {
        this(
            "BlockedSnapshotIndexInput(path=\""
                + directory.getDirectory().toString()
                + "/"
                + fileInfo.physicalName()
                + "\", "
                + "offset="
                + 0
                + ", length= "
                + fileInfo.length()
                + ")",
            fileInfo,
            0L,
            fileInfo.length(),
            false,
            directory,
            transferManager
        );
    }

    public OnDemandBlockSnapshotIndexInput(
        String resourceDescription,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        long offset,
        long length,
        boolean isClone,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        this(
            OnDemandBlockIndexInput.builder().resourceDescription(resourceDescription).isClone(isClone).offset(offset).length(length),
            fileInfo,
            directory,
            transferManager
        );
    }

    OnDemandBlockSnapshotIndexInput(
        OnDemandBlockIndexInput.Builder builder,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        FSDirectory directory,
        TransferManager transferManager
    ) {
        super(builder);
        this.transferManager = transferManager;
        this.fileInfo = fileInfo;
        if (fileInfo.partSize() != null) {
            this.partSize = fileInfo.partSize().getBytes();
        } else {
            // Repository implementations can define a size at which to split files
            // into multiple objects in the repository. If partSize() is null, then
            // no splitting happens, so default to Long.MAX_VALUE here to have the
            // same effect. See {@code BlobStoreRepository#chunkSize()}.
            this.partSize = Long.MAX_VALUE;
        }
        this.fileName = fileInfo.physicalName();
        this.directory = directory;
        this.originalFileSize = fileInfo.length();
    }

    @Override
    protected OnDemandBlockSnapshotIndexInput buildSlice(String sliceDescription, long offset, long length) {
        return new OnDemandBlockSnapshotIndexInput(
            OnDemandBlockIndexInput.builder()
                .blockSizeShift(blockSizeShift)
                .isClone(true)
                .offset(this.offset + offset)
                .length(length)
                .resourceDescription(sliceDescription),
            fileInfo,
            directory,
            transferManager
        );
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        final String blockFileName = fileName + "." + blockId;

        final long blockStart = getBlockStart(blockId);
        final long blockEnd = blockStart + getActualBlockSize(blockId);

        // Block may be present on multiple chunks of a file, so we need
        // to fetch each chunk/blob part separately to fetch an entire block.
        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .blobParts(getBlobParts(blockStart, blockEnd))
            .directory(directory)
            .fileName(blockFileName)
            .build();
        return transferManager.fetchBlob(blobFetchRequest);
    }

    /**
     * Returns list of blob parts/chunks in a file for a given block.
     */
    protected List<BlobFetchRequest.BlobPart> getBlobParts(long blockStart, long blockEnd) {
        // If the snapshot file is chunked, we must account for this by
        // choosing the appropriate file part and updating the position
        // accordingly.
        int partNum = (int) (blockStart / partSize);
        long pos = blockStart;
        long diff = (blockEnd - blockStart);

        List<BlobFetchRequest.BlobPart> blobParts = new ArrayList<>();
        while (diff > 0) {
            long partStart = pos % partSize;
            long partEnd;
            if ((partStart + diff) > partSize) {
                partEnd = partSize;
            } else {
                partEnd = (partStart + diff);
            }
            long fetchBytes = partEnd - partStart;
            blobParts.add(new BlobFetchRequest.BlobPart(fileInfo.partName(partNum), partStart, fetchBytes));
            partNum++;
            pos = pos + fetchBytes;
            diff = (blockEnd - pos);
        }
        return blobParts;
    }

    @Override
    public OnDemandBlockSnapshotIndexInput clone() {
        OnDemandBlockSnapshotIndexInput clone = buildSlice("clone", 0L, this.length);
        // ensures that clones may be positioned at the same point as the blocked file they were cloned from
        clone.cloneBlock(this);
        return clone;
    }

    protected long getActualBlockSize(int blockId) {
        return (blockId != getBlock(originalFileSize - 1)) ? blockSize : getBlockOffset(originalFileSize - 1) + 1;
    }
}
