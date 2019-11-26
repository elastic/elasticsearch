/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.io.EOFException;
import java.io.IOException;
import java.util.Objects;

/**
 * A {@link SearchableSnapshotIndexInput} instance corresponds to a single file from a Lucene directory that has been snapshotted. Because
 * large Lucene file might be split into multiple parts during the snapshot, {@link SearchableSnapshotIndexInput} requires a
 * {@link FileInfo} object at creation time. This object is used to retrieve the file name and length of the original Lucene file, as well
 * as all the parts (stored as "blobs" in the repository) that composed the file in the snapshot.
 *
 * For example, the following {@link FileInfo}:
 *  [name: __4vdpz_HFQ8CuKjCERX0o2A, numberOfParts: 2, partSize: 997b, partBytes: 997, metadata: name [_0_Asserting_0.pos], length [1413]
 *
 * Indicates that the Lucene file "_0_Asserting_0.pos" has a total length of 1413 and is snapshotted into 2 parts:
 * - __4vdpz_HFQ8CuKjCERX0o2A.part1 of size 997b
 * - __4vdpz_HFQ8CuKjCERX0o2A.part2 of size 416b
 *
 * {@link SearchableSnapshotIndexInput} maintains a global position that indicates the current position in the Lucene file where the
 * next read will occur. In the case of a Lucene file snapshotted into multiple parts, this position is used to identify which part must
 * be read at which position (see {@link #readInternal(byte[], int, int)}. This position is also passed over to cloned and sliced input
 * along with the {@link FileInfo} so that they can also track their reading position.
 */
public class SearchableSnapshotIndexInput extends BufferedIndexInput {

    private final BlobBytesReader reader;
    private final FileInfo fileInfo;

    private long position;
    private boolean closed;

    public SearchableSnapshotIndexInput(final BlobBytesReader reader, final FileInfo fileInfo) {
        this("SearchableSnapshotIndexInput(" + fileInfo + ")", reader, fileInfo, 0L);
    }

    private SearchableSnapshotIndexInput(final String resourceDesc, final BlobBytesReader reader,
                                         final FileInfo fileInfo, final long position) {
        super(resourceDesc);
        this.reader = Objects.requireNonNull(reader);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.position = position;
        this.closed = false;
    }

    @Override
    public long length() {
        return fileInfo.length();
    }

    private void ensureOpen() throws IOException {
        if (closed) {
            throw new IOException(toString() + " is closed");
        }
    }

    @Override
    protected void readInternal(byte[] b, int offset, int length) throws IOException {
        ensureOpen();
        if (fileInfo.numberOfParts() == 1L) {
            readInternalBytes(0L, position, b, offset, length);
        } else {
            int len = length;
            int off = offset;
            while (len > 0) {
                long currentPart = position / fileInfo.partSize().getBytes();
                int remainingBytesInPart;
                if (currentPart < (fileInfo.numberOfParts() - 1)) {
                    remainingBytesInPart = Math.toIntExact(((currentPart + 1L) * fileInfo.partSize().getBytes()) - position);
                } else {
                    remainingBytesInPart = Math.toIntExact(fileInfo.length() - position);
                }
                final int read = Math.min(len, remainingBytesInPart);
                readInternalBytes(currentPart, position % fileInfo.partSize().getBytes(), b, off, read);
                len -= read;
                off += read;
            }
        }
    }

    private void readInternalBytes(final long part, final long pos, byte[] b, int offset, int length) throws IOException {
        reader.readBlobBytes(fileInfo.partName(part), pos, length, b, offset);
        position += length;
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > fileInfo.length()) {
            throw new EOFException("Reading past end of file [position=" + pos + ", length=" + fileInfo.length() + "] for " + toString());
        } else if (pos < 0L) {
            throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
        }
        this.position = pos;
    }

    @Override
    public BufferedIndexInput clone() {
        return new SearchableSnapshotIndexInput("clone(" + this + ")", reader, fileInfo, position);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((offset >= 0L) && (length >= 0L) && (offset + length <= length())) {
            final Slice slice = new Slice(sliceDescription, offset, length, this);
            slice.seek(0L);
            return slice;
        } else {
            throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset
                + ",length=" + length + ",fileLength=" + length() + ": " + this);
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "(resourceDesc=" + super.toString()
            + ", name=" + fileInfo.physicalName()
            + ", length=" + fileInfo.length()
            + ", sizeOfParts=" + fileInfo.partSize()
            + ", numberOfParts=" + fileInfo.numberOfParts() + ")";
    }

    /**
     * A slice created from a {@link SearchableSnapshotIndexInput}.
     *
     * The slice overrides the {@link #length()} and {@link #seekInternal(long)}
     * methods so that it adjust the values according to initial offset position
     * from which the slice was created.
     */
    private static class Slice extends SearchableSnapshotIndexInput {

        private final long offset;
        private final long length;

        Slice(String sliceDescription, long offset, long length, SearchableSnapshotIndexInput base) {
            super(base.toString() + " [slice=" + sliceDescription + "]", base.reader, base.fileInfo, base.position);
            this.offset = offset;
            this.length = length;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        protected void seekInternal(long pos) throws IOException {
            super.seekInternal(offset + pos);
        }

        @Override
        public BufferedIndexInput clone() {
            return new Slice("clone(" + this + ")", offset, length, this);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            if ((offset >= 0L) && (length >= 0L) && (offset + length <= length())) {
                final Slice slice = new Slice(sliceDescription, offset + this.offset, length, this);
                slice.seek(0L);
                return slice;
            } else {
                throw new IllegalArgumentException("slice() " + sliceDescription + " out of bounds: offset=" + offset
                    + ",length=" + length + ",fileLength=" + length() + ": " + this);
            }
        }
    }
}
