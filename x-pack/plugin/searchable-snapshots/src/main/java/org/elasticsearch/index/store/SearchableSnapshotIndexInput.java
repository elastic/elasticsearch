/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
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

    private final BlobContainer blobContainer;
    private final FileInfo fileInfo;
    private final long offset;
    private final long length;

    private long position;
    private volatile boolean closed;

    // optimisation for the case where we perform a single seek, then read a large block of data sequentially, then close the input
    @Nullable // if not currently reading sequentially
    private StreamForSequentialReads streamForSequentialReads;
    private long sequentialReadSize;
    private static final long NO_SEQUENTIAL_READ_OPTIMIZATION = 0L;


    SearchableSnapshotIndexInput(final BlobContainer blobContainer, final FileInfo fileInfo, long sequentialReadSize, int bufferSize) {
        this("SearchableSnapshotIndexInput(" + fileInfo.physicalName() + ")", blobContainer, fileInfo, 0L, 0L, fileInfo.length(),
            sequentialReadSize, bufferSize);
    }

    private SearchableSnapshotIndexInput(final String resourceDesc, final BlobContainer blobContainer, final FileInfo fileInfo,
                                         final long position, final long offset, final long length, final long sequentialReadSize,
                                         final int bufferSize) {
        super(resourceDesc, bufferSize);
        this.blobContainer = Objects.requireNonNull(blobContainer);
        this.fileInfo = Objects.requireNonNull(fileInfo);
        this.offset = offset;
        this.length = length;
        this.position = position;
        assert sequentialReadSize >= 0;
        this.sequentialReadSize = sequentialReadSize;
        this.closed = false;
    }

    @Override
    public long length() {
        return length;
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
            readInternalBytes(0, position, b, offset, length);
        } else {
            int len = length;
            int off = offset;
            while (len > 0) {
                int currentPart = Math.toIntExact(position / fileInfo.partSize().getBytes());
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

    private void readInternalBytes(final int part, long pos, final byte[] b, int offset, int length) throws IOException {
        int optimizedReadSize = readOptimized(part, pos, b, offset, length);
        assert optimizedReadSize <= length;
        position += optimizedReadSize;

        if (optimizedReadSize < length) {
            // we did not read everything in an optimized fashion, so read the remainder directly
            try (InputStream inputStream
                     = blobContainer.readBlob(fileInfo.partName(part), pos + optimizedReadSize, length - optimizedReadSize)) {
                final int directReadSize = inputStream.read(b, offset + optimizedReadSize, length - optimizedReadSize);
                assert optimizedReadSize + directReadSize == length : optimizedReadSize + " and " + directReadSize + " vs " + length;
                position += directReadSize;
            }
        }
    }

    /**
     * Attempt to satisfy this read in an optimized fashion using {@code streamForSequentialReadsRef}.
     * @return the number of bytes read
     */
    private int readOptimized(int part, long pos, byte[] b, int offset, int length) throws IOException {
        if (sequentialReadSize == NO_SEQUENTIAL_READ_OPTIMIZATION) {
            return 0;
        }

        int read = 0;
        if (streamForSequentialReads == null) {
            // starting a new sequential read
            read = readFromNewSequentialStream(part, pos, b, offset, length);
        } else if (streamForSequentialReads.canContinueSequentialRead(part, pos)) {
            // continuing a sequential read that we started previously
            read = streamForSequentialReads.read(b, offset, length);
            if (streamForSequentialReads.isFullyRead()) {
                // the current stream was exhausted by this read, so it should be closed
                streamForSequentialReads.close();
                streamForSequentialReads = null;
            } else {
                // the current stream contained enough data for this read and more besides, so we leave it in place
                assert read == length : length + " remaining";
            }

            if (read < length) {
                // the current stream didn't contain enough data for this read, so we must read more
                read += readFromNewSequentialStream(part, pos + read, b, offset + read, length - read);
            }
        } else {
            // not a sequential read, so stop optimizing for this usage pattern and fall through to the unoptimized behaviour
            assert streamForSequentialReads.isFullyRead() == false;
            sequentialReadSize = NO_SEQUENTIAL_READ_OPTIMIZATION;
            IOUtils.close(streamForSequentialReads);
            streamForSequentialReads = null;
        }
        return read;
    }

    /**
     * If appropriate, open a new stream for sequential reading and satisfy the given read using it.
     * @return the number of bytes read; if a new stream wasn't opened then nothing was read so the caller should perform the read directly.
     */
    private int readFromNewSequentialStream(int part, long pos, byte[] b, int offset, int length)
        throws IOException {

        assert streamForSequentialReads == null : "should only be called when a new stream is needed";
        assert sequentialReadSize > 0L : "should only be called if optimizing sequential reads";

        final long streamLength = Math.min(sequentialReadSize, fileInfo.partBytes(part) - pos);
        if (streamLength <= length) {
            // streamLength <= length so this single read will consume the entire stream, so there is no need to keep hold of it, so we can
            // tell the caller to read the data directly
            return 0;
        }

        // if we open a stream of length streamLength then it will not be completely consumed by this read, so it is worthwhile to open
        // it and keep it open for future reads
        final InputStream inputStream = blobContainer.readBlob(fileInfo.partName(part), pos, streamLength);
        streamForSequentialReads = new StreamForSequentialReads(inputStream, part, pos, streamLength);

        final int read = streamForSequentialReads.read(b, offset, length);
        assert read == length : read + " vs " + length;
        assert streamForSequentialReads.isFullyRead() == false;
        return read;
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length) {
            throw new EOFException("Reading past end of file [position=" + pos + ", length=" + length + "] for " + toString());
        } else if (pos < 0L) {
            throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
        }
        if (position != offset + pos) {
            position = offset + pos;
            IOUtils.close(streamForSequentialReads);
            streamForSequentialReads = null;
        }
    }

    @Override
    public BufferedIndexInput clone() {
        return new SearchableSnapshotIndexInput("clone(" + this + ")", blobContainer, fileInfo, position, offset, length,
            NO_SEQUENTIAL_READ_OPTIMIZATION, getBufferSize());
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        if ((offset >= 0L) && (length >= 0L) && (offset + length <= length())) {
            final SearchableSnapshotIndexInput slice = new SearchableSnapshotIndexInput(sliceDescription, blobContainer, fileInfo, position,
                this.offset + offset, length, NO_SEQUENTIAL_READ_OPTIMIZATION, getBufferSize());
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
        IOUtils.close(streamForSequentialReads);
        streamForSequentialReads = null;
    }

    @Override
    public String toString() {
        return "SearchableSnapshotIndexInput{" +
            "resourceDesc=" + super.toString() +
            ", fileInfo=" + fileInfo +
            ", offset=" + offset +
            ", length=" + length +
            ", position=" + position +
            '}';
    }

    private static class StreamForSequentialReads implements Closeable {
        private final InputStream inputStream;
        private final int part;
        private long pos; // position within this part
        private final long maxPos;

        StreamForSequentialReads(InputStream inputStream, int part, long pos, long streamLength) {
            this.inputStream = Objects.requireNonNull(inputStream);
            this.part = part;
            this.pos = pos;
            this.maxPos = pos + streamLength;
        }

        boolean canContinueSequentialRead(int part, long pos) {
            return this.part == part && this.pos == pos;
        }

        int read(byte[] b, int offset, int length) throws IOException {
            assert this.pos < maxPos : "should not try and read from a fully-read stream";
            int read = inputStream.read(b, offset, length);
            assert read <= length : read + " vs " + length;
            pos += read;
            return read;
        }

        boolean isFullyRead() {
            assert this.pos <= maxPos;
            return this.pos >= maxPos;
        }

        @Override
        public void close() throws IOException {
            inputStream.close();
        }
    }
}
