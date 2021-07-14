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
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.store.IndexInputStats;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;

/**
 * A {@link DirectBlobContainerIndexInput} instance corresponds to a single file from a Lucene directory that has been snapshotted. Because
 * large Lucene file might be split into multiple parts during the snapshot, {@link DirectBlobContainerIndexInput} requires a
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
 * {@link DirectBlobContainerIndexInput} maintains a global position that indicates the current position in the Lucene file where the
 * next read will occur. In the case of a Lucene file snapshotted into multiple parts, this position is used to identify which part must
 * be read at which position (see {@link #readInternal(ByteBuffer)}. This position is also passed over to cloned and sliced input
 * along with the {@link FileInfo} so that they can also track their reading position.
 *
 * The {@code sequentialReadSize} constructor parameter configures the {@link DirectBlobContainerIndexInput} to perform a larger read on the
 * underlying {@link BlobContainer} than it needs in order to fill its internal buffer, on the assumption that the client is reading
 * sequentially from this file and will consume the rest of this stream in due course. It keeps hold of the partially-consumed
 * {@link InputStream} in {@code streamForSequentialReads}. Clones and slices, however, do not expect to be read sequentially and so make
 * a new request to the {@link BlobContainer} each time their internal buffer needs refilling.
 */
public class DirectBlobContainerIndexInput extends BaseSearchableSnapshotIndexInput {

    private static final Logger logger = LogManager.getLogger(DirectBlobContainerIndexInput.class);

    private long position;

    @Nullable // if not currently reading sequentially
    private StreamForSequentialReads streamForSequentialReads;
    private long sequentialReadSize;
    private static final long NO_SEQUENTIAL_READ_OPTIMIZATION = 0L;
    private static final int COPY_BUFFER_SIZE = 8192;

    public DirectBlobContainerIndexInput(
        String name,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long sequentialReadSize,
        int bufferSize
    ) {
        this(name, directory, fileInfo, context, stats, 0L, 0L, fileInfo.length(), sequentialReadSize, bufferSize);
        stats.incrementOpenCount();
    }

    private DirectBlobContainerIndexInput(
        String name,
        SearchableSnapshotDirectory directory,
        FileInfo fileInfo,
        IOContext context,
        IndexInputStats stats,
        long position,
        long offset,
        long length,
        long sequentialReadSize,
        int bufferSize
    ) {
        super(logger, name, directory, fileInfo, context, stats, offset, length, ByteRange.EMPTY, ByteRange.EMPTY); // TODO should use blob
                                                                                                                    // cache
        this.position = position;
        assert sequentialReadSize >= 0;
        this.sequentialReadSize = sequentialReadSize;
        setBufferSize(bufferSize);
    }

    @Override
    protected void doReadInternal(ByteBuffer b) throws IOException {
        ensureOpen();
        if (fileInfo.numberOfParts() == 1) {
            readInternalBytes(0, position, b, b.remaining());
        } else {
            while (b.hasRemaining()) {
                int currentPart = Math.toIntExact(position / fileInfo.partSize().getBytes());
                long remainingBytesInPart;
                if (currentPart < (fileInfo.numberOfParts() - 1)) {
                    remainingBytesInPart = ((currentPart + 1) * fileInfo.partSize().getBytes()) - position;
                } else {
                    remainingBytesInPart = toIntBytes(fileInfo.length() - position);
                }
                final int read = toIntBytes(Math.min(b.remaining(), remainingBytesInPart));
                readInternalBytes(currentPart, position % fileInfo.partSize().getBytes(), b, read);
            }
        }
    }

    private void readInternalBytes(final int part, long pos, final ByteBuffer b, int length) throws IOException {
        int optimizedReadSize = readOptimized(part, pos, b, length);
        assert optimizedReadSize <= length;
        position += optimizedReadSize;

        if (optimizedReadSize < length) {
            // we did not read everything in an optimized fashion, so read the remainder directly
            final long startTimeNanos = stats.currentTimeNanos();
            try (InputStream inputStream = openBlobStream(part, pos + optimizedReadSize, length - optimizedReadSize)) {
                final int directReadSize = readFully(
                    inputStream,
                    b,
                    length - optimizedReadSize,
                    () -> { throw new EOFException("Read past EOF at [" + position + "] with length [" + fileInfo.partBytes(part) + "]"); }
                );
                assert optimizedReadSize + directReadSize == length : optimizedReadSize + " and " + directReadSize + " vs " + length;
                position += directReadSize;
                final long endTimeNanos = stats.currentTimeNanos();
                stats.addDirectBytesRead(directReadSize, endTimeNanos - startTimeNanos);
            }
        }
    }

    /**
     * Attempt to satisfy this read in an optimized fashion using {@code streamForSequentialReadsRef}.
     * @return the number of bytes read
     */
    private int readOptimized(int part, long pos, ByteBuffer b, int length) throws IOException {
        if (sequentialReadSize == NO_SEQUENTIAL_READ_OPTIMIZATION) {
            return 0;
        }

        int read = 0;
        if (streamForSequentialReads == null) {
            // starting a new sequential read
            read = readFromNewSequentialStream(part, pos, b, length);
        } else if (streamForSequentialReads.canContinueSequentialRead(part, pos)) {
            // continuing a sequential read that we started previously
            read = streamForSequentialReads.read(b, length);
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
                read += readFromNewSequentialStream(part, pos + read, b, length - read);
            }
        } else {
            // not a sequential read, so stop optimizing for this usage pattern and fall through to the unoptimized behaviour
            assert streamForSequentialReads.isFullyRead() == false;
            sequentialReadSize = NO_SEQUENTIAL_READ_OPTIMIZATION;
            closeStreamForSequentialReads();
        }
        return read;
    }

    private void closeStreamForSequentialReads() throws IOException {
        try {
            IOUtils.close(streamForSequentialReads);
        } finally {
            streamForSequentialReads = null;
        }
    }

    /**
     * If appropriate, open a new stream for sequential reading and satisfy the given read using it.
     * @return the number of bytes read; if a new stream wasn't opened then nothing was read so the caller should perform the read directly.
     */
    private int readFromNewSequentialStream(int part, long pos, ByteBuffer b, int length) throws IOException {

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
        final InputStream inputStream = openBlobStream(part, pos, streamLength);
        streamForSequentialReads = new StreamForSequentialReads(new FilterInputStream(inputStream) {
            private final LongAdder bytesRead = new LongAdder();
            private final LongAdder timeNanos = new LongAdder();

            private int onOptimizedRead(CheckedSupplier<Integer, IOException> read) throws IOException {
                final long startTimeNanos = stats.currentTimeNanos();
                final int result = read.get();
                final long endTimeNanos = stats.currentTimeNanos();
                if (result != -1) {
                    bytesRead.add(result);
                    timeNanos.add(endTimeNanos - startTimeNanos);
                }
                return result;
            }

            @Override
            public int read() throws IOException {
                return onOptimizedRead(super::read);
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return onOptimizedRead(() -> super.read(b, off, len));
            }

            @Override
            public void close() throws IOException {
                super.close();
                stats.addOptimizedBytesRead(Math.toIntExact(bytesRead.sumThenReset()), timeNanos.sumThenReset());
            }
        }, part, pos, streamLength);

        final int read = streamForSequentialReads.read(b, length);
        assert read == length : read + " vs " + length;
        assert streamForSequentialReads.isFullyRead() == false;
        return read;
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        if (pos > length()) {
            throw new EOFException("Reading past end of file [position=" + pos + ", length=" + length() + "] for " + toString());
        } else if (pos < 0L) {
            throw new IOException("Seeking to negative position [" + pos + "] for " + toString());
        }
        if (position != offset + pos) {
            position = offset + pos;
            closeStreamForSequentialReads();
        }
    }

    @Override
    public DirectBlobContainerIndexInput clone() {
        final DirectBlobContainerIndexInput clone = new DirectBlobContainerIndexInput(
            "clone(" + this + ")",
            directory,
            fileInfo,
            context,
            stats,
            position,
            offset,
            length,
            // Clones might not be closed when they are no longer needed, but we must always close streamForSequentialReads. The simple
            // solution: do not optimize sequential reads on clones.
            NO_SEQUENTIAL_READ_OPTIMIZATION,
            getBufferSize()
        );
        clone.isClone = true;
        return clone;
    }

    @Override
    public IndexInput slice(String sliceName, long offset, long length) throws IOException {
        if ((offset >= 0L) && (length >= 0L) && (offset + length <= length())) {
            final DirectBlobContainerIndexInput slice = new DirectBlobContainerIndexInput(
                sliceName,
                directory,
                fileInfo,
                context,
                stats,
                position,
                this.offset + offset,
                length,
                // Slices might not be closed when they are no longer needed, but we must always close streamForSequentialReads. The simple
                // solution: do not optimize sequential reads on slices.
                NO_SEQUENTIAL_READ_OPTIMIZATION,
                getBufferSize()
            );
            slice.isClone = true;
            slice.seek(0L);
            return slice;
        } else {
            throw new IllegalArgumentException(
                "slice() " + sliceName + " out of bounds: offset=" + offset + ",length=" + length + ",fileLength=" + length() + ": " + this
            );
        }
    }

    @Override
    public void doClose() throws IOException {
        closeStreamForSequentialReads();
    }

    @Override
    public String toString() {
        return super.toString() + "[read seq=" + (streamForSequentialReads != null ? "yes" : "no") + ']';
    }

    private InputStream openBlobStream(int part, long pos, long length) throws IOException {
        assert assertCurrentThreadMayAccessBlobStore();
        stats.addBlobStoreBytesRequested(length);
        return blobContainer.readBlob(fileInfo.partName(part), pos, length);
    }

    /**
     * Fully read up to {@code length} bytes from the given {@link InputStream}
     */
    private static int readFully(InputStream inputStream, final ByteBuffer b, int length, CheckedRunnable<IOException> onEOF)
        throws IOException {
        int totalRead = 0;
        final byte[] buffer = new byte[Math.min(length, COPY_BUFFER_SIZE)];
        while (totalRead < length) {
            final int len = Math.min(length - totalRead, COPY_BUFFER_SIZE);
            final int read = inputStream.read(buffer, 0, len);
            if (read == -1) {
                onEOF.run();
                break;
            }
            b.put(buffer, 0, read);
            totalRead += read;
        }
        return totalRead > 0 ? totalRead : -1;
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

        int read(ByteBuffer b, int length) throws IOException {
            assert this.pos < maxPos : "should not try and read from a fully-read stream";
            final int read = readFully(inputStream, b, length, () -> {});
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
