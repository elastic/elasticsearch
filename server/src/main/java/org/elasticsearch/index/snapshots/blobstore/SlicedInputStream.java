/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.snapshots.blobstore;

import org.elasticsearch.core.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 *  A {@link SlicedInputStream} is a logical
 * concatenation one or more input streams. In contrast to the JDKs
 * {@link java.io.SequenceInputStream} this stream doesn't require the instantiation
 * of all logical sub-streams ahead of time. Instead, {@link #openSlice(int)} is called
 * if a new slice is required. Each slice is closed once it's been fully consumed or if
 * close is called before.
 */
public abstract class SlicedInputStream extends InputStream {
    private int nextSlice = 0;
    private InputStream currentStream;
    private long currentSliceOffset = 0;
    private final int numSlices;
    private boolean closed = false;
    private boolean initialized = false;
    private int markedSlice = -1;
    private long markedSliceOffset = -1;

    /**
     * Creates a new SlicedInputStream
     * @param numSlices the number of slices to consume
     */
    protected SlicedInputStream(final int numSlices) {
        this.numSlices = numSlices;
    }

    private InputStream nextStream() throws IOException {
        assert initialized == false || currentStream != null;
        assert closed == false : "attempted to get next stream when closed";
        initialized = true;
        IOUtils.close(currentStream);
        if (nextSlice < numSlices) {
            currentStream = openSlice(nextSlice++);
        } else {
            currentStream = null;
        }
        currentSliceOffset = 0;
        return currentStream;
    }

    /**
     * Called for each logical slice given a zero based slice ordinal.
     *
     * Note that if {@link InputStream#markSupported()} is true (can be overridden to return false), the function may be called again to
     * open a previous slice (which must have the same size as before). The returned InputStreams do not need to support mark/reset.
     */
    protected abstract InputStream openSlice(int slice) throws IOException;

    private InputStream currentStream() throws IOException {
        if (currentStream == null) {
            return initialized ? null : nextStream();
        }
        return currentStream;
    }

    @Override
    public final int read() throws IOException {
        InputStream stream = currentStream();
        if (stream == null) {
            return -1;
        }
        final int read = stream.read();
        if (read == -1) {
            nextStream();
            return read();
        }
        currentSliceOffset++;
        return read;
    }

    @Override
    public final int read(byte[] buffer, int offset, int length) throws IOException {
        final InputStream stream = currentStream();
        if (stream == null) {
            return -1;
        }
        final int read = stream.read(buffer, offset, length);
        if (read <= 0) {
            nextStream();
            return read(buffer, offset, length);
        }
        currentSliceOffset += read;
        return read;
    }

    @Override
    public long skip(long n) throws IOException {
        long remaining = n;
        while (remaining > 0) {
            final InputStream stream = currentStream();
            if (stream == null) {
                break;
            }
            long skipped = stream.skip(remaining);
            currentSliceOffset += skipped;
            if (skipped < remaining) {
                // read one more byte to see if we reached EOF in order to proceed to the next stream.
                if (stream.read() < 0) {
                    nextStream();
                } else {
                    currentSliceOffset++;
                    skipped++;
                }
            }
            remaining -= skipped;
        }
        return n - remaining;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        initialized = true;
        currentSliceOffset = 0;
        final InputStream stream = currentStream;
        currentStream = null;
        IOUtils.close(stream);
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public final int available() throws IOException {
        InputStream stream = currentStream();
        return stream == null ? 0 : stream.available();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void mark(int readLimit) {
        // We ignore readLimit since openSlice() can re-open previous InputStreams, and we can skip as many bytes as we'd like.
        // According to JDK documentation, marking a closed InputStream should have no effect.
        if (markSupported() && isClosed() == false && numSlices > 0) {
            if (initialized) {
                markedSlice = (currentStream == null) ? numSlices : nextSlice - 1;
                markedSliceOffset = currentSliceOffset;
            } else {
                markedSlice = 0;
                markedSliceOffset = 0;
            }
        }
    }

    @Override
    public void reset() throws IOException {
        if (markSupported()) {
            if (isClosed()) {
                throw new IOException("reset called on a closed stream");
            } else if (numSlices > 0) {
                if (markedSlice < 0 || markedSliceOffset < 0) {
                    throw new IOException("Mark has not been set");
                }

                nextSlice = markedSlice;
                initialized = true;
                IOUtils.close(currentStream);
                if (nextSlice < numSlices) {
                    currentStream = openSlice(nextSlice++);
                    // We do not call the SlicedInputStream's skipNBytes but call skipNBytes directly on the returned stream, to ensure that
                    // the skip is performed on the marked slice and no other slices are involved. This may help uncover any bugs.
                    currentStream.skipNBytes(markedSliceOffset);
                } else {
                    currentStream = null;
                }
                currentSliceOffset = markedSliceOffset;
            }
        } else {
            throw new IOException("mark/reset not supported");
        }
    }
}
