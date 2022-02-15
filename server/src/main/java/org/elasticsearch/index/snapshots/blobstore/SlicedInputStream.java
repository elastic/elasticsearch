/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.snapshots.blobstore;

import org.elasticsearch.core.internal.io.IOUtils;

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
    private int slice = 0;
    private InputStream currentStream;
    private final int numSlices;
    private boolean initialized = false;

    /**
     * Creates a new SlicedInputStream
     * @param numSlices the number of slices to consume
     */
    protected SlicedInputStream(final int numSlices) {
        this.numSlices = numSlices;
    }

    private InputStream nextStream() throws IOException {
        assert initialized == false || currentStream != null;
        initialized = true;
        IOUtils.close(currentStream);
        if (slice < numSlices) {
            currentStream = openSlice(slice++);
        } else {
            currentStream = null;
        }
        return currentStream;
    }

    /**
     * Called for each logical slice given a zero based slice ordinal.
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
        return read;
    }

    @Override
    public final void close() throws IOException {
        IOUtils.close(currentStream);
        initialized = true;
        currentStream = null;
    }

    @Override
    public final int available() throws IOException {
        InputStream stream = currentStream();
        return stream == null ? 0 : stream.available();
    }

}
