/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasables;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Base class for doing chunked writes to a blob store. Some blob stores require either up-front knowledge of the size of the blob that
 * will be written or writing it in chunks that are then joined into the final blob at the end of the write. This class provides a basis
 * on which to implement an output stream that encapsulates such a chunked write.
 *
 * @param <T> type of chunk identifier
 */
public abstract class ChunkedBlobOutputStream<T> extends OutputStream {

    /**
     * Size of the write buffer above which it must be flushed to storage.
     */
    protected final long maxBytesToBuffer;

    /**
     * List of identifiers of already written chunks.
     */
    protected final List<T> parts = new ArrayList<>();

    /**
     * Big arrays to be able to allocate buffers from pooled bytes.
     */
    private final BigArrays bigArrays;

    /**
     * Current write buffer.
     */
    protected ReleasableBytesStreamOutput buffer;

    /**
     * Set to true once no more calls to {@link #write} are expected and the blob has been received by {@link #write} in full so that
     * {@link #close()} knows whether to clean up existing chunks or finish a chunked write.
     */
    protected boolean successful = false;

    /**
     * Number of bytes written to storage writeBlso far.
     */
    protected long written = 0L;

    protected ChunkedBlobOutputStream(BigArrays bigArrays, long maxBytesToBuffer) {
        this.bigArrays = bigArrays;
        this.maxBytesToBuffer = maxBytesToBuffer;
        buffer = new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public void write(int b) throws IOException {
        buffer.write(b);
        maybeFlushBuffer();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        buffer.write(b, off, len);
        maybeFlushBuffer();
    }

    @Override
    public void close() throws IOException {
        try {
            doClose();
        } finally {
            Releasables.close(buffer);
        }
    }

    /**
     * Mark all blob bytes as properly received by {@link #write}, indicating that {@link #close} may finalize the blob.
     */
    public void markSuccess() {
        this.successful = true;
    }

    /**
     * Finish writing the current buffer contents to storage and track them by the given {@code partId}. Depending on whether all contents
     * have already been written either prepare the write buffer for additional writes or release the buffer.
     *
     * @param partId part identifier to track for use when closing
     */
    protected final void finishPart(T partId) {
        written += buffer.size();
        parts.add(partId);
        buffer.close();
        // only need a new buffer if we're not done yet
        if (successful) {
            buffer = null;
        } else {
            buffer = new ReleasableBytesStreamOutput(bigArrays);
        }
    }

    protected abstract void maybeFlushBuffer() throws IOException;

    protected abstract void doClose() throws IOException;
}
