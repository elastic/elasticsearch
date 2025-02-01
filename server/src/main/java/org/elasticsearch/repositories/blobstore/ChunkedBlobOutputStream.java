/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.AlreadyClosedException;
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
     * List of identifiers of already written chunks.
     */
    protected final List<T> parts = new ArrayList<>();

    /**
     * Size of the write buffer above which it must be flushed to storage.
     */
    private final long maxBytesToBuffer;

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
     * Is set to {@code true} once this stream has been closed.
     */
    private boolean closed = false;

    /**
     * Number of bytes flushed to blob storage so far.
     */
    protected long flushedBytes = 0L;

    protected ChunkedBlobOutputStream(BigArrays bigArrays, long maxBytesToBuffer) {
        this.bigArrays = bigArrays;
        if (maxBytesToBuffer <= 0) {
            throw new IllegalArgumentException("maximum buffer size must be positive");
        }
        this.maxBytesToBuffer = maxBytesToBuffer;
        buffer = new ReleasableBytesStreamOutput(bigArrays);
    }

    @Override
    public final void write(int b) throws IOException {
        buffer.write(b);
        maybeFlushBuffer();
    }

    @Override
    public final void write(byte[] b, int off, int len) throws IOException {
        buffer.write(b, off, len);
        maybeFlushBuffer();
    }

    @Override
    public final void close() throws IOException {
        if (closed) {
            assert false : "this output stream should only be closed once";
            throw new AlreadyClosedException("already closed");
        }
        closed = true;
        try {
            if (successful) {
                onCompletion();
            } else {
                onFailure();
            }
        } finally {
            Releasables.close(buffer);
        }
    }

    /**
     * Mark all blob bytes as properly received by {@link #write}, indicating that {@link #close} may finalize the blob.
     */
    public final void markSuccess() {
        this.successful = true;
    }

    /**
     * Finish writing the current buffer contents to storage and track them by the given {@code partId}. Depending on whether all contents
     * have already been written either prepare the write buffer for additional writes or release the buffer.
     *
     * @param partId part identifier to track for use when closing
     */
    protected final void finishPart(T partId) {
        flushedBytes += buffer.size();
        parts.add(partId);
        buffer.close();
        // only need a new buffer if we're not done yet
        if (successful) {
            buffer = null;
        } else {
            buffer = new ReleasableBytesStreamOutput(bigArrays);
        }
    }

    /**
     * Write the contents of {@link #buffer} to storage. Implementations should call {@link #finishPart} at the end to track the chunk
     * of data just written and ready {@link #buffer} for the next write.
     */
    protected abstract void flushBuffer() throws IOException;

    /**
     * Invoked once all write chunks/parts are ready to be combined into the final blob. Implementations must invoke the necessary logic
     * for combining the uploaded chunks into the final blob in this method.
     */
    protected abstract void onCompletion() throws IOException;

    /**
     * Invoked in case writing all chunks of data to storage failed. Implementations should run any cleanup required for the already
     * written data in this method.
     */
    protected abstract void onFailure();

    private void maybeFlushBuffer() throws IOException {
        if (buffer.size() >= maxBytesToBuffer) {
            flushBuffer();
        }
    }
}
