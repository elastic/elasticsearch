/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * Presents a {@link StorageObject} of known length as one ordered {@link InputStream} fed by a bounded
 * window of concurrent ranged reads.
 *
 * <p>A stream-only codec (gzip, zstd) cannot be entered in the middle, so it must consume the object
 * sequentially. Reading it through one GET caps the throughput at a single connection, which over a long
 * round trip is far below the link. This stream keeps up to {@code rangesInFlight} fixed-size ranges
 * outstanding through {@link StorageObject#startReadBytesAsync} and hands their bytes out strictly in
 * offset order, whatever order they complete in. Permits, retries, the pinned generation and cancellation
 * come from the decorators around the object; buffers are charged to the breaker behind the factory.
 *
 * <p>Not thread-safe for reads; {@link #close()} may be called from any thread, once reads have stopped
 * or to abandon them.
 */
final class RangeReadAheadInputStream extends InputStream {

    private final StorageObject object;
    private final long length;
    private final int rangeBytes;
    private final int rangesInFlight;
    private final DirectBufferFactory factory;
    private final Executor executor;

    private final ArrayDeque<Range> window = new ArrayDeque<>();
    private long nextOffset;
    private Range current;
    private volatile boolean closed;

    RangeReadAheadInputStream(
        StorageObject object,
        long length,
        int rangeBytes,
        int rangesInFlight,
        DirectBufferFactory factory,
        Executor executor
    ) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative, got: " + length);
        }
        if (rangeBytes <= 0 || rangesInFlight <= 0) {
            throw new IllegalArgumentException("rangeBytes and rangesInFlight must be positive");
        }
        this.object = Objects.requireNonNull(object);
        this.length = length;
        this.rangeBytes = rangeBytes;
        this.rangesInFlight = rangesInFlight;
        this.factory = Objects.requireNonNull(factory);
        this.executor = Objects.requireNonNull(executor);
    }

    @Override
    public int read() throws IOException {
        byte[] one = new byte[1];
        int n = read(one, 0, 1);
        return n < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        Objects.checkFromIndexSize(off, len, b.length);
        if (len == 0) {
            return 0;
        }
        ByteBuffer buf = nextBuffer();
        if (buf == null) {
            return -1;
        }
        int n = Math.min(len, buf.remaining());
        buf.get(b, off, n);
        return n;
    }

    @Override
    public int available() {
        ByteBuffer buf = current == null ? null : current.buffer;
        return buf == null ? 0 : buf.remaining();
    }

    /** Returns the buffer holding unread bytes of the head range, advancing the window; null at end of object. */
    private ByteBuffer nextBuffer() throws IOException {
        while (true) {
            if (closed) {
                throw new IOException("stream closed");
            }
            if (current != null) {
                if (current.buffer.hasRemaining()) {
                    return current.buffer;
                }
                current.release();
                current = null;
            }
            fill();
            current = window.poll();
            if (current == null) {
                return null;
            }
            current.await();
            if (current.buffer.remaining() != current.expected) {
                throw new IOException(
                    "short read of ["
                        + object.path()
                        + "]: expected ["
                        + current.expected
                        + "] bytes at offset ["
                        + current.offset
                        + "] but got ["
                        + current.buffer.remaining()
                        + "]"
                );
            }
        }
    }

    private void fill() throws IOException {
        while (window.size() < rangesInFlight && nextOffset < length) {
            int len = (int) Math.min(rangeBytes, length - nextOffset);
            Range range = new Range(nextOffset, len);
            // Enqueue before starting so close() cancels it even if start throws midway.
            window.add(range);
            nextOffset += len;
            range.start();
        }
    }

    @Override
    public void close() {
        Range[] toRelease;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            toRelease = window.toArray(new Range[0]);
            window.clear();
        }
        if (current != null) {
            current.release();
            current = null;
        }
        for (Range r : toRelease) {
            r.release();
        }
    }

    private final class Range implements ActionListener<DirectReadBuffer> {
        final long offset;
        final int expected;
        private Releasable cancel = () -> {};
        private DirectReadBuffer drb;
        private ByteBuffer buffer;
        private Exception failure;
        private boolean done;
        private boolean released;

        Range(long offset, int expected) {
            this.offset = offset;
            this.expected = expected;
        }

        void start() {
            Releasable handle = object.startReadBytesAsync(offset, expected, factory, executor, this);
            synchronized (this) {
                if (released) {
                    handle.close();
                } else {
                    cancel = handle;
                }
            }
        }

        @Override
        public void onResponse(DirectReadBuffer result) {
            synchronized (this) {
                if (released == false) {
                    drb = result;
                    buffer = result.buffer();
                    done = true;
                    notifyAll();
                    return;
                }
            }
            // Abandoned before the bytes arrived: give the breaker charge back.
            result.close();
        }

        @Override
        public void onFailure(Exception e) {
            synchronized (this) {
                failure = e;
                done = true;
                notifyAll();
            }
        }

        void await() throws IOException {
            synchronized (this) {
                while (done == false) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("interrupted while reading [" + object.path() + "] at offset [" + offset + "]", e);
                    }
                }
                if (failure != null) {
                    if (failure instanceof IOException ioe) {
                        throw new IOException(ioe.getMessage(), ioe);
                    }
                    throw new IOException("failed to read [" + object.path() + "] at offset [" + offset + "]", failure);
                }
            }
        }

        void release() {
            Releasable toCancel;
            DirectReadBuffer toClose;
            synchronized (this) {
                if (released) {
                    return;
                }
                released = true;
                toCancel = cancel;
                toClose = drb;
                drb = null;
                buffer = null;
            }
            toCancel.close();
            if (toClose != null) {
                toClose.close();
            }
        }
    }
}
