/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.AbstractTestStorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test helper that simulates S3 / Apache HttpClient {@code ContentLengthInputStream} behaviour:
 * {@code close()} on a leftover of at most {@link DecompressingStorageObject#MAX_TRAILING_DRAIN_BYTES}
 * drains the rest of the body; a larger leftover (or an explicit {@link StorageObject#abortStream})
 * discards it. Keep the leftover threshold in sync with {@code TransientTypingInputStream.close()}.
 */
public final class DrainSimulatingStorageObject {

    private DrainSimulatingStorageObject() {}

    /** Mutable counters shared between a {@link #create} call and test assertions. */
    public static final class Tracking {
        public final AtomicLong bytesConsumed = new AtomicLong();
        /**
         * Sticky OR across every stream from this object. Drain-vs-abort is per-stream
         * ({@link DrainTrackingInputStream#isAborted()}); a later stream can still drain a small
         * tail after this is true. Use {@link #abortCalls} or the stream flag to tell streams apart.
         */
        public final AtomicBoolean aborted = new AtomicBoolean();
        /** Set when {@code close()} fires on any stream returned by this storage object. */
        public final AtomicBoolean closed = new AtomicBoolean();
        public final AtomicInteger abortCalls = new AtomicInteger();
        /**
         * Set when a caller's read returns {@code -1}: the point where Apache HttpClient returns the connection
         * to the pool. The fixture's own close-time drain does not set it.
         */
        public final AtomicBoolean endOfBodyRead = new AtomicBoolean();
        /**
         * {@link #endOfBodyRead} as of the first {@code abortStream}. {@code true} means the abort arrived after
         * the connection was released (a no-op on S3); {@code false} means it discarded the connection.
         */
        public final AtomicBoolean endOfBodyReadBeforeAbort = new AtomicBoolean();
        /**
         * If non-null, {@code close()} awaits this <em>before</em> the first drain read so a leftover
         * of at most {@link DecompressingStorageObject#MAX_TRAILING_DRAIN_BYTES} does not transfer a
         * chunk while blocked. Abort-on-close never reaches the drain path and does not wait.
         */
        public volatile CountDownLatch drainLatch;
    }

    public static StorageObject create(byte[] bytes, Tracking tracking) {
        return create(bytes, tracking, StoragePath.of("s3://bucket/test.data"));
    }

    public static StorageObject create(byte[] bytes, Tracking tracking, StoragePath path) {
        return new AbstractTestStorageObject() {
            @Override
            public InputStream newStream() {
                return new DrainTrackingInputStream(new ByteArrayInputStream(bytes), tracking);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int from = (int) position;
                int to = (int) Math.min(position + length, bytes.length);
                return new DrainTrackingInputStream(new ByteArrayInputStream(bytes, from, to - from), tracking);
            }

            @Override
            public void abortStream(InputStream stream) throws IOException {
                if (tracking.aborted.getAndSet(true) == false) {
                    tracking.endOfBodyReadBeforeAbort.set(tracking.endOfBodyRead.get());
                }
                if (stream instanceof DrainTrackingInputStream drain) {
                    drain.markAborted();
                }
                tracking.abortCalls.incrementAndGet();
                stream.close();
            }

            @Override
            public long length() {
                return bytes.length;
            }

            @Override
            public Instant lastModified() {
                return Instant.EPOCH;
            }

            @Override
            public boolean exists() {
                return true;
            }

            @Override
            public StoragePath path() {
                return path;
            }
        };
    }

    /**
     * Per-stream GET: drain-vs-abort is decided here, not by a shared object-level flag.
     * A later stream from the same object must still drain a small tail after an earlier abort.
     */
    static final class DrainTrackingInputStream extends InputStream {
        private final ByteArrayInputStream delegate;
        private final Tracking tracking;
        private final AtomicBoolean aborted = new AtomicBoolean();
        private final AtomicBoolean closed = new AtomicBoolean();

        DrainTrackingInputStream(ByteArrayInputStream delegate, Tracking tracking) {
            this.delegate = delegate;
            this.tracking = tracking;
        }

        void markAborted() {
            aborted.set(true);
            tracking.aborted.set(true);
        }

        boolean isAborted() {
            return aborted.get();
        }

        @Override
        public int read() {
            int b = delegate.read();
            if (b >= 0) {
                tracking.bytesConsumed.incrementAndGet();
            } else {
                tracking.endOfBodyRead.set(true);
            }
            return b;
        }

        @Override
        public int read(byte[] buf, int off, int len) {
            int n = delegate.read(buf, off, len);
            if (n > 0) {
                tracking.bytesConsumed.addAndGet(n);
            } else if (n < 0) {
                tracking.endOfBodyRead.set(true);
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            tracking.closed.set(true);
            if (closed.getAndSet(true)) {
                return;
            }
            if (aborted.get()) {
                return;
            }
            int unread = delegate.available();
            // Keep in sync with TransientTypingInputStream.close(): leftover above the pool-sized
            // tail aborts rather than draining. Same package as MAX_TRAILING_DRAIN_BYTES.
            if (unread > DecompressingStorageObject.MAX_TRAILING_DRAIN_BYTES) {
                markAborted();
                return;
            }
            if (unread == 0) {
                return;
            }
            CountDownLatch latch = tracking.drainLatch;
            if (latch != null) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while draining", e);
                }
            }
            byte[] drain = new byte[8192];
            int n;
            while ((n = delegate.read(drain)) != -1) {
                tracking.bytesConsumed.addAndGet(n);
            }
        }
    }
}
