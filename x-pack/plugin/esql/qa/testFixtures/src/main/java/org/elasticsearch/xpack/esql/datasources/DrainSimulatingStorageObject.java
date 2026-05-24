/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test helper that simulates S3 / Apache HttpClient {@code ContentLengthInputStream} behaviour:
 * {@code close()} on a partially-read stream drains all remaining bytes. The
 * {@link StorageObject#abortStream(InputStream)} override flips {@link Tracking#aborted} and
 * suppresses the drain — mirroring {@code ResponseInputStream.abort()}.
 */
public final class DrainSimulatingStorageObject {

    private DrainSimulatingStorageObject() {}

    /** Mutable counters shared between a {@link #create} call and test assertions. */
    public static final class Tracking {
        public final AtomicLong bytesConsumed = new AtomicLong();
        public final AtomicBoolean aborted = new AtomicBoolean();
        /** Set when {@code close()} fires on any stream returned by this storage object. */
        public final AtomicBoolean closed = new AtomicBoolean();
        public final AtomicInteger abortCalls = new AtomicInteger();
    }

    public static StorageObject create(byte[] bytes, Tracking tracking) {
        return create(bytes, tracking, StoragePath.of("s3://bucket/test.data"));
    }

    public static StorageObject create(byte[] bytes, Tracking tracking, StoragePath path) {
        return new StorageObject() {
            @Override
            public InputStream newStream() {
                return drainTrackingStream(new ByteArrayInputStream(bytes), tracking);
            }

            @Override
            public InputStream newStream(long position, long length) {
                int from = (int) position;
                int to = (int) Math.min(position + length, bytes.length);
                return drainTrackingStream(new ByteArrayInputStream(bytes, from, to - from), tracking);
            }

            @Override
            public void abortStream(InputStream stream) throws IOException {
                tracking.aborted.set(true);
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

    private static InputStream drainTrackingStream(ByteArrayInputStream delegate, Tracking tracking) {
        return new InputStream() {
            @Override
            public int read() {
                int b = delegate.read();
                if (b >= 0) {
                    tracking.bytesConsumed.incrementAndGet();
                }
                return b;
            }

            @Override
            public int read(byte[] buf, int off, int len) {
                int n = delegate.read(buf, off, len);
                if (n > 0) {
                    tracking.bytesConsumed.addAndGet(n);
                }
                return n;
            }

            @Override
            public void close() throws IOException {
                tracking.closed.set(true);
                if (tracking.aborted.get()) {
                    return;
                }
                byte[] drain = new byte[8192];
                int n;
                while ((n = delegate.read(drain)) != -1) {
                    tracking.bytesConsumed.addAndGet(n);
                }
            }
        };
    }
}
