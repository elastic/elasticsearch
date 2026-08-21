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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * In-memory {@link StorageObject} that records every request it serves, so a test can assert on the shape of the
 * I/O rather than only on its result.
 * <p>
 * This is the instrument for the connection-reuse claim behind {@link ChunkedStorageInputStream}: a request is
 * poolable exactly when it is a closed range that is read to completion, so the counters a test cares about are
 * how many requests were issued, how long each one declared itself to be, and whether any of them had to be
 * aborted. Aborts are the failure signal — the probe path is expected to produce none.
 * <p>
 * {@code maxBytesPerReadCall} makes positional reads return short, which providers backed by a
 * {@code FileChannel} are entitled to do, so callers that assume a single call fills the buffer are caught here
 * rather than in production.
 */
final class RecordingStorageObject implements StorageObject {

    private final byte[] data;
    private final int maxBytesPerReadCall;
    private final StoragePath path;

    /** One {@code [position, requestedLength]} entry per {@link #readBytes} call, in call order. */
    final List<long[]> readBytesCalls = new CopyOnWriteArrayList<>();
    /** One {@code [position, requestedLength]} entry per {@link #newStream(long, long)} call, in call order. */
    final List<long[]> newStreamCalls = new CopyOnWriteArrayList<>();
    final AtomicInteger abortCalls = new AtomicInteger();

    RecordingStorageObject(byte[] data) {
        this(data, Integer.MAX_VALUE);
    }

    RecordingStorageObject(byte[] data, int maxBytesPerReadCall) {
        if (maxBytesPerReadCall <= 0) {
            throw new IllegalArgumentException("maxBytesPerReadCall must be positive, got: " + maxBytesPerReadCall);
        }
        this.data = data;
        this.maxBytesPerReadCall = maxBytesPerReadCall;
        this.path = StoragePath.of("s3://bucket/recording.data");
    }

    @Override
    public int readBytes(long position, ByteBuffer target) {
        readBytesCalls.add(new long[] { position, target.remaining() });
        if (position >= data.length) {
            return -1;
        }
        int available = (int) Math.min(data.length - position, target.remaining());
        int n = Math.min(available, maxBytesPerReadCall);
        target.put(data, (int) position, n);
        return n;
    }

    @Override
    public InputStream newStream(long position, long length) {
        newStreamCalls.add(new long[] { position, length });
        int from = (int) Math.min(position, data.length);
        int to = length == READ_TO_END ? data.length : (int) Math.min(position + length, data.length);
        return new ByteArrayInputStream(data, from, Math.max(0, to - from));
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        abortCalls.incrementAndGet();
        stream.close();
    }

    @Override
    public long length() {
        return data.length;
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
}
