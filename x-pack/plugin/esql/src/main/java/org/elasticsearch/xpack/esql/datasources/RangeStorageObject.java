/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.DirectBufferFactory;
import org.elasticsearch.xpack.esql.datasources.spi.DirectReadBuffer;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObjectMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.Executor;

/**
 * A view over a byte range of a delegate {@link StorageObject}.
 * Used for every {@link FileSplit} so format readers and splittable decompressors
 * only see the split's compressed byte span (including offset {@code 0}).
 */
class RangeStorageObject implements StorageObject {

    private final StorageObject delegate;
    private final long offset;
    private final long length;

    RangeStorageObject(StorageObject delegate, long offset, long length) {
        Check.notNull(delegate, "delegate must not be null");
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0, got: " + offset);
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0, got: " + length);
        }
        this.delegate = delegate;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public InputStream newStream() throws IOException {
        return delegate.newStream(offset, length);
    }

    @Override
    public InputStream newStream(long position, long rangeLength) throws IOException {
        if (rangeLength == READ_TO_END) {
            // READ_TO_END within this view means "to the end of the VIEW" — a bounded read of the delegate ending
            // at offset+length, not to the end of the underlying object. A position at/after the view's end is an
            // empty stream (matching the open-ended past-the-end contract), never a negative length.
            long remaining = length - position;
            return remaining <= 0 ? InputStream.nullInputStream() : delegate.newStream(Math.addExact(offset, position), remaining);
        }
        // Closed range: clamp to the view so an oversized request never reads past offset+length into the next
        // split's bytes — matching the READ_TO_END branch above and the by-length async sibling below.
        long closedRemaining = length - position;
        if (closedRemaining <= 0) {
            return InputStream.nullInputStream();
        }
        return delegate.newStream(Math.addExact(offset, position), Math.min(rangeLength, closedRemaining));
    }

    @Override
    public int readBytes(long position, ByteBuffer target) throws IOException {
        if (position >= length) {
            return -1;
        }
        // Cap the read to the view: a target larger than the view's remaining bytes would otherwise read past
        // offset+length into the next split. Shrink the target's limit so the delegate cannot overrun it.
        long viewRemaining = length - position;
        if (target.remaining() <= viewRemaining) {
            return delegate.readBytes(Math.addExact(offset, position), target);
        }
        int savedLimit = target.limit();
        target.limit(target.position() + Math.toIntExact(viewRemaining));
        try {
            return delegate.readBytes(Math.addExact(offset, position), target);
        } finally {
            target.limit(savedLimit);
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public Instant lastModified() throws IOException {
        return delegate.lastModified();
    }

    @Override
    public boolean exists() throws IOException {
        return delegate.exists();
    }

    @Override
    public StoragePath path() {
        return delegate.path();
    }

    @Override
    public void abortStream(InputStream stream) throws IOException {
        // Forward to the underlying StorageObject so providers like S3 can perform a
        // non-draining abort (e.g. Abortable.abort()). Falling through to the SPI default
        // stream.close() would drain the entire response body for partial reads.
        delegate.abortStream(stream);
    }

    @Override
    public void readBytesAsync(
        long position,
        long length,
        DirectBufferFactory factory,
        Executor executor,
        ActionListener<DirectReadBuffer> listener
    ) {
        if (position >= this.length) {
            // Allocate a zero-length buffer through the factory so the returned DirectReadBuffer
            // is direct and allocator-owned, consistent with the StorageObject.readBytesAsync
            // contract.
            try {
                listener.onResponse(factory.allocate(0));
            } catch (IOException e) {
                listener.onFailure(e);
            }
            return;
        }
        long cappedLength = Math.min(length, this.length - position);
        delegate.readBytesAsync(Math.addExact(offset, position), cappedLength, factory, executor, listener);
    }

    @Override
    public void readBytesAsync(long position, ByteBuffer target, Executor executor, ActionListener<Integer> listener) {
        if (position >= this.length) {
            listener.onResponse(-1);
            return;
        }
        long viewRemaining = this.length - position;
        if (target.remaining() <= viewRemaining) {
            delegate.readBytesAsync(Math.addExact(offset, position), target, executor, listener);
            return;
        }
        // Cap to the view (as the sync readBytes does); restore the caller's limit once the read completes.
        int savedLimit = target.limit();
        target.limit(target.position() + Math.toIntExact(viewRemaining));
        delegate.readBytesAsync(Math.addExact(offset, position), target, executor, ActionListener.wrap(n -> {
            target.limit(savedLimit);
            listener.onResponse(n);
        }, e -> {
            target.limit(savedLimit);
            listener.onFailure(e);
        }));
    }

    @Override
    public boolean supportsNativeAsync() {
        return delegate.supportsNativeAsync();
    }

    @Override
    public StorageObjectMetrics metrics() {
        return delegate.metrics();
    }

    @Override
    public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {
        delegate.attachMetrics(metrics, scheme);
    }

    StorageObject rawDelegate() {
        return delegate;
    }

    long offset() {
        return offset;
    }
}
