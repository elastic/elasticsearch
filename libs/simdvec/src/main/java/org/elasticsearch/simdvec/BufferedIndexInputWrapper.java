/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Wraps an index input, buffering data as it reads. */
public final class BufferedIndexInputWrapper extends BufferedIndexInput implements MemorySegmentAccessInput {

    private IndexInput delegate;

    public static IndexInput wrap(String resourceDescription, IndexInput delegate, int bufferSize) {
        return new BufferedIndexInputWrapper(resourceDescription, delegate, bufferSize);
    }

    private BufferedIndexInputWrapper(String resourceDescription, IndexInput delegate, int bufferSize) {
        super(resourceDescription, bufferSize);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    protected void readInternal(ByteBuffer buffer) throws IOException {
        int length = buffer.remaining();
        if (buffer.hasArray()) {
            byte[] arr = buffer.array();
            int offset = buffer.arrayOffset() + buffer.position();
            delegate.readBytes(arr, offset, length); // TODOL: consider useBuffer false
            buffer.position(buffer.position() + length);
        } else {
            byte[] tmp = new byte[length];
            delegate.readBytes(tmp, 0, length);
            buffer.put(tmp);
        }
    }

    @Override
    protected void seekInternal(long pos) throws IOException {
        delegate.seek(pos);
    }

    @Override
    public long length() {
        return delegate.length();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public MemorySegment segmentSliceOrNull(long offset, long length) throws IOException {
        return delegate instanceof MemorySegmentAccessInput msai ? msai.segmentSliceOrNull(offset, length) : null;
    }

    @Override
    public BufferedIndexInputWrapper clone() {
        BufferedIndexInputWrapper clone = (BufferedIndexInputWrapper) super.clone();
        // Each clone must wrap its own delegate clone
        clone.delegate = delegate.clone();
        return clone;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        IndexInput slice = delegate.slice(sliceDescription, offset, length);
        return new BufferedIndexInputWrapper(sliceDescription, slice, getBufferSize());
    }
}
