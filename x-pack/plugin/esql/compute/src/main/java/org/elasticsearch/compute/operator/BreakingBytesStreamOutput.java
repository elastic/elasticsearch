/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.StreamOutputHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

/**
 * A {@link StreamOutput} that writes into a contiguous byte array tracked by a {@link CircuitBreaker}.
 * <p>
 *     Useful to serialize state that will end up in a {@link BytesRef}, as the result can be read
 *     with {@link #bytesRefView()} without the extra, untracked copy that {@code BytesStreamOutput} makes.
 * </p>
 */
public class BreakingBytesStreamOutput extends StreamOutput implements Releasable, Accountable {
    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BreakingBytesStreamOutput.class);

    private final BreakingBytesRefBuilder delegate;
    private final CircuitBreaker breaker;

    /**
     * Build.
     * @param breaker the {@link CircuitBreaker} to check on resize
     * @param label the label reported by the breaker when it breaks
     */
    public BreakingBytesStreamOutput(CircuitBreaker breaker, String label) {
        this.breaker = breaker;
        BreakingBytesRefBuilder delegate = new BreakingBytesRefBuilder(breaker, label);
        boolean success = false;
        try {
            breaker.addEstimateBytesAndMaybeBreak(SHALLOW_SIZE, label);
            success = true;
        } finally {
            if (success == false) {
                Releasables.close(delegate);
            }
        }
        this.delegate = delegate;
    }

    /**
     * Discards the written bytes, keeping the allocated buffer.
     */
    public void clear() {
        delegate.clear();
    }

    /**
     * A view of the written bytes. The view is only valid until the next write or {@link #close()}.
     */
    public BytesRef bytesRefView() {
        return delegate.bytesRefView();
    }

    @Override
    public long position() {
        return delegate.length();
    }

    @Override
    public void writeByte(byte b) {
        delegate.append(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) {
        delegate.append(b, offset, length);
    }

    @Override
    public void writeString(String str) throws IOException {
        StreamOutputHelper.writeString(str, this);
    }

    @Override
    public void writeOptionalString(@Nullable String str) throws IOException {
        StreamOutputHelper.writeOptionalString(str, this);
    }

    @Override
    public void writeGenericString(String value) throws IOException {
        StreamOutputHelper.writeGenericString(value, this);
    }

    @Override
    public void flush() {
        // no-op
    }

    @Override
    public void close() {
        breaker.addWithoutBreaking(-SHALLOW_SIZE);
        delegate.close();
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + delegate.ramBytesUsed();
    }
}
