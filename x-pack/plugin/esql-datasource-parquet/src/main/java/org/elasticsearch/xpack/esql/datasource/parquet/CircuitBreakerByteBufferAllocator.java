/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.bytes.ByteBufferAllocator;
import org.elasticsearch.common.breaker.CircuitBreaker;

import java.nio.ByteBuffer;

/**
 * A Parquet {@code ByteBufferAllocator} that uses a circuit breaker to manage memory usage.
 */
public class CircuitBreakerByteBufferAllocator implements ByteBufferAllocator {
    private final ByteBufferAllocator delegate;
    private final CircuitBreaker breaker;

    public CircuitBreakerByteBufferAllocator(ByteBufferAllocator delegate, CircuitBreaker breaker) {
        this.delegate = delegate;
        this.breaker = breaker;
    }

    @Override
    public ByteBuffer allocate(int i) {
        breaker.addEstimateBytesAndMaybeBreak(i, "parquet reader");
        return delegate.allocate(i);
    }

    @Override
    public void release(ByteBuffer byteBuffer) {
        delegate.release(byteBuffer);
        breaker.addWithoutBreaking(-byteBuffer.capacity());
    }

    @Override
    public boolean isDirect() {
        return delegate.isDirect();
    }
}
