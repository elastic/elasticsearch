/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.indices.breaker.CircuitBreakerService;

/**
 * An implementation of {@link BigArrays} that does not allow any operations.  This is intended to be used by aggregations after we have
 * read a {@link BigArray} from an input stream, which will likely have given us a read-only instance.
 */
public class NoOpBigArrays extends BigArrays {

    public NoOpBigArrays(PageCacheRecycler recycler, CircuitBreakerService breakerService, String breakerName) {
        super(null, null, null);
    }

    @Override
    void adjustBreaker(long delta, boolean isDataAlreadyCreated) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public BigArrays withCircuitBreaking() {
        return this;
    }

    @Override
    public BigArrays withBreakerService(CircuitBreakerService breakerService) {
        return this;
    }

    @Override
    public CircuitBreakerService breakerService() {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public ByteArray newByteArray(long size, boolean clearOnResize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public ByteArray newByteArray(long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public ByteArray resize(ByteArray array, long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public ByteArray grow(ByteArray array, long minSize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public IntArray newIntArray(long size, boolean clearOnResize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public IntArray newIntArray(long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public IntArray resize(IntArray array, long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public IntArray grow(IntArray array, long minSize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public LongArray newLongArray(long size, boolean clearOnResize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public LongArray newLongArray(long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public LongArray resize(LongArray array, long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public LongArray grow(LongArray array, long minSize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public DoubleArray newDoubleArray(long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public DoubleArray resize(DoubleArray array, long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public DoubleArray grow(DoubleArray array, long minSize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public FloatArray newFloatArray(long size, boolean clearOnResize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public FloatArray newFloatArray(long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public FloatArray resize(FloatArray array, long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public FloatArray grow(FloatArray array, long minSize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public <T> ObjectArray<T> newObjectArray(long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public <T> ObjectArray<T> resize(ObjectArray<T> array, long size) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    public <T> ObjectArray<T> grow(ObjectArray<T> array, long minSize) {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }

    @Override
    protected boolean shouldCheckBreaker() {
        throw new UnsupportedOperationException("Attempt to use disabled instance");
    }
}
