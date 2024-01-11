/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Releasable;

/**
 * A driver context that doesn't support any interaction. Consider it as a place holder where we need a dummy driver context.
 */
final class ThrowingDriverContext extends DriverContext {
    ThrowingDriverContext() {
        super(new ThrowingBigArrays(), BlockFactory.getInstance(new NoopCircuitBreaker("throwing-context"), new ThrowingBigArrays()));
    }

    @Override
    public BigArrays bigArrays() {
        throw unsupported();
    }

    @Override
    public BlockFactory blockFactory() {
        throw unsupported();
    }

    @Override
    public boolean addReleasable(Releasable releasable) {
        throw unsupported();
    }

    @Override
    public void addAsyncAction() {
        throw unsupported();
    }

    static UnsupportedOperationException unsupported() {
        assert false : "ThrowingDriverContext doesn't support any interaction";
        throw new UnsupportedOperationException("ThrowingDriverContext doesn't support any interaction");
    }

    static class ThrowingBigArrays extends BigArrays {

        ThrowingBigArrays() {
            super(null, null, "fake");
        }

        @Override
        public ByteArray newByteArray(long size, boolean clearOnResize) {
            throw unsupported();
        }

        @Override
        public IntArray newIntArray(long size, boolean clearOnResize) {
            throw unsupported();
        }

        @Override
        public LongArray newLongArray(long size, boolean clearOnResize) {
            throw unsupported();
        }

        @Override
        public FloatArray newFloatArray(long size, boolean clearOnResize) {
            throw unsupported();
        }

        @Override
        public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
            throw unsupported();
        }
    }
}
