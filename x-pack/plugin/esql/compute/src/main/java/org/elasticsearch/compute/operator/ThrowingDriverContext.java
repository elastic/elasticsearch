/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Releasable;

public class ThrowingDriverContext extends DriverContext {
    public ThrowingDriverContext() {
        super(new ThrowingBigArrays(), BlockFactory.getNonBreakingInstance());
    }

    @Override
    public BigArrays bigArrays() {
        throw new AssertionError("should not reach here");
    }

    @Override
    public BlockFactory blockFactory() {
        throw new AssertionError("should not reach here");
    }

    @Override
    public boolean addReleasable(Releasable releasable) {
        throw new AssertionError("should not reach here");
    }

    static class ThrowingBigArrays extends BigArrays {

        ThrowingBigArrays() {
            super(null, null, "fake");
        }

        @Override
        public ByteArray newByteArray(long size, boolean clearOnResize) {
            throw new AssertionError("should not reach here");
        }

        @Override
        public IntArray newIntArray(long size, boolean clearOnResize) {
            throw new AssertionError("should not reach here");
        }

        @Override
        public LongArray newLongArray(long size, boolean clearOnResize) {
            throw new AssertionError("should not reach here");
        }

        @Override
        public FloatArray newFloatArray(long size, boolean clearOnResize) {
            throw new AssertionError("should not reach here");
        }

        @Override
        public DoubleArray newDoubleArray(long size, boolean clearOnResize) {
            throw new AssertionError("should not reach here");
        }
    }
}
