/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.WrapperTDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestByteArray;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;
import org.elasticsearch.tdigest.arrays.TDigestLongArray;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TDigestTestCase extends ESTestCase {
    private final Collection<Releasable> CREATED_ARRAYS = ConcurrentHashMap.newKeySet();

    /**
     * Create a new TDigestArrays instance with a limited breaker. This method may be called multiple times.
     *
     * <p>
     *     The arrays created by this method will be automatically released after the test.
     * </p>
     */
    protected DelegatingTDigestArrays arrays() {
        return new DelegatingTDigestArrays();
    }

    /**
     * Release all arrays before {@link ESTestCase} checks for unreleased bytes.
     */
    @After
    public void releaseArrays() {
        Releasables.close(CREATED_ARRAYS);
        CREATED_ARRAYS.clear();
    }

    private <T extends Releasable> T register(T releasable) {
        CREATED_ARRAYS.add(releasable);
        return releasable;
    }

    protected final class DelegatingTDigestArrays implements TDigestArrays {
        private final WrapperTDigestArrays delegate;

        DelegatingTDigestArrays() {
            this.delegate = new WrapperTDigestArrays(newLimitedBreaker(ByteSizeValue.ofMb(100)));
        }

        public TDigestDoubleArray newDoubleArray(double[] data) {
            return register(delegate.newDoubleArray(data));
        }

        @Override
        public TDigestDoubleArray newDoubleArray(int size) {
            return register(delegate.newDoubleArray(size));
        }

        public TDigestIntArray newIntArray(int[] data) {
            return register(delegate.newIntArray(data));
        }

        @Override
        public TDigestIntArray newIntArray(int size) {
            return register(delegate.newIntArray(size));
        }

        @Override
        public TDigestLongArray newLongArray(int size) {
            return register(delegate.newLongArray(size));
        }

        @Override
        public TDigestByteArray newByteArray(int initialSize) {
            return register(delegate.newByteArray(initialSize));
        }
    }
}
