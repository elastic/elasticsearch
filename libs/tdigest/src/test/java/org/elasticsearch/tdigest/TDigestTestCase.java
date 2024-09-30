/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.metrics.MemoryTrackingTDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestArrays;
import org.elasticsearch.tdigest.arrays.TDigestByteArray;
import org.elasticsearch.tdigest.arrays.TDigestDoubleArray;
import org.elasticsearch.tdigest.arrays.TDigestIntArray;
import org.elasticsearch.tdigest.arrays.TDigestLongArray;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Base class for TDigest tests that require {@link TDigestArrays} instances.
 * <p>
 *     This class provides arrays that will be automatically closed after the test.
 *     It will also test that all memory have been freed, as the arrays use a counting CircuitBreaker.
 * </p>
 */
public abstract class TDigestTestCase extends ESTestCase {
    private final Collection<Releasable> trackedArrays = ConcurrentHashMap.newKeySet();

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
        Releasables.close(trackedArrays);
        trackedArrays.clear();
    }

    private <T extends Releasable> T register(T releasable) {
        trackedArrays.add(releasable);
        return releasable;
    }

    protected final class DelegatingTDigestArrays implements TDigestArrays {
        private final MemoryTrackingTDigestArrays delegate;

        DelegatingTDigestArrays() {
            this.delegate = new MemoryTrackingTDigestArrays(newLimitedBreaker(ByteSizeValue.ofMb(100)));
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
