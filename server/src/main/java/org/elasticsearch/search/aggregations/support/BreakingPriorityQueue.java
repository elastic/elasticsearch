/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support;

import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lease.Releasable;

import java.util.function.Consumer;

/**
 * A thin extension of Lucene's PriorityQueue which also tracks memory usage via circuit
 * breakers.
 *
 * Lucenes PriorityQueue allocates an array of `size` immediately upon instantiation.
 * This is potentially very dangerous though if the supplied size is large, and
 * the caller doesn't correctly account for circuit breaker allocation and de-allocation.
 *
 * This class ensures that a breaker is called prior to the PQ being initialized, and
 * again on deallocation
 *
 */
public abstract class BreakingPriorityQueue<T> extends PriorityQueue<T> implements Releasable {
    private final int size;
    private final Consumer<Long> circuitBreaker;

    public BreakingPriorityQueue(int size, Consumer<Long> circuitBreaker) {
        super(applyBreaker(size, circuitBreaker));
        this.size = size;
        this.circuitBreaker = circuitBreaker;
    }

    protected abstract boolean lessThan(T a, T b);

    private static int applyBreaker(int size, Consumer<Long> breaker) {
        breaker.accept((long)size * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        return size;
    }

    @Override
    public void close() {
        circuitBreaker.accept((long) -size * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
    }
}
