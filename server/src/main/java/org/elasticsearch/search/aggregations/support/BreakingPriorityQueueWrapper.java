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
 * A wrapper around Lucene's PriorityQueue which also tracks memory usage via circuit
 * breakers.
 *
 * Lucenes PriorityQueue allocates an array of `size` immediately upon instantiation.
 * This is potentially very dangerous though if the supplied size is large, and
 * the caller doesn't correctly account for circuit breaker allocation and de-allocation.
 *
 * This class wraps the PQ so that the circuit breaker is correctly called before
 * and after de-allocation.
 *
 * NOTE: this class has _no null checks_ after the queue has been de-allocated (in
 * order to preserve performance).  Caller must not use the queue after
 * {@link BreakingPriorityQueueWrapper#close()} is called otherwise an NPE will
 * be thrown.
 */
public abstract class BreakingPriorityQueueWrapper<T> implements Releasable {
    private PriorityQueue<T> pq;
    private final long size;
    private final Consumer<Long> circuitBreaker;

    public BreakingPriorityQueueWrapper(int size, Consumer<Long> circuitBreaker) {
        // Account for PQ since it can be non-negligible if there are many values
        // (e.g. min_doc_count: 0 on high cardinality field).  PQ allocates `size` objects
        circuitBreaker.accept((long) size * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        this.size = size;
        this.circuitBreaker = circuitBreaker;

        this.pq = new PriorityQueue<>(size) {
            @Override
            protected boolean lessThan(T a, T b) {
                return BreakingPriorityQueueWrapper.this.lessThan(a, b);
            }
        };

    }

    protected abstract boolean lessThan(T a, T b);

    public final T add(T element) {
        return pq.add(element);
    }

    public T insertWithOverflow(T element) {
        return pq.insertWithOverflow(element);
    }

    public final int size() {
        return pq.size();
    }

    public final T pop() {
        return pq.pop();
    }

    public final T top() {
        return pq.top();
    }

    public final T updateTop() {
        return pq.updateTop();
    }

    @Override
    public void close() {
        if (pq != null) {
            this.pq = null;
            circuitBreaker.accept(-size * RamUsageEstimator.NUM_BYTES_OBJECT_REF);
        }
    }
}
