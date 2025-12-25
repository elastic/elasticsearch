/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasables;

class UngroupedQueue implements Queue {
    private final PriorityQueue<Row> pq;
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Queue.class);
    private final CircuitBreaker breaker;
    private final int topCount;

    /**
     * Track memory usage in the breaker then build the {@link Queue}.
     */
    static Queue build(CircuitBreaker breaker, int topCount) {
        breaker.addEstimateBytesAndMaybeBreak(sizeOf(topCount), "esql engine topn");
        return new UngroupedQueue(breaker, topCount);
    }

    private UngroupedQueue(CircuitBreaker breaker, int topCount) {
        this.pq = new PriorityQueue<>(topCount) {
            @Override
            protected boolean lessThan(Row r1, Row r2) {
                return UngroupedQueue.this.lessThan(r1, r2);
            }
        };
        this.breaker = breaker;
        this.topCount = topCount;
    }

    @Override
    public boolean lessThan(Row r1, Row r2) {
        return TopNOperator.compareRows(r1, r2) < 0;
    }

    @Override
    public String toString() {
        return size() + "/" + topCount;
    }

    @Override
    public int size() {
        return pq.size();
    }

    @Override
    public Row add(Row element) {
        return pq.add(element);
    }

    @Override
    public int topCount() {
        return topCount;
    }

    @Override
    public Row updateTop(Row newTop) {
        return pq.updateTop(newTop);
    }

    @Override
    public Row pop() {
        return pq.pop();
    }

    @Override
    public Row top() {
        return pq.top();
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;
        total += RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
        );
        for (Row r : pq) {
            total += r == null ? 0 : r.ramBytesUsed();
        }
        return total;
    }

    @Override
    public void close() {
        Releasables.close(
            // Release all entries in the topn
            Releasables.wrap(pq),
            // Release the array itself
            () -> breaker.addWithoutBreaking(-sizeOf(topCount))
        );
    }

    private static long sizeOf(int topCount) {
        long total = SHALLOW_SIZE;
        total += RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
        );
        return total;
    }
}
