/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;

/**
 * A bounded min-heap of {@link TopNRow}s used to find the top-N rows by sort key.
 * Used both by {@link TopNOperator} (one global queue) and by {@link GroupedQueue}
 * (one queue per group).
 */
class TopNQueue extends PriorityQueue<TopNRow> implements Accountable, Releasable {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNQueue.class);

    private final CircuitBreaker breaker;
    final int topCount;

    /**
     * Track memory usage in the breaker then build the {@link TopNQueue}.
     */
    static TopNQueue build(CircuitBreaker breaker, int topCount) {
        breaker.addEstimateBytesAndMaybeBreak(sizeOf(topCount), "esql engine topn");
        return new TopNQueue(breaker, topCount);
    }

    private TopNQueue(CircuitBreaker breaker, int topCount) {
        super(topCount);
        this.breaker = breaker;
        this.topCount = topCount;
    }

    @Override
    protected boolean lessThan(TopNRow lhs, TopNRow rhs) {
        return lhs.compareTo(rhs) < 0;
    }

    /**
     * Attempts to insert a row into the queue.
     * @return {@code null} if the row was inserted into a non-full queue;
     *         the evicted row if the row replaced the current top;
     *         the input row itself if it was rejected (worse than all in the queue).
     */
    TopNRow addRow(TopNRow row) {
        if (size() < topCount) {
            add(row);
            return null;
        } else if (lessThan(top(), row)) {
            TopNRow evicted = top();
            updateTop(row);
            return evicted;
        }
        return row;
    }

    /**
     * Drains all rows from this queue into the given list.
     */
    void popAllInto(List<TopNRow> target) {
        while (size() > 0) {
            target.add(pop());
        }
    }

    @Override
    public String toString() {
        return size() + "/" + topCount;
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;
        total += RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
        );
        for (TopNRow r : this) {
            total += r == null ? 0 : r.ramBytesUsed();
        }
        return total;
    }

    @Override
    public void close() {
        Releasables.close(
            /*
             * Release all entries in the topn, nulling references to each row after closing them
             * so they can be GC immediately. Without this nulling very large heaps can race with
             * the circuit breaker itself. With this we're still racing, but we're only racing a
             * single row at a time. And single rows can only be so large. And we have enough slop
             * to live with being inaccurate by one row.
             */
            () -> {
                for (int i = 0; i < getHeapArray().length; i++) {
                    TopNRow row = (TopNRow) getHeapArray()[i];
                    if (row != null) {
                        row.close();
                        getHeapArray()[i] = null;
                    }
                }
            },
            () -> breaker.addWithoutBreaking(-sizeOf(topCount))
        );
    }

    static long sizeOf(int topCount) {
        long total = SHALLOW_SIZE;
        total += RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
        );
        return total;
    }
}
