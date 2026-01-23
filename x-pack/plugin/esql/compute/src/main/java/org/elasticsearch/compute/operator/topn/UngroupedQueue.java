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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

final class UngroupedQueue implements TopNQueue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(UngroupedQueue.class) + shallowSizeOfInstance(PriorityQueue.class);

    private final PriorityQueueHack pq;
    private final CircuitBreaker breaker;
    private final int topCount;
    private Object[] heapForClosing;

    static UngroupedQueue build(CircuitBreaker breaker, int topCount) {
        breaker.addEstimateBytesAndMaybeBreak(sizeOf(topCount), "topn");
        return new UngroupedQueue(breaker, topCount);
    }

    // FIXME(gal, NOCOMMIT) yuck, hack
    private static class PriorityQueueHack extends PriorityQueue<Row> {
        PriorityQueueHack(int maxSize) {
            super(maxSize);
        }

        Object[] getHeapArrayHack() {
            return getHeapArray();
        }

        @Override
        protected boolean lessThan(Row r1, Row r2) {
            return TopNOperator.compareRows(r1, r2) < 0;
        }
    }

    private UngroupedQueue(CircuitBreaker breaker, int topCount) {
        // FIXME(gal, NOCOMMIT) Verify with Nik that composition over inheritance is fine here.
        this.pq = new PriorityQueueHack(topCount);
        this.breaker = breaker;
        this.topCount = topCount;
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
    public Row add(Row row) {
        if (size() < topCount) {
            // Heap not yet full, just add the element.
            pq.add(row);
            return null;
        } else if (TopNOperator.compareRows(pq.top(), row) < 0) {
            // Heap full BUT this node fits in it.
            Row evicted = pq.top();
            pq.updateTop(row);
            return evicted;
        }
        // Heap full AND this node does not fit in it.
        return row;
    }

    @Override
    public List<Row> popAll() {
        var results = new ArrayList<Row>(size());
        while (size() > 0) {
            results.add(pq.pop());
        }
        Collections.reverse(results);
        return results;
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;
        // TODO extract this to a constant.
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
            /*
             * Release all entries in the topn, nulling references to each row after closing them
             * so they can be GC immediately. Without this nulling very large heaps can race with
             * the circuit breaker itself. With this we're still racing, but we're only racing a
             * single row at a time. And single rows can only be so large. And we have enough slop
             * to live with being inaccurate by one row.
             */
            () -> {
                var heapArray = pq.getHeapArrayHack();
                for (int i = 0; i < heapArray.length; i++) {
                    Row row = (Row) heapArray[i];
                    if (row != null) {
                        row.close();
                        heapArray[i] = null;
                    }
                }
            },

            // Release the array itself
            () -> breaker.addWithoutBreaking(-sizeOf(topCount))
        );
    }

    private static long sizeOf(int topCount) {
        long total = SHALLOW_SIZE;
        total += RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * (topCount + 1L)
        );
        return total;
    }
}
