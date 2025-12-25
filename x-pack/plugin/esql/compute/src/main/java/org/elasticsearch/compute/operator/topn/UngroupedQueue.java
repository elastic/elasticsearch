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

final class UngroupedQueue implements TopNQueue {
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(UngroupedQueue.class);

    private final PriorityQueue<Row> pq;
    private final CircuitBreaker breaker;
    private final int topCount;

    static UngroupedQueue build(CircuitBreaker breaker, int topCount) {
        breaker.addEstimateBytesAndMaybeBreak(sizeOf(topCount), "esql engine topn");
        return new UngroupedQueue(breaker, topCount);
    }

    private UngroupedQueue(CircuitBreaker breaker, int topCount) {
        // FIXME(gal, NOCOMMIT) Verify with Nik that composition over inheritance is fine here.
        this.pq = new PriorityQueue<>(topCount) {
            @Override
            protected boolean lessThan(Row r1, Row r2) {
                return TopNOperator.compareRows(r1, r2) < 0;
            }
        };
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
    public AddResult add(RowFiller rowFiller, int i, Row row, int spareValuesPreAllocSize) {
        if (size() < topCount) {
            // Heap not yet full, just add the element.
            rowFiller.writeValues(i, row);
            int newSpareValuesPreAllocSize = newPreAllocSize(row, spareValuesPreAllocSize);
            pq.add(row);
            return new AddResult(null, newSpareValuesPreAllocSize);
        } else if (TopNOperator.compareRows(pq.top(), row) < 0) {
            // Heap full BUT this node fits in it.
            Row nextSpare = pq.top();
            rowFiller.writeValues(i, row);
            int newSpareValuesPreAllocSize = newPreAllocSize(row, spareValuesPreAllocSize);
            pq.updateTop(row);
            return new AddResult(nextSpare, newSpareValuesPreAllocSize);
        }
        // Heap full AND this node does not fit in it.
        return null;
    }

    private static int newPreAllocSize(Row spare, int spareValuesPreAllocSize) {
        return Math.max(spare.values().length(), spareValuesPreAllocSize / 2);
    }

    @Override
    public Row pop() {
        return pq.pop();
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
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * (topCount + 1L)
        );
        return total;
    }
}
