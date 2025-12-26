/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

class GroupedQueue implements TopNQueue {
    private final Map<BytesRef, UngroupedQueue> queuesByGroupKey = new HashMap<>();
    private final SortedMap<BytesRef, Row> sortedRows = new TreeMap<>();
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNQueue.class);
    private final CircuitBreaker breaker;
    private final int topCount;

    static TopNQueue build(CircuitBreaker breaker, int topCount) {
        // FIXME(gal, NOCOMMIT) is this computation correct?
        breaker.addEstimateBytesAndMaybeBreak(sizeOf(topCount), "esql engine topn");
        return new GroupedQueue(breaker, topCount);
    }

    private GroupedQueue(CircuitBreaker breaker, int topCount) {
        this.breaker = breaker;
        this.topCount = topCount;
    }

    @Override
    public String toString() {
        return size() + "/" + topCount;
    }

    @Override
    public int size() {
        int totalSize = 0;
        for (var queue : queuesByGroupKey.values()) {
            totalSize += queue.size();
        }
        return totalSize;
    }

    @Override
    public Row add(Row row) {
        var groupedRow = (GroupedRow) row;
        BytesRef key = ((GroupedRow) row).groupKey().bytesRefView().clone();
        var queue = queuesByGroupKey.computeIfAbsent(key, unused -> UngroupedQueue.build(breaker, topCount));
        var result = queue.add(groupedRow);
        if (result != null) {
            if (result != row) {
                // We don't need to close the row from sortedRows because it's the same instance as the one in the queue.
                sortedRows.remove(key);
            } else {
                sortedRows.put(key, row);
            }
        }
        return result;
    }

    @Override
    public Row pop() {
        var result = sortedRows.pollFirstEntry();
        if (result == null) {
            return null;
        }
        var resultFromQueue = queuesByGroupKey.get(result.getKey()).pop();
        // The first entry in sortedRows has the first *global* key, and thus also has the first key in its own queue via Dictum de Omni (I
        // just wanted an excuse to use that phrase).
        if (result.getValue() != resultFromQueue) {
            throw new IllegalStateException("Inconsistent state between sortedRows and queuesByGroupKey");
        }
        return resultFromQueue;
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;
        total += RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
        );
        for (Row r : sortedRows.values()) {
            total += r.ramBytesUsed();
        }
        return total;
    }

    @Override
    public void close() {
        Releasables.close(
            // Release all entries in the topn
            Releasables.wrap(queuesByGroupKey.values()),
            // Release the array itself
            () -> breaker.addWithoutBreaking(-sizeOf(topCount))
        );
    }

    private static long sizeOf(int topCount) {
        throw new AssertionError("TODO(gal) NOCOMMIT");
    }
}
