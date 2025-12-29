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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;

import java.util.HashMap;
import java.util.Map;

class GroupedQueue implements TopNQueue {
    private final Map<BytesRef, UngroupedQueue> queuesByGroupKey = new HashMap<>();
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TopNQueue.class);
    private final CircuitBreaker breaker;
    private final int topCount;

    static TopNQueue build(CircuitBreaker breaker, int topCount) {
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
        return getQueue(groupedRow).add(groupedRow);
    }

    private UngroupedQueue getQueue(GroupedRow row) {
        BytesRef keyView = row.groupKey().bytesRefView();
        var result = queuesByGroupKey.get(keyView);
        if (result != null) {
            return result;
        }
        boolean success = false;
        UngroupedQueue newQueue = null;
        BytesRef key = null;
        try {
            newQueue = UngroupedQueue.build(breaker, topCount);
            breaker.addEstimateBytesAndMaybeBreak(keyView.length, "topn");
            key = BytesRef.deepCopyOf(keyView);
            queuesByGroupKey.put(key, newQueue);
            success = true;
            return newQueue;
        } finally {
            if (success == false) {
                if (key != null) {
                    breaker.addWithoutBreaking(-RamUsageEstimator.sizeOf(keyView.length));
                }
                Releasables.close(newQueue);
            }
        }
    }

    @Override
    @Nullable
    public Row pop() {
        if (size() == 0) {
            return null;
        }
        var iterator = queuesByGroupKey.entrySet().iterator();
        var next = iterator.next();
        var key = next.getKey();
        UngroupedQueue queue = next.getValue();
        if (queue.size() == 0) {
            throw new IllegalStateException("Invariant violation: empty queue in grouped queue");
        }
        var row = queue.pop();
        if (queue.size() == 0) {
            iterator.remove();
            breaker.addWithoutBreaking(-RamUsageEstimator.sizeOf(key.length));
            queue.close();
        }
        return row;
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;
        total += RamUsageEstimator.alignObjectSize(
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
        );
        return total;
    }

    @Override
    public void close() {
        Releasables.close(
            // Release all entries in the topn
            () -> breaker.addWithoutBreaking(-sizeOf(queuesByGroupKey.keySet())),
            Releasables.wrap(queuesByGroupKey.values())
        // Releasables.wrap(queuesByGroupKey.keySet()),
        // Release the array itself
        );
    }

    private static long sizeOf(Iterable<BytesRef> bytes) {
        long total = 0;
        for (BytesRef b : bytes) {
            total += b.length;
        }
        return total;
    }
}
