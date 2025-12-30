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

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

class GroupedQueue implements TopNQueue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(GroupedQueue.class) + shallowSizeOfInstance(HashMap.class);
    public static final long BYTES_REF_HEADER_SIZE = shallowSizeOfInstance(BytesRef.class) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;

    private final Map<BytesRef, UngroupedQueue> queuesByGroupKey = new HashMap<>();
    private final CircuitBreaker breaker;
    private final int topCount;

    static GroupedQueue build(CircuitBreaker breaker, int topCount) {
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
            // FIXME(gal, NOCOMMIT) Inaccurate count here.
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
        UngroupedQueue queue = next.getValue();
        if (queue.size() == 0) {
            throw new IllegalStateException("Invariant violation: empty queue in grouped queue");
        }
        var row = queue.pop();
        if (queue.size() == 0) {
            // FIXME(gal, NOCOMMIT) Inaccurate count here.
            breaker.addWithoutBreaking(-RamUsageEstimator.sizeOf(next.getKey().length));
            iterator.remove();
            queue.close();
        }
        return row;
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;

        long entrySize = 0;
        for (var entry : queuesByGroupKey.entrySet()) {
            total += entry.getValue().ramBytesUsed();
            total += alignObjectSize(entry.getKey().length + BYTES_REF_HEADER_SIZE);
            if (entrySize == 0) {
                entrySize = shallowSizeOfInstance(entry.getClass());
            }
            total += entrySize;
            // Account for unused entries in the map's table, assuming current load of 0.5.
            total += 2L * NUM_BYTES_OBJECT_REF;
        }

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
