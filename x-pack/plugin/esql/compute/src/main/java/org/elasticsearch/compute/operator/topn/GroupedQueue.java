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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

// Note: This is a temporary implementation. The "real" implementation, to be done later, will use a BlockHash instead of a HashMap.
class GroupedQueue implements TopNQueue {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(GroupedQueue.class) + shallowSizeOfInstance(HashMap.class);
    public static final long BYTES_REF_HEADER_SIZE = shallowSizeOfInstance(BytesRef.class);

    private final Map<BytesRef, UngroupedQueue> queuesByGroupKey = new HashMap<>(0);
    private final CircuitBreaker breaker;
    private final int topCount;

    GroupedQueue(CircuitBreaker breaker, int topCount) {
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
    public Row addRow(Row row) {
        var groupedRow = (GroupedRow) row;
        return getQueue(groupedRow).addRow(groupedRow);
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
                    breaker.addWithoutBreaking(-keyView.length);
                }
                if (newQueue != null) {
                    newQueue.close();
                }
            }
        }
    }

    @Override
    public List<Row> popAll() {
        List<Row> allRows = new ArrayList<>(size());
        var iterator = queuesByGroupKey.entrySet().iterator();
        while (iterator.hasNext()) {
            var next = iterator.next();
            try (UngroupedQueue queue = next.getValue()) {
                queue.popAllInto(allRows);
                breaker.addWithoutBreaking(-next.getKey().length);
                iterator.remove();
            }
        }
        // TODO this sorts all rows across all groups using the main sort key, ignoring the individual groups. We *might* want to sort only
        // within each group.
        allRows.sort((r1, r2) -> -TopNOperator.compareRows(r1, r2));
        return allRows;
    }

    // Note: This implementation is temporary anyway, so this might not be entirely accurate, but that's fine.
    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;

        for (var entry : queuesByGroupKey.entrySet()) {
            total += entry.getValue().ramBytesUsed();
            total += alignObjectSize(entry.getKey().length + BYTES_REF_HEADER_SIZE);
            total += HASH_MAP_NODE_SIZE;
            // Account for unused entries in the map's table, assuming current load of 0.5.
            total += 2L * NUM_BYTES_OBJECT_REF;
        }

        return total;
    }

    @Override
    public void close() {
        Releasables.close(
            () -> breaker.addWithoutBreaking(-sizeOf(queuesByGroupKey.keySet())),
            Releasables.wrap(queuesByGroupKey.values())
        );
    }

    private static long sizeOf(Iterable<BytesRef> bytes) {
        long total = 0;
        for (BytesRef b : bytes) {
            total += b.length;
        }
        return total;
    }

    // Visible for testing.
    static final long HASH_MAP_NODE_SIZE;

    static {
        var map = new HashMap<Integer, Integer>();
        map.put(0, 0);
        HASH_MAP_NODE_SIZE = RamUsageEstimator.shallowSizeOfInstance(map.entrySet().iterator().next().getClass());
    }
}
