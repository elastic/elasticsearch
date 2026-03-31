/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;
import static org.apache.lucene.util.RamUsageEstimator.alignObjectSize;
import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

/**
 * A queue that maintains a separate {@link TopNQueue} per group, indexed by group IDs.
 */
class GroupedQueue implements Accountable, Releasable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(GroupedQueue.class);

    private final CircuitBreaker breaker;
    private final BigArrays bigArrays;
    private final int topCount;
    private ObjectArray<TopNQueue> queues;

    GroupedQueue(CircuitBreaker breaker, BigArrays bigArrays, int topCount) {
        this.breaker = breaker;
        this.bigArrays = bigArrays;
        this.topCount = topCount;
        this.queues = bigArrays.newObjectArray(0);
    }

    @Override
    public String toString() {
        return size() + "/" + queues.size() + "/" + topCount;
    }

    int size() {
        int totalSize = 0;
        for (long i = 0; i < queues.size(); i++) {
            TopNQueue queue = queues.get(i);
            if (queue != null) {
                totalSize += queue.size();
            }
        }
        return totalSize;
    }

    TopNQueue getOrCreateQueue(long groupId) {
        if (groupId >= queues.size()) {
            queues = bigArrays.grow(queues, groupId + 1);
        }
        TopNQueue queue = queues.get(groupId);
        if (queue == null) {
            queue = TopNQueue.build(breaker, topCount);
            queues.set(groupId, queue);
        }
        return queue;
    }

    /**
     * Drains all rows from all per-group queues into the given list, closing each queue
     * as it is emptied. The list is then sorted in descending key order so that for an
     * ascending sort the first element is the minimum.
     */
    void popAllInto(List<TopNRow> target) {
        for (long i = 0; i < queues.size(); i++) {
            TopNQueue queue = queues.get(i);
            if (queue != null) {
                queue.popAllInto(target);
                queue.close();
                queues.set(i, null);
            }
        }
        // sort allocates an internal array for sorting
        long sortBytes = estimateArrayBytes(target.size());
        breaker.addEstimateBytesAndMaybeBreak(sortBytes, "grouped_queue");
        try {
            target.sort((r1, r2) -> -r1.compareTo(r2));
        } finally {
            breaker.addWithoutBreaking(-sortBytes);
        }
    }

    /**
     * Estimates the bytes used by an {@code Object[]} of the given capacity.
     */
    static long estimateArrayBytes(int size) {
        return alignObjectSize(NUM_BYTES_ARRAY_HEADER + (long) size * NUM_BYTES_OBJECT_REF);
    }

    @Override
    public long ramBytesUsed() {
        long total = SHALLOW_SIZE;
        if (queues != null) {
            total += queues.ramBytesUsed();
            for (long i = 0; i < queues.size(); i++) {
                TopNQueue queue = queues.get(i);
                if (queue != null) {
                    total += queue.ramBytesUsed();
                }
            }
        }
        return total;
    }

    @Override
    public void close() {
        Releasables.close(() -> {
            if (queues != null) {
                for (long i = 0; i < queues.size(); i++) {
                    TopNQueue queue = queues.get(i);
                    if (queue != null) {
                        queue.close();
                        queues.set(i, null);
                    }
                }
            }
        }, queues);
    }
}
