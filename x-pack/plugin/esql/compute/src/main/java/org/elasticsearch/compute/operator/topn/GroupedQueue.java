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

import java.util.ArrayList;
import java.util.List;

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
     * Removes and returns all rows from all per-group queues.
     * For an ascending order, the first element will be the min element (or last in the
     * priority queue), and vice versa.
     */
    List<TopNRow> popAll() {
        List<TopNRow> allRows = new ArrayList<>(size());
        for (long i = 0; i < queues.size(); i++) {
            TopNQueue queue = queues.get(i);
            if (queue != null) {
                queue.popAllInto(allRows);
                queue.close();
                queues.set(i, null);
            }
        }
        allRows.sort((r1, r2) -> -r1.compareTo(r2));
        return allRows;
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
