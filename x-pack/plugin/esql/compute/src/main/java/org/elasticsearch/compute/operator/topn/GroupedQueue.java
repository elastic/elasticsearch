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
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.util.RamUsageEstimator.shallowSizeOfInstance;

/**
 * A queue that maintains a separate per-group priority queue, indexed by integer group IDs
 * assigned by a {@link org.elasticsearch.compute.aggregation.blockhash.BlockHash}.
 * Uses a {@link BigArrays}-backed {@link ObjectArray} for better performance and circuit
 * breaker integration.
 */
class GroupedQueue implements Accountable, Releasable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(GroupedQueue.class);

    private final CircuitBreaker breaker;
    private final BigArrays bigArrays;
    private final int topCount;
    private ObjectArray<PerGroupQueue> queues;

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
            PerGroupQueue queue = queues.get(i);
            if (queue != null) {
                totalSize += queue.size();
            }
        }
        return totalSize;
    }

    /**
     * Attempts to add the row to the appropriate per-group queue based on {@link GroupedRow#groupId}.
     * @return If the row was added and the queue was full, the evicted row.
     *         If the row was added and it wasn't full, {@code null}.
     *         If the row wasn't added, the input row.
     */
    GroupedRow addRow(GroupedRow row) {
        return getOrCreateQueue(row.groupId).addRow(row);
    }

    private PerGroupQueue getOrCreateQueue(long groupId) {
        if (groupId >= queues.size()) {
            queues = bigArrays.grow(queues, groupId + 1);
        }
        PerGroupQueue queue = queues.get(groupId);
        if (queue == null) {
            queue = PerGroupQueue.build(breaker, topCount);
            queues.set(groupId, queue);
        }
        return queue;
    }

    /**
     * Removes and returns all rows from all per-group queues.
     * For an ascending order, the first element will be the min element (or last in the
     * priority queue), and vice versa.
     */
    List<GroupedRow> popAll() {
        List<GroupedRow> allRows = new ArrayList<>(size());
        for (long i = 0; i < queues.size(); i++) {
            PerGroupQueue queue = queues.get(i);
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
                PerGroupQueue queue = queues.get(i);
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
                    PerGroupQueue queue = queues.get(i);
                    if (queue != null) {
                        queue.close();
                        queues.set(i, null);
                    }
                }
            }
        }, queues);
    }

    /**
     * A single-group priority queue backed by Lucene's PriorityQueue.
     */
    static final class PerGroupQueue extends PriorityQueue<GroupedRow> implements Accountable, Releasable {
        private static final long SHALLOW_SIZE = shallowSizeOfInstance(PerGroupQueue.class);

        private final CircuitBreaker breaker;
        private final int topCount;

        private PerGroupQueue(CircuitBreaker breaker, int topCount) {
            super(topCount);
            this.topCount = topCount;
            this.breaker = breaker;
        }

        static PerGroupQueue build(CircuitBreaker breaker, int topCount) {
            breaker.addEstimateBytesAndMaybeBreak(sizeOf(topCount), "topn");
            return new PerGroupQueue(breaker, topCount);
        }

        @Override
        protected boolean lessThan(GroupedRow lhs, GroupedRow rhs) {
            return lhs.compareTo(rhs) < 0;
        }

        GroupedRow addRow(GroupedRow row) {
            if (size() < topCount) {
                add(row);
                return null;
            } else if (lessThan(top(), row)) {
                GroupedRow evicted = top();
                updateTop(row);
                return evicted;
            }
            return row;
        }

        void popAllInto(List<GroupedRow> target) {
            while (size() > 0) {
                target.add(pop());
            }
        }

        @Override
        public long ramBytesUsed() {
            long total = SHALLOW_SIZE;
            total += RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * ((long) topCount + 1)
            );
            for (GroupedRow r : this) {
                total += r == null ? 0 : r.ramBytesUsed();
            }
            return total;
        }

        @Override
        public void close() {
            Releasables.close(() -> {
                var heapArray = getHeapArray();
                for (int i = 0; i < heapArray.length; i++) {
                    GroupedRow row = (GroupedRow) heapArray[i];
                    if (row != null) {
                        row.close();
                        heapArray[i] = null;
                    }
                }
            }, () -> breaker.addWithoutBreaking(-sizeOf(topCount)));
        }

        private static long sizeOf(int topCount) {
            long total = SHALLOW_SIZE;
            total += RamUsageEstimator.alignObjectSize(
                RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + RamUsageEstimator.NUM_BYTES_OBJECT_REF * (topCount + 1L)
            );
            return total;
        }
    }
}
