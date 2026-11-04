/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.Accountable;
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
 * A queue that maintains a separate {@link TopNQueue} per group, indexed by group IDs.
 * <p>
 * With ties, a group also keeps every row whose sort key equals that of its worst kept row once its queue is full,
 * like SQL's {@code FETCH FIRST n ROWS WITH TIES}: the group returns all rows ranked no worse than its n-th row.
 */
class GroupedQueue implements Accountable, Releasable {
    private static final long SHALLOW_SIZE = shallowSizeOfInstance(GroupedQueue.class);
    private static final long TIED_LIST_SHALLOW_SIZE = shallowSizeOfInstance(ArrayList.class);

    private final CircuitBreaker breaker;
    private final BigArrays bigArrays;
    private final int topCount;
    private ObjectArray<TopNQueue> queues;
    /**
     * Per group, the rows tied with the worst row of its full queue. {@code null} without ties.
     */
    private ObjectArray<List<TopNRow>> ties;

    GroupedQueue(CircuitBreaker breaker, BigArrays bigArrays, int topCount) {
        this(breaker, bigArrays, topCount, false);
    }

    GroupedQueue(CircuitBreaker breaker, BigArrays bigArrays, int topCount, boolean withTies) {
        this.breaker = breaker;
        this.bigArrays = bigArrays;
        this.topCount = topCount;
        this.queues = bigArrays.newObjectArray(0);
        this.ties = withTies ? bigArrays.newObjectArray(0) : null;
    }

    boolean withTies() {
        return ties != null;
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
        if (ties != null) {
            for (long i = 0; i < ties.size(); i++) {
                List<TopNRow> tied = ties.get(i);
                if (tied != null) {
                    totalSize += tied.size();
                }
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
     * Keeps {@code row}, whose sort key equals that of the worst row in its group's full queue.
     */
    void addTie(long groupId, TopNRow row) {
        assert ties != null : "ties are off";
        if (groupId >= ties.size()) {
            ties = bigArrays.grow(ties, groupId + 1);
        }
        List<TopNRow> tied = ties.get(groupId);
        if (tied == null) {
            tied = new ArrayList<>();
            ties.set(groupId, tied);
        }
        tied.add(row);
    }

    /**
     * Settles {@code evicted}, just replaced in its group's full queue by a better row: it stays as a tie while the
     * queue's worst row still ranks equal to it, otherwise it and the rows tied with it fall out.
     *
     * @return {@code evicted} when it fell out, for the caller to reuse, or {@code null} when it was kept
     */
    TopNRow evict(long groupId, TopNRow evicted) {
        if (ties == null) {
            return evicted;
        }
        if (evicted.compareTo(queues.get(groupId).top()) == 0) {
            addTie(groupId, evicted);
            return null;
        }
        if (groupId < ties.size()) {
            List<TopNRow> tied = ties.get(groupId);
            if (tied != null) {
                Releasables.close(tied);
                ties.set(groupId, null);
            }
        }
        return evicted;
    }

    /**
     * Removes and returns all rows from all per-group queues.
     * When {@code outputOrdering} is {@link GroupedTopNOperator.OutputOrdering#SORTED}, rows are sorted by sort key across all groups.
     * For an ascending order, the first element will be the min element (or last in the
     * priority queue), and vice versa.
     */
    List<TopNRow> popAll(GroupedTopNOperator.OutputOrdering outputOrdering) {
        List<TopNRow> allRows = new ArrayList<>(size());
        for (long i = 0; i < queues.size(); i++) {
            TopNQueue queue = queues.get(i);
            if (queue != null) {
                queue.popAllInto(allRows);
                queue.close();
                queues.set(i, null);
            }
        }
        if (ties != null) {
            for (long i = 0; i < ties.size(); i++) {
                List<TopNRow> tied = ties.get(i);
                if (tied != null) {
                    allRows.addAll(tied);
                    ties.set(i, null);
                }
            }
        }
        if (outputOrdering == GroupedTopNOperator.OutputOrdering.SORTED) {
            allRows.sort((r1, r2) -> -r1.compareTo(r2));
        }
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
        if (ties != null) {
            total += ties.ramBytesUsed();
            for (long i = 0; i < ties.size(); i++) {
                List<TopNRow> tied = ties.get(i);
                if (tied != null) {
                    total += TIED_LIST_SHALLOW_SIZE + RamUsageEstimator.alignObjectSize(
                        RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) RamUsageEstimator.NUM_BYTES_OBJECT_REF * tied.size()
                    );
                    for (TopNRow row : tied) {
                        total += row.ramBytesUsed();
                    }
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
        }, queues, () -> {
            if (ties != null) {
                for (long i = 0; i < ties.size(); i++) {
                    List<TopNRow> tied = ties.get(i);
                    if (tied != null) {
                        Releasables.close(tied);
                        ties.set(i, null);
                    }
                }
            }
        }, ties);
    }
}
