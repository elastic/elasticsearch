/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.operator.SideChannel;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.compute.operator.topn.TopNOperator.SMALL_NULL;

/**
 * Merges per-driver local {@link TopNQueue} snapshots into one global top-K heap and publishes
 * the global worst-kept sort key to {@link SharedMinCompetitive} for Lucene pruning once the
 * global heap is full.
 *
 * <p>The global merge fires more competitive bounds than per-driver publishing: with {@code LIMIT N}
 * and {@code D} drivers, each driver must accumulate {@code N} rows before publishing its own bound,
 * but the global heap reaches {@code N} as soon as any combination of drivers has contributed that
 * many rows in total.
 */
public final class SharedGlobalTopK extends SideChannel {

    public static final class Supplier extends SideChannel.Supplier<SharedGlobalTopK> {
        private final CircuitBreaker breaker;
        private final int topCount;
        private final SharedMinCompetitive.Supplier minCompetitive;

        public Supplier(CircuitBreaker breaker, int topCount, SharedMinCompetitive.Supplier minCompetitive) {
            this.breaker = breaker;
            this.topCount = topCount;
            this.minCompetitive = minCompetitive;
        }

        public int topCount() {
            return topCount;
        }

        @Override
        protected SharedGlobalTopK build() {
            return new SharedGlobalTopK(breaker, topCount, minCompetitive.get(), this);
        }
    }

    private final ReentrantLock lock = new ReentrantLock();
    private final TopNQueue globalQueue;
    private final SharedMinCompetitive minCompetitive;
    private final CircuitBreaker breaker;

    private int mergeAttempts;
    private int mergesSkippedUnchanged;
    private int publishCount;

    private SharedGlobalTopK(CircuitBreaker breaker, int topCount, SharedMinCompetitive minCompetitive, Supplier supplier) {
        super(supplier);
        this.breaker = breaker;
        this.minCompetitive = minCompetitive;
        this.globalQueue = TopNQueue.build(breaker, topCount);
    }

    /**
     * Merges all rows currently in {@code local} into the global heap and publishes a new bound
     * if the global heap is now full.
     *
     * <p>Skips the merge when the local heap's worst-kept row hasn't changed since the last merge
     * (dirty-skip optimization): if {@code lastMergedWorstKept} matches the current local top, the
     * local heap contributed nothing new and re-merging would be wasteful.
     *
     * @param local               the driver's local top-N queue (not modified, not closed here)
     * @param lastMergedWorstKept the worst-kept key from the last successful merge for this driver,
     *                            or {@code null} on the first call
     * @return {@code true} if a new competitive bound was published to {@link SharedMinCompetitive}
     */
    public boolean mergeLocalHeap(TopNQueue local, @Nullable BytesRef lastMergedWorstKept) {
        mergeAttempts++;
        if (local == null || local.size() == 0) {
            return false;
        }
        BytesRef localWorstKept = local.size() >= local.topCount ? local.top().keys.bytesRefView() : null;
        if (localWorstKept != null && lastMergedWorstKept != null && localWorstKept.equals(lastMergedWorstKept)) {
            mergesSkippedUnchanged++;
            return false;
        }
        lock.lock();
        try {
            for (TopNRow row : local) {
                TopNRow copy = copySortKeys(row);
                TopNRow leftover = globalQueue.addRow(copy);
                if (leftover != null) {
                    leftover.close();
                }
            }
            return publishIfFull();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Final merge when a driver finishes. Always attempts to merge even if the local worst-kept
     * hasn't changed, to ensure the global heap reflects the driver's final state.
     *
     * @param local the driver's local top-N queue (not modified, not closed here)
     * @return {@code true} if a new competitive bound was published to {@link SharedMinCompetitive}
     */
    public boolean mergeLocalHeapOnFinish(TopNQueue local) {
        mergeAttempts++;
        if (local == null || local.size() == 0) {
            return false;
        }
        lock.lock();
        try {
            for (TopNRow row : local) {
                TopNRow copy = copySortKeys(row);
                TopNRow leftover = globalQueue.addRow(copy);
                if (leftover != null) {
                    leftover.close();
                }
            }
            return publishIfFull();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Publishes the global worst-kept bound to {@link SharedMinCompetitive} if the global heap
     * has reached its capacity. Must be called while holding {@link #lock}.
     */
    private boolean publishIfFull() {
        if (globalQueue.size() < globalQueue.topCount) {
            return false;
        }
        BytesRef worstKept = globalQueue.top().keys.bytesRefView();
        boolean allNullUnderNullsFirst = markNoFurtherCandidatesIfSaturatedNulls(worstKept);
        if (minCompetitive.offer(worstKept)) {
            publishCount++;
            return true;
        }
        return allNullUnderNullsFirst;
    }

    /**
     * If the sort is single-key with {@code NULLS FIRST} and the global heap top is null, every
     * remaining non-null row is non-competitive. Marks the source as exhausted.
     */
    private boolean markNoFurtherCandidatesIfSaturatedNulls(BytesRef worstKept) {
        if (minCompetitive.configs().size() != 1) {
            return false;
        }
        SharedMinCompetitive.KeyConfig config = minCompetitive.configs().getFirst();
        if (config.nullsFirst() && worstKept.length > 0 && worstKept.bytes[worstKept.offset] == SMALL_NULL) {
            minCompetitive.markNoFurtherCandidates();
            return true;
        }
        return false;
    }

    /** Copies only the sort-key bytes from {@code source}, discarding value columns. */
    private TopNRow copySortKeys(TopNRow source) {
        TopNRow copy = new TopNRow(breaker, source.keys.length(), 0);
        copy.keys.append(source.keys.bytesRefView());
        return copy;
    }

    int mergeAttempts() {
        return mergeAttempts;
    }

    int mergesSkippedUnchanged() {
        return mergesSkippedUnchanged;
    }

    int publishCount() {
        return publishCount;
    }

    @Override
    protected void closeSideChannel() {
        Releasables.closeExpectNoException((Releasable) globalQueue);
    }
}
