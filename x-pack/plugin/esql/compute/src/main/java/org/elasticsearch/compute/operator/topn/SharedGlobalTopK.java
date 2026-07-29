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
import org.elasticsearch.core.Releasables;

import java.util.concurrent.locks.ReentrantLock;

import static org.elasticsearch.compute.operator.topn.TopNOperator.SMALL_NULL;

/**
 * Pilot: merge local {@link TopNQueue} snapshots from parallel drivers into one global top-K heap
 * and publish the global worst-kept sort key to {@link SharedMinCompetitive} for Lucene pruning.
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
     * Merge all rows currently in {@code local} into the global heap. Returns whether a new bound was published.
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
     * Final merge when a driver finishes. Always attempts to merge even if the local worst-kept is unchanged.
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

    private boolean publishIfFull() {
        if (globalQueue.size() < globalQueue.topCount) {
            return false;
        }
        BytesRef worstKept = globalQueue.top().keys.bytesRefView();
        if (minCompetitive.offer(worstKept)) {
            publishCount++;
            if (minCompetitive.configs().size() == 1) {
                SharedMinCompetitive.KeyConfig config = minCompetitive.configs().getFirst();
                if (config.nullsFirst() && worstKept.length > 0 && worstKept.bytes[worstKept.offset] == SMALL_NULL) {
                    minCompetitive.markNoFurtherCandidates();
                }
            }
            return true;
        }
        return false;
    }

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
        Releasables.closeExpectNoException(globalQueue);
    }
}
