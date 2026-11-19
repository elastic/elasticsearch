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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.List;

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
            SharedMinCompetitive mc = minCompetitive.get();
            try {
                return new SharedGlobalTopK(breaker, topCount, mc, this);
            } catch (Exception e) {
                Releasables.closeExpectNoException(mc);
                throw e;
            }
        }
    }

    private final Object lock = new Object();
    private final TopNQueue globalQueue;
    private final SharedMinCompetitive minCompetitive;
    private final CircuitBreaker breaker;

    private int publishCount;

    private SharedGlobalTopK(CircuitBreaker breaker, int topCount, SharedMinCompetitive minCompetitive, Supplier supplier) {
        super(supplier);
        this.breaker = breaker;
        this.minCompetitive = minCompetitive;
        this.globalQueue = TopNQueue.build(breaker, topCount);
    }

    /**
     * Adds the sort keys of rows that newly entered a driver's local queue since the last call
     * into the global heap, and publishes a new competitive bound if the heap is now full.
     *
     * <p><b>Caller contract</b>: {@code newKeys} must contain <em>only</em> keys that were not
     * passed in any previous call for this driver. Repeating a previously-contributed key inserts
     * a duplicate into the global heap, which can shift the published bound to a value that is too
     * tight, causing documents that belong in the top-N to be skipped.
     *
     * <p>In practice the caller ({@link TopNOperator}) tracks exactly which rows entered its local
     * queue since the last merge and passes only those keys here; rows already in the queue are
     * never re-passed.
     *
     * @param newKeys encoded sort keys for rows that entered the driver's local queue since the
     *                last call; an empty list is a no-op
     * @return {@code true} if a new competitive bound was published to {@link SharedMinCompetitive}
     */
    public boolean mergeKeys(List<BytesRef> newKeys) {
        if (newKeys.isEmpty()) {
            return false;
        }
        synchronized (lock) {
            for (BytesRef key : newKeys) {
                TopNRow copy = new TopNRow(breaker, key.length, 0);
                boolean success = false;
                try {
                    copy.keys.append(key);
                    TopNRow leftover = globalQueue.addRow(copy);
                    success = true;
                    if (leftover != null) {
                        leftover.close();
                    }
                } finally {
                    if (success == false) {
                        copy.close();
                    }
                }
            }
            return publishIfFull();
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

    int publishCount() {
        return publishCount;
    }

    @Override
    protected void closeSideChannel() {
        Releasables.closeExpectNoException((Releasable) globalQueue, minCompetitive);
    }
}
