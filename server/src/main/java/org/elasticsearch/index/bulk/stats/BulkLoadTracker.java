/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

public class BulkLoadTracker {
    public static final BulkLoadTracker NO_OP = new BulkLoadTracker(() -> 0L) {
        private static final Releasable EMPTY_RELEASABLE = () -> {};

        @Override
        public Releasable trackWriteLoad() {
            return EMPTY_RELEASABLE;
        }

        @Override
        public long totalIndexingTimeInNanos() {
            return 0L;
        }
    };

    private final LongSupplier relativeTimeSupplier;
    private final LongAdder shardBulkMetric = new LongAdder();

    private final Set<OnGoingOperationTracker> onGoingOperations = ConcurrentCollections.newConcurrentSet();

    public BulkLoadTracker(LongSupplier relativeTimeSupplier) {
        this.relativeTimeSupplier = relativeTimeSupplier;
    }

    public Releasable trackWriteLoad() {
        final var tracker = new OnGoingOperationTracker(relativeTimeSupplier.getAsLong());
        onGoingOperations.add(tracker);
        return tracker;
    }

    private void onOperationComplete(OnGoingOperationTracker tracker, long totalTime) {
        boolean removed = onGoingOperations.remove(tracker);
        assert removed;
        shardBulkMetric.add(totalTime);
    }

    // private void release(int slot) {
    // long ts;
    // synchronized (mutex) {
    // ts = startTime[slot];
    // startTime[slot] = 0;
    // freeSlots.add(slot);
    // }
    // // long ts;
    // // do {
    // // ts = outstandingRequestStartTime.get(reqId);
    // // } while (outstandingRequestStartTime.compareAndSet(reqId, ts, 0) == false);
    // //
    // // freeElement.add(reqId);
    //
    // if (ts > 0) {
    // long total = relativeTimeSupplier.getAsLong() - ts;
    // shardBulkMetric.add(total);
    // }
    // }

    class OnGoingOperationTracker implements Releasable {
        private final AtomicLong startTime;

        OnGoingOperationTracker(long startTime) {
            this.startTime = new AtomicLong(startTime);
        }

        @Override
        public void close() {
            long latestSample;
            do {
                latestSample = startTime.get();
            } while (latestSample != 0 && startTime.compareAndSet(latestSample, 0) == false);

            final long total = relativeTimeSupplier.getAsLong() - latestSample;
            onOperationComplete(this, total);
        }

        public long updateLatestSampleAndGetDelta() {
            long ts;
            long newStartTime;
            do {
                ts = startTime.get();
                newStartTime = relativeTimeSupplier.getAsLong();
            } while (ts != 0 && startTime.compareAndSet(ts, newStartTime) == false);
            return ts == 0 ? 0 : newStartTime - ts;
        }
    }

    public long totalIndexingTimeInNanos() {
        long totalTime = 0;
        for (OnGoingOperationTracker onGoingOperation : onGoingOperations) {
            totalTime += onGoingOperation.updateLatestSampleAndGetDelta();
        }
        // synchronized (mutex) {
        // for (int i = 0; i < 4; i++) {
        // long ts = startTime[i];
        // long now = relativeTimeSupplier.getAsLong();
        // if (ts > 0) {
        // startTime[i] = now;
        // }
        // if (ts > 0) {
        // totalTime += (now - ts);
        // }
        // // long startTime = outstandingRequestStartTime.get(i);
        // // if (startTime > 0) {
        // // long ts;
        // // do {
        // // ts = outstandingRequestStartTime.get(i);
        // // } while (ts > 0 && outstandingRequestStartTime.compareAndSet(i, ts, System.nanoTime()) == false);
        // // if (ts > 0) {
        // // totalTime += (System.nanoTime() - ts);
        // // }
        // // }
        // }
        // }
        shardBulkMetric.add(totalTime);

        return shardBulkMetric.sum();
    }
}
