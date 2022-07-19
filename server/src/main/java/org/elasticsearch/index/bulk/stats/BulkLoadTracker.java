/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.core.Releasable;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;
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

    // Shared across all write threads / stats collector thread
    private final Queue<Integer> freeSlots = new ConcurrentLinkedDeque<>(List.of(0, 1, 2, 3));
    private final long[] startTime = new long[4];
    private final Object mutex = new Object();

    private final LongSupplier relativeTimeSupplier;
    private final LongAdder shardBulkMetric = new LongAdder();

    public BulkLoadTracker(LongSupplier relativeTimeSupplier) {
        this.relativeTimeSupplier = relativeTimeSupplier;
    }

    public Releasable trackWriteLoad() {
        synchronized (mutex) {
            int slot = freeSlots.remove();
            long time = relativeTimeSupplier.getAsLong();
            startTime[slot] = time;
            // final boolean replaced = outstandingRequestStartTime.compareAndSet(slot, 0, System.nanoTime());
            return () -> release(slot);
        }
    }

    private void release(int slot) {
        long ts;
        synchronized (mutex) {
            ts = startTime[slot];
            startTime[slot] = 0;
            freeSlots.add(slot);
        }
        // long ts;
        // do {
        // ts = outstandingRequestStartTime.get(reqId);
        // } while (outstandingRequestStartTime.compareAndSet(reqId, ts, 0) == false);
        //
        // freeElement.add(reqId);

        if (ts > 0) {
            long total = relativeTimeSupplier.getAsLong() - ts;
            shardBulkMetric.add(total);
        }
    }

    public long totalIndexingTimeInNanos() {
        long totalTime = 0;
        synchronized (mutex) {
            for (int i = 0; i < 4; i++) {
                long ts = startTime[i];
                long now = relativeTimeSupplier.getAsLong();
                if (ts > 0) {
                    startTime[i] = now;
                }
                if (ts > 0) {
                    totalTime += (now - ts);
                }
                // long startTime = outstandingRequestStartTime.get(i);
                // if (startTime > 0) {
                // long ts;
                // do {
                // ts = outstandingRequestStartTime.get(i);
                // } while (ts > 0 && outstandingRequestStartTime.compareAndSet(i, ts, System.nanoTime()) == false);
                // if (ts > 0) {
                // totalTime += (System.nanoTime() - ts);
                // }
                // }
            }
        }
        shardBulkMetric.add(totalTime);

        return shardBulkMetric.sum();
    }
}
