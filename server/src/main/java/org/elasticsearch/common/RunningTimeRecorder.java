/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongSupplier;

/**
 * This class allows recording the total time spent running operations, it includes
 * the elapsed time since the last reading for all ongoing recorded operations.
 * This last property allows getting more fine-grained results and avoids skewing
 * results for long-running operations when the sampling of this information is
 * more frequent than the running operations.
 */
public class RunningTimeRecorder {
    public static final RunningTimeRecorder NO_OP = new RunningTimeRecorder(() -> 0L) {
        private static final Releasable EMPTY_RELEASABLE = () -> {};

        @Override
        public Releasable trackRunningTime() {
            return EMPTY_RELEASABLE;
        }

        @Override
        public long totalRunningTimeInNanos() {
            return 0L;
        }
    };

    private final LongSupplier relativeTimeSupplier;
    private final LongAdder totalRunningTimeInNanos = new LongAdder();

    private final Set<OperationRunningTimeRecorder> operationRunningTimeRecorders = ConcurrentCollections.newConcurrentSet();

    public RunningTimeRecorder(LongSupplier relativeTimeSupplier) {
        this.relativeTimeSupplier = relativeTimeSupplier;
    }

    /**
     * Starts recording the running time of an operation, the recording stops once the returned {@link Releasable} is
     * closed.
     * @return a {@link Releasable} that should be called once the operation is finished to stop the running time recording.
     */
    public Releasable trackRunningTime() {
        final var recorder = new OperationRunningTimeRecorder(relativeTimeSupplier.getAsLong());
        operationRunningTimeRecorders.add(recorder);
        return recorder;
    }

    private void onOperationComplete(OperationRunningTimeRecorder tracker, long runningTimeInNanos) {
        boolean removed = operationRunningTimeRecorders.remove(tracker);
        if (removed) {
            totalRunningTimeInNanos.add(runningTimeInNanos);
        }
    }

    private class OperationRunningTimeRecorder implements Releasable {
        private static final long FINISHED_OP_SENTINEL = Long.MIN_VALUE;

        private final AtomicLong latestReadingRelativeTime;

        private OperationRunningTimeRecorder(long relativeStartTime) {
            this.latestReadingRelativeTime = new AtomicLong(relativeStartTime);
        }

        @Override
        public void close() {
            final long previousReadingTime = latestReadingRelativeTime.getAndSet(FINISHED_OP_SENTINEL);
            assert previousReadingTime != FINISHED_OP_SENTINEL : previousReadingTime;

            onOperationComplete(this, relativeTimeSupplier.getAsLong() - previousReadingTime);
        }

        private long getPartialRunningTimeAndUpdateLatestReadingTime() {
            long previousReadingTime;
            long updatedReadingTime;
            do {
                previousReadingTime = latestReadingRelativeTime.get();
                updatedReadingTime = relativeTimeSupplier.getAsLong();
            } while (previousReadingTime != FINISHED_OP_SENTINEL
                && latestReadingRelativeTime.compareAndSet(previousReadingTime, updatedReadingTime) == false);
            return previousReadingTime == FINISHED_OP_SENTINEL ? 0 : updatedReadingTime - previousReadingTime;
        }
    }

    /**
     * Returns the total running time for all the recorded operations, it includes the time elapsed since the last
     * reading of all unfinished operations.
     */
    public long totalRunningTimeInNanos() {
        long unfinishedOperationsRunningTimeSinceLastReading = 0;
        for (OperationRunningTimeRecorder operationRunningTimeRecorder : operationRunningTimeRecorders) {
            unfinishedOperationsRunningTimeSinceLastReading += operationRunningTimeRecorder
                .getPartialRunningTimeAndUpdateLatestReadingTime();
        }

        totalRunningTimeInNanos.add(unfinishedOperationsRunningTimeSinceLastReading);
        return totalRunningTimeInNanos.sum();
    }

    public int inFlightOps() {
        return operationRunningTimeRecorders.size();
    }
}
