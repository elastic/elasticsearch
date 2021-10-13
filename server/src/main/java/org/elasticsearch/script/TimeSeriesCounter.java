/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * A counter that keeps a time series history of (resolutionSecs * numEpochs) seconds.
 * {@code inc} always updates the accumulator for the current epoch.  If the given timestamp indicates
 * an epoch rollover, the previous value of the accumulator is stored in the appropriate epoch.
 */
public class TimeSeriesCounter {
    public static final LongSupplier SYSTEM_SECONDS_TIME_PROVIDER = () -> System.currentTimeMillis() / 1000;
    public static final int MINUTE = 60;
    public static final int HOUR = 60 * MINUTE;

    protected final long resolutionSecs;
    protected final int[] epochs;

    // TOOD(stu): Use ClusterService.getClusterApplierService().threadPool().absoluteTimeInMillis()
    protected final LongSupplier timeProvider;

    protected final ReentrantReadWriteLock lock;

    protected long currentEpochStart;
    protected final LongAdder currentEpochAdder;
    protected final TimeSeriesCounter currentEpochTimeSeries;

    TimeSeriesCounter(int numEpochs, int resolutionSecs, LongSupplier timeProvider) {
        this(numEpochs, resolutionSecs, timeProvider, new ReentrantReadWriteLock());
    }

    private TimeSeriesCounter(int numEpochs, int resolutionSecs, LongSupplier timeProvider, ReentrantReadWriteLock lock) {
        assert numEpochs > 0;
        assert resolutionSecs > 0;
        this.epochs = new int[numEpochs];
        this.resolutionSecs = resolutionSecs;
        this.timeProvider = timeProvider;
        this.lock = lock;
        this.currentEpochAdder = new LongAdder();
        this.currentEpochTimeSeries = null;
    }

    TimeSeriesCounter(int numEpochs, int resolutionSecs, LongSupplier timeProvider, int subNumEpochs, int subResolutionSecs) {
        assert numEpochs > 0;
        assert resolutionSecs > 0;
        if (subNumEpochs * subResolutionSecs != resolutionSecs) {
            throw new IllegalArgumentException("sub counter with"
                + " resolution [" + subResolutionSecs + "] and numEpochs [" + subNumEpochs + "]"
                + " does not cover one epoch for TimeSeriesCounter with "
                + " resolution [" + resolutionSecs + "] and numEpochs [" + numEpochs + "]");
        }
        this.epochs = new int[numEpochs];
        this.resolutionSecs = resolutionSecs;
        this.timeProvider = timeProvider;
        this.lock = new ReentrantReadWriteLock();
        this.currentEpochAdder = null;
        this.currentEpochTimeSeries = new TimeSeriesCounter(subNumEpochs, subResolutionSecs, timeProvider, null);
    }

    public static TimeSeriesCounter nestedCounter(int numEpochs, int resolutionSecs, int subNumEpochs, int subResolutionSecs) {
        return new TimeSeriesCounter(numEpochs, resolutionSecs, SYSTEM_SECONDS_TIME_PROVIDER, subNumEpochs, subResolutionSecs);
    }

    /**
     * Increment the counter at the current time
     */
    public void inc() {
        inc(timeProvider.getAsLong());
    }

    /**
     * Increment the counter at the given time
     */
    public void inc(long nowSecs) {
        assert nowSecs >= 0;
        if (lock != null) {
            lock.writeLock().lock();
        }
        try {
            int gap = epochsBetween(nowSecs);

            if (gap < -1) {  // Excessive negative gap
                start(nowSecs);
            } else if (gap == -1) {  // Clamp small negative jitter to current epoch
                incAccumulator(currentEpochStart);
            } else if (gap == 0) {
                incAccumulator(nowSecs);
            } else if (gap > epochs.length) { // Initialization or history expired
                start(nowSecs);
            } else {
                int currentEpochIndex = storeAccumulator();
                for (int i = 1; i <= gap; i++) {
                    epochs[(currentEpochIndex + i) % epochs.length] = 0;
                }
                incAccumulator(nowSecs);
                currentEpochStart = epochStartMillis(nowSecs);
            }
        } finally {
            if (lock != null) {
                lock.writeLock().unlock();
            }
        }
    }

    /**
     * Get the current timestamp from the timeProvider, for consistent reads across different counters
     */
    public long timestamp() {
        return timeProvider.getAsLong();
    }

    /**
     * Get the counts at time {@code now} for each timePeriod, as number of seconds ago until now.
     */
    public int[] counts(long now, int ... timePeriods) {
        if (lock != null) {
            lock.readLock().lock();
        }
        try {
            int[] countsForPeriod = new int[timePeriods.length];
            for (int i = 0; i < countsForPeriod.length; i++) {
                countsForPeriod[i] = count(now - timePeriods[i], now);
            }
            return countsForPeriod;
        } finally {
            if (lock != null) {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * Get the counts for each timePeriod (as seconds ago) in timePeriods
     */
    public int[] counts(int ... timePeriods) {
        return counts(timeProvider.getAsLong(), timePeriods);
    }

    /**
     * Count the entire contents of the time series, for testing
     */
    int count() {
        if (lock != null) {
            lock.readLock().lock();
        }
        try {
            int count = 0;
            for (int j : epochs) {
                count += j;
            }
            return count + currentEpochCount();
        } finally {
            if (lock != null) {
                lock.readLock().unlock();
            }
        }
    }

    /**
     * Get the count between two times, clamped to the resolution of the counters.
     */
    protected int count(long start, long end) {
        if (end < start) {
            throw new IllegalArgumentException("start [" + start + "] must be before end [" + end + "]");
        }

        // Clamp to range
        long earliestTimeStamp = beginningOfTimeSeries();
        if (start < earliestTimeStamp) {
            if (end < earliestTimeStamp) {
                return 0;
            }
            start = earliestTimeStamp;
        }
        long latestTimeStamp = endOfTimeSeries();
        if (end > latestTimeStamp) {
            if (start > latestTimeStamp) {
                return 0;
            }
            end = latestTimeStamp;
        }

        int total = 0;
        // handle the current bucket
        if (start <= latestTimeStamp && end >= currentEpochStart) {

        }
        if (end >= currentEpochStart) {
            if (currentEpochTimeSeries != null) {
                total += currentEpochTimeSeries.count(currentEpochStart, end);
            } else {
                total += currentEpochAdderCount();
            }
            end = currentEpochStart - 1;
            // only covers one bucket
            if (end < start) {
                return total;
            }
        }

        // handle the rest of the buckets, end guaranteed to stop before current bucket
        int numEpochs = epochsBetween(start, end);
        if (numEpochs < 0 || numEpochs >= epochs.length) {
            return 0;
        }
        int startEpoch = epochIndex(start);
        for (int i = 0; i < numEpochs; i++) {
            total += epochs[(startEpoch + i) % epochs.length];
        }
        return total;
    }

    /**
     * The earliest millisecond valid for this time series.
     */
    public long beginningOfTimeSeries() {
        return currentEpochStart - (resolutionSecs * (epochs.length - 1));
    }

    /**
     * The latest millisecond valid for this time series.
     */
    public long endOfTimeSeries() {
        return currentEpochStart + resolutionSecs - 1;
    }

    long getCurrentEpochStart() {
        return currentEpochStart;
    }

    /**
     * reset the accumulator and all arrays
     */
    protected void reset() {
        clearAccumulator();
        Arrays.fill(epochs, 0);
    }

    /**
     * get the count of the current epoch accumulator, long adder or nested TimeSeriesCounter
     */
    protected int currentEpochCount() {
        if (currentEpochAdder != null) {
            long sum = currentEpochAdder.sum();
            if (sum > Integer.MAX_VALUE) {
                return Integer.MAX_VALUE;
            }
            return (int) sum;
        } else {
            assert currentEpochTimeSeries != null;
            return currentEpochTimeSeries.count();
        }
    }

    protected int currentEpochAdderCount() {
        if (currentEpochAdder == null) {
            return 0;
        }
        long sum = currentEpochAdder.sum();
        if (sum > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else if (sum < 0) {
            return 0;
        }
        return (int) sum;
    }

    /**
     * increment current epoch accumulator, long adder or nested TimeSeriesCounter
     */
    protected void incAccumulator(long time) {
        if (currentEpochAdder != null) {
            currentEpochAdder.increment();
        } else {
            assert currentEpochTimeSeries != null;
            currentEpochTimeSeries.inc(time);
        }
    }

    /**
     * clear the current epoch accumulator, long adder or nested TimeSeriesCounter
     */
    protected void clearAccumulator() {
        if (currentEpochAdder != null) {
            currentEpochAdder.reset();
        } else {
            assert currentEpochTimeSeries != null;
            currentEpochTimeSeries.reset();
        }
    }

    /**
     * How many milliseconds of history does this {@code TimeSeriesCounter} cover?
     */
    protected long duration() {
        return epochs.length * resolutionSecs;
    }

    /**
     * Index in the epoch array for the given time
     */
    protected int epochIndex(long millis) {
        return (int)((millis / resolutionSecs) % epochs.length);
    }

    /**
     * The beginning of the epoch given by {@code nowMillis}
     */
    protected long epochStartMillis(long nowMillis) {
        return (nowMillis / resolutionSecs) * resolutionSecs;
    }

    /**
     * Starts the TimeSeries at {@code nowMillis}
     */
    protected void start(long nowMillis) {
        reset();
        currentEpochStart = epochStartMillis(nowMillis);
        incAccumulator(nowMillis);
    }

    /**
     * Store the {@code currentEpochAccumulator} into the {@code epoch} history array at the appropriate index before
     * moving on to a new epoch.
     *
     * This overrides the previous value in that index (expected to be zero), so calling this multiple times for the
     * same epoch will lose counts.
     */
    protected int storeAccumulator() {
        int currentEpochIndex = epochIndex(currentEpochStart);
        epochs[currentEpochIndex] = currentEpochCount();
        clearAccumulator();
        return currentEpochIndex;
    }

    /**
     * The number of epochs between {@code currentEpochStart} and the given time.  Clamped to the range [Int.MAX, Int.Min].
     */
    protected int epochsBetween(long nowMillis) {
        return epochsBetween(currentEpochStart, nowMillis);
    }

    /**
     * The number of epochs between {@code start} and {@code end}.  Clamped to the range [Int.MAX, Int.Min].
     */
    protected int epochsBetween(long start, long end) {
        long gap = (end / resolutionSecs) - (start / resolutionSecs);
        if (gap > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else if (gap < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        }
        return (int) gap;
    }

    @Override
    public String toString() {
        return "TimeSeriesCounter{" +
            "resolutionSecs=" + resolutionSecs +
            ", epochs=" + Arrays.toString(epochs) +
            ", currentEpochStart=" + currentEpochStart +
            ", currentEpochAdder=" + currentEpochAdder +
            ", currentEpochTimeSeries=" + currentEpochTimeSeries +
            '}';
    }
}
