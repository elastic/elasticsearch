/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * {@link TimeSeriesCounter} implements an event counter for keeping running stats at fixed intervals, such as 5m/15m/24h.
 * Callers use {@link #inc()} to increment the counter at the current time, as supplied by {@link #timeProvider}.
 * To fetch a count for a given time range, callers use {@link #count(long, long)}, providing an end time and a duration.
 *
 * {@link TimeSeriesCounter} has counts for the given duration at two different resolutions.  From the last updated time,
 * {@link #latestSec}, for {@link #lowSec} ({@code lowSecPerEpoch} in the constructor) previous seconds, the resolution is
 * {@link #highSec} ({@code highSecPerEpoch} in the constructor).
 *
 * Outside of that range, but within {@code totalDuration} seconds, the resolution is {@link #lowSec}.
 *
 * This two ranges balance recent high resolution, historical range and memory usage.
 *
 * This class is implemented with two arrays, {@link #high}, {@link #low} and a {@link LongAdder}.
 *
 * All recent updates hit the LongAdder, {@link #adder} unless an increment causes a roll over to a new in high epoch or both
 * a new high and new low epoch.
 *
 * The two arrays have overlapping time ranges.  {@link #low} delegates its most recent low resolution epoch to {@link #high}
 * and {@link #adder}. Similarly, {@link #high} delegates its most recent high resolution epoch to {@link #adder}.
 *
 * There are some useful time ranges to think about when reading this code:
 * Total authority range - The range of validity of the time series.  Ends within low resolution seconds of the last update and
 *                         starts duration before the last update (plus or minus low resolution seconds.
 * Adder authority range - {@link #adder} must be consulted if the queried time overlaps this range.
 * High authority range - The {@link #high} array must be consulted if the queried time overlaps this range.
 *                        This range may partially overlap the previous {@link #low} epoch because the high array
 *                        does not necessarily reset when rolling over the low epoch.  If we are only a few seconds
 *                        into the new epoch, high keeps higher resolution counts.
 * Low authority range - The {@link #low} array is the authority for counts within this range.
 * High as low delegate - The latest low epoch is represented by a combination of the high array and adder.
 *                        This range occurs immediately after the Low authority range.
 *                        The two ranges together equal the Total authority range.
 *                        Use {@link #sumHighDelegate} to get the correct count out of this range when combining with
 *                        counts from previous low epochs.  As mentioned above, high frequently overlaps with the
 *                        last low epoch and naively counting all of high will lead to double counting.
 */
public class TimeSeriesCounter {
    public static final int SECOND = 1;
    public static final int MINUTE = 60;
    public static final int HOUR = 60 * MINUTE;

    protected final long highSec; // high resolution in seconds
    protected final int[] high;

    protected final long lowSec;  // low resolution in seconds
    protected final int[] low;

    protected final ReadWriteLock lock = new ReentrantReadWriteLock();
    protected final LongSupplier timeProvider;

    protected final LongAdder adder = new LongAdder(); // most recent high epoch
    protected final LongAdder total = new LongAdder(); // the total number of increments
    protected long latestSec; // most recent update time

    /**
     * Create a new time series that covers the given {@code totalDuration} in seconds, with the low resolution epochs covering
     * {@code lowSecPerEpoch} seconds and the high resolution epochs cover {@code highSecPerEpoch}.
     *
     * Because the most recent low resolution epoch is covered completely by the high resolution epochs, {@code lowSecPerEpoch}
     * must be divisible by {@code highSecPerEpoch}.
     */
    public TimeSeriesCounter(long totalDuration, long lowSecPerEpoch, long highSecPerEpoch, LongSupplier timeProvider) {
        if (totalDuration <= 0 || lowSecPerEpoch <= 0 || highSecPerEpoch <= 0) {
            throw new IllegalArgumentException("totalDuration [" + totalDuration + "], lowSecPerEpoch [" + lowSecPerEpoch
                    + "], highSecPerEpoch[" + highSecPerEpoch + "] must be greater than zero");
        } else if (highSecPerEpoch > lowSecPerEpoch) {
            throw new IllegalArgumentException("highSecPerEpoch [" + highSecPerEpoch + "] must be less than lowSecPerEpoch ["
                    + lowSecPerEpoch + "]");
        } else if (totalDuration % lowSecPerEpoch != 0) {
            throw new IllegalArgumentException(
                    "totalDuration [" + totalDuration + "] must be divisible by lowSecPerEpoch [" + lowSecPerEpoch + "]");
        } else if (lowSecPerEpoch % highSecPerEpoch != 0) {
            throw new IllegalArgumentException(
                    "lowSecPerEpoch [" + lowSecPerEpoch + "] must be divisible by highSecPerEpoch [" + highSecPerEpoch + "]");
        }
        this.timeProvider = Objects.requireNonNull(timeProvider);
        this.lowSec = lowSecPerEpoch;
        this.low = new int[(int) (totalDuration / lowSecPerEpoch)];
        this.highSec = highSecPerEpoch;
        this.high = new int[(int) (lowSecPerEpoch / highSecPerEpoch)];
        assert high.length * highSecPerEpoch == lowSecPerEpoch;
    }

    /**
     * Increment the counter at the current time.
     */
    public void inc() {
        inc(now());
    }

    /**
     * Increment the counter at the given time.
     *
     * If {@code nowSec} is less than the last update, it is treated as an increment at the time of the last update
     * unless it's before the begininng of this time series, in which case the time series is reset.
     */
    public void inc(long nowSec) {
        if (nowSec < 0) {
            // The math below relies on now being positive
            return;
        }

        lock.writeLock().lock();
        total.increment();
        try {
            // Handle the busy case quickly
            if (nowSec <= adderAuthorityEnd(latestSec) && nowSec >= adderAuthorityStart(latestSec)) {
                adder.increment();
            } else if (nowSec < latestSec) {
                if (nowSec < totalAuthorityStart(latestSec)) {
                    reset(nowSec);
                } else {
                    adder.increment();
                }
            } else if (nowSec <= highDelegateEnd(latestSec)) {
                rollForwardHigh(nowSec);
                latestSec = nowSec;
                adder.increment();
            } else {
                rollForwardLow(nowSec);
                rollForwardHigh(nowSec);
                latestSec = nowSec;
                adder.increment();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // roll low forward such that t lowAuthorityEnd(t) + 1 = highDelegateStart(t)
    // Assumes t >= latestSec.
    void rollForwardLow(long t) {
        if (totalAuthorityEnd(latestSec) < lowAuthorityStart(t)) {
            // complete rollover
            Arrays.fill(low, 0);
            return;
        }
        int cur = lowIndex(latestSec);
        int dst = lowIndex(t);
        if (cur == dst) {
            // no rollover
            return;
        }

        // grab the high + adder version of the current epoch
        low[cur] = sumHighDelegate(latestSec);
        cur = nextLowIndex(cur);
        while (cur != dst) {
            low[cur] = 0;
            cur = nextLowIndex(cur);
        }
        // low[dst]'s contents is delegated to highDelegate
        low[dst] = 0;
    }

    void rollForwardHigh(long t) {
        if (highDelegateEnd(latestSec) < highAuthorityStart(t)) {
            Arrays.fill(high, 0);
            adder.reset();
            return;
        }
        int cur = highIndex(latestSec);
        int dst = highIndex(t);
        if (cur == dst) {
            // no rollover
            return;
        }

        high[cur] = sumThenResetAdder();
        cur = nextHighIndex(cur);
        while (cur != dst) {
            high[cur] = 0;
            cur = nextHighIndex(cur);
        }
        // high[dst]'s contents is delegated to adder
        high[dst] = 0;
    }

    /**
     * reset the accumulator and all arrays, setting the latestSet to t and incrementing the adder.
     */
    protected void reset(long t) {
        adder.reset();
        Arrays.fill(high, 0);
        Arrays.fill(low, 0);
        adder.increment();
        latestSec = t;
    }

    /**
     * Get the count for the time series ending at {@code end} for {@code duration} seconds beforehand.
     */
    public int count(long end, long duration) {
        if (duration < 0 || end - duration < 0) {
            return 0; // invalid range
        }

        lock.readLock().lock();
        try {
            long start = end - duration;

            if (start >= highAuthorityStart(latestSec)) {
                // entirely within high authority
                return sumHighAuthority(start, end);
            }
            int total = 0;
            if (end >= highDelegateStart(latestSec)) {
                total = sumHighDelegate(end);
                end = lowAuthorityEnd(latestSec);
            }
            return total + sumLow(start, end);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get the total number of increments for all time.
     */
    public long total() {
        long sum = total.sum();
        return sum >= 0 ? sum : 0;
    }

    // sum high range representing the current low resolution epoch.
    int sumHighDelegate(long t) {
        int delegateIndex = highIndex(Math.min(t, latestSec));
        int total = sumAdder();
        for (int i = 0; i < delegateIndex; i++) {
            total += high[i];
        }
        return total;
    }

    // sum within the high range's authority.  Should not be combined with any
    // low epoch counts as the high range's authority may overlap with the
    // previous low epoch.
    int sumHighAuthority(long start, long end) {
        if (start > end) {
            return 0;
        }

        long authorityStart = highAuthorityStart(latestSec);
        long authorityEnd = adderAuthorityEnd(latestSec);

        if (end < authorityStart) {
            return 0;
        } else if (end > authorityEnd) {
            end = authorityEnd;
        }

        if (start > authorityEnd) {
            return 0;
        } else if (start < authorityStart) {
            start = authorityStart;
        }

        int delegateIndex = highIndex(latestSec);
        int cur = highIndex(start);
        int dst = highIndex(end);
        int total = 0;
        while (cur != dst) {
            total += cur == delegateIndex ? sumAdder() : high[cur];
            cur = (cur + 1) % high.length;
        }
        return total + (cur == delegateIndex ? sumAdder() : high[cur]);
    }

    // sum the low epochs represented by the given range
    public int sumLow(long start, long end) {
        if (start > end) {
            return 0;
        }

        long authorityStart = lowAuthorityStart(latestSec);
        long authorityEnd = lowAuthorityEnd(latestSec);

        if (end < authorityStart) {
            return 0;
        } else if (end > authorityEnd) {
            end = authorityEnd;
        }

        if (start > authorityEnd) {
            return 0;
        } else if (start < authorityStart) {
            start = authorityStart;
        }

        int cur = lowIndex(start);
        int dst = lowIndex(end);
        int total = 0;
        while (cur != dst) {
            total += low[cur];
            cur = (cur + 1) % low.length;
        }
        return total + low[cur];
    }

    // get the current time represented by timeProvider
    protected long now() {
        return timeProvider.getAsLong() / 1000;
    }

    // get the current sum from adder, clamped to the range [0, Integer.MAX_VALUE].
    // then reset the adder.  This should only be called when rolling over.
    protected int sumThenResetAdder() {
        long sum = adder.sumThenReset();
        return sum > 0 ? (int) sum : 0;
    }

    // get the current sum from adder, clamped to the range [0, Integer.MAX_VALUE].
    protected int sumAdder() {
        long sum = adder.sum();
        return sum > 0 ? (int) sum : 0;
    }

    // adder is the authority from this time until adderAuthorityEnd.
    // Preceded by high authority range [highAuthorityStart, highAuthorityEnd].
    long adderAuthorityStart(long t) {
        // authority for what _would be_ the latest high epoch
        return (t / highSec) * highSec;
    }

    long adderAuthorityEnd(long t) {
        return adderAuthorityStart(t) + highSec - 1;
    }

    // high is the authority from his time until highAuthorityEnd.
    // This range is proceeded by the adder authority range [adderAuthorityStart, adderAuthorityEnd]
    long highAuthorityStart(long t) {
        return adderAuthorityStart(t) - ((high.length - 1) * highSec);
    }

    long highAuthorityEnd(long t) {
        return adderAuthorityStart(t) - 1;
    }

    // The beginning of the range where high can combine with low to provide accurate counts.
    // This range is preceded by the low authority range [lowAuthorityStart, lowAuthorityEnd]
    long highDelegateStart(long t) {
        return (t / lowSec) * lowSec;
    }

    // The end of the high delegate range [highDelegateStart, highDelegateEnd].  There may
    // not be counts for all parts of this range.  This range only has valid counts until
    // latestSec.
    long highDelegateEnd(long t) {
        return highDelegateStart(t) + lowSec - 1;
    }

    // The beginning of the range where low has counts.
    long lowAuthorityStart(long t) {
        return totalAuthorityStart(t);
    }

    long lowAuthorityEnd(long t) {
        return highDelegateStart(t) - 1;
    }

    // The range of times valid for this TimeSeriesCounter.
    // Equal to [lowAuthorityStart, lowAuthorityEnd] + [highDelegateStart, highDelegateEnd]
    long totalAuthorityStart(long t) {
        return ((t / lowSec) * lowSec) - ((low.length - 1) * lowSec);
    }

    long totalAuthorityEnd(long t) {
        return ((t / lowSec) * lowSec) + (lowSec - 1);
    }

    // the index within the high array of the given time.
    int highIndex(long t) {
        return (int)((t / highSec) % high.length);
    }

    // the index within the low array of the given time.
    int lowIndex(long t) {
        return (int)((t / lowSec) % low.length);
    }

    // the next index within the high array, wrapping as appropriate
    int nextHighIndex(int index) {
        return (index + 1) % high.length;
    }

    // the previous index within the high array, wrapping as appropriate
    int prevHighIndex(int index) {
        return index == 0 ? high.length - 1 : (index - 1) % high.length;
    }

    // the next index within the low array, wrapping as appropriate
    int nextLowIndex(int index) {
        return (index + 1) % low.length;
    }

    // the previous index within the low array, wrapping as appropriate
    int prevLowIndex(int index) {
        return index == 0 ? low.length - 1 : (index - 1) % low.length;
    }

    public Snapshot snapshot(long ... times) {
        return snapshot(now(), times);
    }

    public Snapshot snapshot(Snapshot snapshot) {
        return snapshot(snapshot.now, snapshot.times[0]);
    }

    Snapshot snapshot(long now, long[] times) {
        lock.readLock().lock();
        try {
            Snapshot snapshot = new Snapshot(now, total(), times);
            for (int i = 0; i < snapshot.times.length; i++) {
                snapshot.counts[i] = count(snapshot.now, snapshot.times[i]);
            }
            return snapshot;
        } finally {
            lock.readLock().unlock();
        }
    }

    public static class Snapshot {
        public final long now;
        public final long total;
        public final long[] times;
        public final long[] counts;
        public Snapshot(long now, long total, long ... times) {
            this.now = now;
            this.total = total;
            this.times = new long[times.length];
            this.counts = new long[times.length];
            System.arraycopy(times, 0, this.times, 0, times.length);
        }

        public long getTime(long time) {
            for (int i = 0; i < times.length; i++) {
                if (times[i] == time) {
                    return counts[i];
                }
            }
            return 0;
        }
    }

    // Testing and debugging methods
    int getAdder() {
        return adder.intValue();
    }

    int getLowLength() {
        return low.length;
    }

    int getHighLength() {
        return high.length;
    }

    long getHighSec() {
        return highSec;
    }

    long getLowSec() {
        return lowSec;
    }
}
