/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongSupplier;

/**
 * Provides a counter with a history of 5m/15m/24h.
 *
 * Callers increment the counter and query the current state of the TimeSeries.
 *
 * {@link TimeSeriesCounter#inc()} increments the counter to indicate an event happens "now", with metadata about "now"
 * from the given {@link TimeSeriesCounter#timeProvider}.
 *
 * {@link TimeSeriesCounter#timeSeries()} provides a snapshot of the counters at the current time, given by
 * {@link TimeSeriesCounter#timeProvider}.  The snapshot includes the number of events in the last five minutes, the last fifteen
 * minutes, the last twenty-four hours and an all-time count.
 *
 * Caveats:
 * * If the timeProvider produces a time stamp value, {@code t[j]} that occurs _before_ an earlier invocation {@code t[j]} (where j is an
 *   invocation that *happens-before*, in the java memory model sense), the time stamp is treated as occurring at the latest time seen,
 *   {@code t[latest]}, EXCEPT if {@code t[latest] - t[j]} is earlier than any time covered by the twenty-four counter.  If that occurs,
 *   time goes backwards more than 24 hours (between 24:00 and 23:45 depending on the circumstance) the history is reset but total is
 *   retained.
 * * All counters have a resolution:
 *   - 5m resolution is 15s
 *   - 15m resolution is 90s
 *   - 24h resolution is 15m
 *   The counter will drop events between one second and the resolution minus one second during a bucket rollover.
 */
public class TimeSeriesCounter {
    public static final int SECOND = 1;
    public static final int MINUTE = 60;
    public static final int HOUR = 60 * MINUTE;
    protected LongAdder adder = new LongAdder();

    protected ReadWriteLock lock = new ReentrantReadWriteLock();

    protected Counter fiveMinutes = new Counter(15 * SECOND, 5 * MINUTE);
    protected Counter fifteenMinutes = new Counter(90 * SECOND, 15 * MINUTE);
    protected Counter twentyFourHours = new Counter(15 * MINUTE, 24 * HOUR);
    protected final LongSupplier timeProvider;

    /**
     * Create a TimeSeriesCounter with the given {@code timeProvider}.
     *
     * The {@code timeProvider} must return positive values, in milliseconds.  In live code, this is expected
     * to be {@link System#currentTimeMillis()} or a proxy such as {@link ThreadPool#absoluteTimeInMillis()}.
     */
    public TimeSeriesCounter(LongSupplier timeProvider) {
        this.timeProvider = timeProvider;
    }

    /**
     * Increment counters at timestamp t, any increment more than 24hours before the current time
     * series resets all historical counters, but the total counter is still increments.
     */
    public void inc() {
        long now = now();
        adder.increment();
        lock.writeLock().lock();
        try {
            // If time has skewed more than a day in the past, reset the histories, but not the adder.
            // Counters clamp all times before the end of the current bucket as happening in the current
            // bucket, so this reset avoids pegging counters to their current buckets for too long.
            if (now < twentyFourHours.earliestTimeInCounter()) {
                fiveMinutes.reset(now);
                fifteenMinutes.reset(now);
                twentyFourHours.reset(now);
            } else {
                fiveMinutes.inc(now);
                fifteenMinutes.inc(now);
                twentyFourHours.inc(now);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the value of the counters for the last 5 minutes, last 15 minutes and the last 24 hours from
     * t.  May include events up to resolution before those durations due to counter granularity.
     */
    public TimeSeries timeSeries() {
        long now = now();
        lock.readLock().lock();
        try {
            return new TimeSeries(fiveMinutes.sum(now), fifteenMinutes.sum(now), twentyFourHours.sum(now), count());
        } finally {
            lock.readLock().unlock();
        }
    }

    // The current time, in seconds, from the timeProvider, which emits milliseconds. Clamped to zero or positive numbers.
    protected long now() {
        long t = timeProvider.getAsLong();
        if (t < 0) {
            t = 0;
        } else {
            t /= 1000;
        }
        return t;
    }

    /**
     * The total number of events for all time covered by the counters.
     */
    public long count() {
        long total = adder.sum();
        return total < 0 ? 0 : total;
    }

    /*
     * Keeps track event counts over a duration.  Events are clamped to buckets, either the current bucket or a future
     * bucket.  A bucket represents all events over a period of resolution number of seconds.
     */
    public static class Counter {

        /*
         * In the following diagrams, we take a duration of 100 and resolution of 20.
         *
         *  |___________________________________________________________|
         * duration = 100
         *
         *  |___________|___________|___________|___________|___________|
         * buckets = 5
         *
         *  |___________|
         * resolution = 20
         *
         * Action: inc(235) - Increment the counter at time 235 seconds.
         *
         * While there is only one `buckets` array, it's useful to view the array as overlapping three
         * epoch (time at bucket[0]), the last epoch, the present epoch and the future epoch.
         *
         *  Past
         *       [_]         [_]         [2]         [3]         [4]
         *  |___________|___________|___________|___________|___________|
         *                         140[e]->    160->       180->       199
         *
         * Present
         *        [0]        [1][b]      [2][g]       [3]         [4]
         *  |___________|_____1_____|___________|___________|___________|
         * 200[a]->    220->       240[d]->    260->       280->       299
         *
         * Future
         *       [0]         [_]         [_]         [_]         [_]
         *  |___________|___________|___________|___________|___________|
         * 300[c]->    320[f]
         *
         * [a] Beginning of the current epoch
         * startOfCurrentEpoch = 200 = (t / duration) * duration = (235 / 100) * 100
         * Start time of bucket zero, this is used to anchor the bucket ring in time.  Without `startOfCurrentEpoch`,
         * it would be impossible to distinguish between two times that are `duration` away from each other.
         * In this example, the first inc used time 235, since startOfCurrentEpoch is rounded down to the nearest
         * duration (100), it is 200.
         *
         * [b] The current bucket
         * curBucket = 1 = (t / resolution) % buckets.length = (235 / 20) % 5
         * The latest active bucket in the bucket ring.  The bucket of a timestamp is determined by the `resolution`.
         * In this case the `resolution` is 20, so each bucket covers 20 seconds, the first covers 200-219, the
         * second covers 220->239, the third 240->259 and so on.  235 is in the second bucket, at index 1.
         *
         * [c] Beginning of the next epoch
         * nextEpoch() = 300 = startOfCurrentEpoch + duration = 200 + 100
         * The first time of the next epoch, this indicates when `startOfCurrentEpoch` should be updated.  When `curBucket`
         * advances to or past zero, `startOfCurrentEpoch` must be updated to `nextEpoch()`
         *
         * [d] Beginning of the next bucket
         * nextBucketStartTime() = 240 = startOfCurrentEpoch + ((curBucket + 1) * resolution) = 200 + ((1 + 1) * 20
         * The first time of the next bucket, when a timestamp is greater than or equal to this time, we must update
         * the `curBucket` and potentially the `startOfCurrentEpoch`.
         *
         * [e] The earliest time to sum
         * earliestTimeInCounter() = 140 = nextBucketStartTime() - duration = 240 - 100
         * `curBucket` covers the latest timestamp seen by the `Counter`.  Since the counter keeps a history, when a
         * caller calls `sum(t)`, the `Counter` must clamp the range to the earliest time covered by its current state.
         * The times proceed backwards for `buckets.length - 1`.
         * **Important** this is likely _before_ the `startOfCurrentEpoch`. `startOfCurrentEpoch` is the timestamp of bucket[0].
         *
         * [f] The counter is no longer valid at this time
         * counterExpired() = 320 = startOfCurrentEpoch + (curBucket * resolution) + duration = 200 + (1 * 20) + 100
         * Where `earliestTimeInCounter()` is looking backwards, to the history covered by the counter, `counterExpired()`
         * looks forward to when current counter has expired.  Since `curBucket` represents the latest time in this counter,
         * `counterExpired()` is `duration` from the start of the time covered from `curBucket`
         *
         * [g] The next bucket in the bucket ring
         * nextBucket(curBucket) = 2 = (i + 1) % buckets.length = (1 + 1) % 5
         * Since `buckets` is a ring, the next bucket may wrap around.
         *
         * ------------------------------------------------------------------------------------------------------------------
         *
         * Action: inc(238) - since this inc is within the current bucket, it is incremented and nothing else changes
         *
         * Present
         *        [0]        [1][b]      [2][g]       [3]         [4]
         *  |___________|_____2_____|___________|___________|___________|
         * 200[a]->    220->       240[d]->    260->       280->       299
         *
         * ------------------------------------------------------------------------------------------------------------------
         *
         * Action: inc(165) - only the current bucket is incremented, so increments from a timestamp in the past are
         *                    clamped to the current bucket.  This makes `inc(long)` dependent on the ordering of timestamps,
         *                    but it avoids revising a history that may have already been exposed via `sum(long)`.
         *
         * Present
         *        [0]        [1][b]      [2][g]       [3]         [4]
         *  |___________|_____3_____|___________|___________|___________|
         * 200[a]->    220->       240[d]->    260->       280->       299
         *
         * ------------------------------------------------------------------------------------------------------------------
         *
         * Action: inc(267) - 267 is in bucket 3, so bucket 2 is zeroed and skipped.  Bucket 2 is zeroed because it may have
         *                    had contents that were relevant for timestamps 140 - 159.
         *
         *                    The `startOfCurrentEpoch`[a], does not change while `curBucket`[b] is now bucket 3.
         *
         *                    `nextEpoch()`[c] does not change as there hasn't been a rollover.
         *
         *                    `nextBucketStartTime()`[d] is now 280, the start time of bucket 4.
         *
         *                    `earliestTimeInCounter()`[e] is now 180, bucket 2 was zeroed, erasing the history from 140-159 and
         *                    bucket 3 was set to 1, now representing 260-279 rather than 160-179.
         *
         *                    `counterExpired()`[f] is now 360.  Bucket 3 in the current epoch represents 260->279, an
         *                    `inc(long)` any of time (260 + `duration`) or beyond would require clearing all `buckets` in the
         *                    `Counter` and any `sum(long)` that starts at 360 or later does not cover the valid time range for
         *                    this state of the counter.
         *
         *                    `nextBucket(curBucket)`[g] is now 4, the bucket after 3.
         *
         *
         *  Past
         *       [_]         [_]         [_]         [_]         [4]
         *  |___________|___________|___________|___________|___________|
         *                                                 180[e]->    199
         *
         * Present
         *        [0]        [1]         [2]          [3][b]     [4][g]
         *  |___________|_____3_____|___________|______1____|___________|
         * 200[a]->    220->       240->       260->       280[d]->    299
         *
         * Future
         *       [0]         [1]         [2]         [_]         [_]
         *  |___________|___________|___________|___________|___________|
         * 300[c]->    320->       340->       360[f]->
         *
         * ------------------------------------------------------------------------------------------------------------------
         *
         * Action: inc(310) - 310 is in bucket 0, so bucket 4 is zeroed and skipped, as it may have had contents
         *                    for timestamps 180-199.
         *
         *                    The `startOfCurrentEpoch`[a], is now 300 as the `Counter` has rolled through bucket 0.
         *
         *                    `curBucket`[b] is now bucket 0.
         *
         *                    `nextEpoch()`[c] is now 400 because `startOfCurrentEpoch`[a] has changed.
         *
         *                    `nextBucketStartTime()`[d] is now 320, the start time of bucket 1 in this new epoch.
         *
         *                    `earliestTimeInCounter()`[e] is now 220, bucket 4 was zeroed, erasing the history from 180-199 and
         *                    bucket 0 was set to 1, now representing 300-319 due to the epoch change, rather than 200-219, so
         *                    220 is the earliest time available in the `Counter`.
         *
         *                    `counterExpired()`[f] is now 400.  Bucket 0 in the current epoch represents 300-319,  an
         *                    `inc(long)` any of time (300 + `duration`) or beyond would require clearing all `buckets` in the
         *                    `Counter` and any `sum(long)` that starts at 400 or later does not cover the valid time range for
         *                    this state of the counter.
         *
         *                    `nextBucket(curBucket)`[g] is now 1, the bucket after 0.
         *
         *
         * Past
         *        [_]        [1]         [2]          [3]        [4]
         *  |___________|_____3_____|___________|______1____|___________|
         *             220[e]->    240->       260->       280->       299
         *
         * Present
         *       [0][b]      [1][g]      [2]         [3]         [4]
         *  |_____1_____|___________|___________|___________|___________|
         * 300[a]->    320[d]->    340->       360->       380->       399
         *
         * Future
         *       [_]         [_]         [_]         [_]         [_]
         *  |___________|___________|___________|___________|___________|
         * 400[c][f]->
         *
         * ------------------------------------------------------------------------------------------------------------------
         *
         * Action: inc(321) - 321 is in bucket 1, so the previous contents of bucket 1 is replaced with the value 1.
         *
         *                    The `startOfCurrentEpoch`[a] remains 300.
         *
         *                    `curBucket`[b] is now bucket 1.
         *
         *                    `nextEpoch()`[c] remains 400.
         *
         *                    `nextBucketStartTime()`[d] is now 340, the start time of bucket 2.
         *
         *                    `earliestTimeInCounter()`[e] is now 240 as bucket 1 now represents 320-339 rather than 220-239.
         *
         *                    `counterExpired()`[f] is now 420.  Bucket 1 in the current epoch represents 320-339,  an
         *                    `inc(long)` any of time (320 + `duration`) or beyond would require clearing all `buckets` in the
         *                    `Counter` and any `sum(long)` that starts at 420 or later does not cover the valid time range for
         *                    this state of the counter.
         *
         *                    `nextBucket(curBucket)`[g] is now 2, the bucket after 1.
         *
         * Past
         *        [_]        [_]         [2]          [3]        [4]
         *  |___________|___________|___________|______1____|___________|
         *                         240[e]->    260->       280->       299
         *
         * Present
         *       [0]         [1][b]      [2][g]      [3]         [4]
         *  |_____1_____|_____1_____|___________|___________|___________|
         * 300[a]->    320->       340[d]->    360->       380->       399
         *
         * Future
         *       [0]         [_]         [_]         [_]         [_]
         *  |_____0_____|___________|___________|___________|___________|
         * 400[c]->    420[f]->
         *
         *
         * ------------------------------------------------------------------------------------------------------------------
         *
         * Action: sum(321) - This is a sum at the exact time of the last update, because of the `earliestTimeInCounter` check,
         *                    it starts at bucket 2, which is after the current bucket index, 1, but the earliest time covered by
         *                    the counter, 240 to 259.
         *   1) calculate start = 321 - duration = 321 - 100 = 221
         *   2) start is before the nextBucketStartTime (340), so sum does not terminate early
         *   3) start is before the earliestTimeInCounter (240) -> start = 240
         *   3) Iterate from bucket(start) = bucket(240) = 2 until curBucket 1, summing the following
         *      bucket 2 = 0, bucket 3 = 1, bucket 4 = 0, bucket 0 = 1 -> 1 + 1 = 2
         *   4) return that with the context of bucket 1 = 1 -> 2 + 1 = 3
         *
         * Action: sum(465) - This sum is so far in the future, it does not overlap with any bucket in range
         *   1) calculate start = 465 - duration = 465 - 100 = 365
         *   2) start is greater than or equal to the nextBucketStartTime (340), so we know the counter has no contexts
         *      -> return 0
         *
         * Action: sum(439) - This sum starts _after_ the last update, which is at 321, but because of the bucket resolution
         *                    sum still catches the value bucket 1, times 320 to 339.
         *   1) calculate start = 439 - duration = 439 - 100 = 339
         *   2) start is before nextBucketStartTime(340), so sum does not terminate early
         *   3) start is after earliestTimeInCounter (240), so it is now updated
         *   4) bucket(start) = 1 which is curBucket, so the for loop falls through
         *   5) return total = 0 + buckets[curBucket] = 0 + 1 = 1
         */
        protected final long resolution;
        protected final long duration;
        protected final long[] buckets;

        // The start time of buckets[0]. bucket(t + (i * duration)) is the same for all i. startOfCurrentEpoch allows the counter
        // to differentiate between those times.
        protected long startOfCurrentEpoch;
        protected int curBucket = 0;

        /**
         * Create a Counter that covers duration seconds at the given resolution.  Duration must be divisible by resolution.
         */
        public Counter(long resolution, long duration) {
            if (resolution <= 0) {
                throw new IllegalArgumentException("resolution [" + resolution + "] must be greater than zero");
            } else if (duration <= 0) {
                throw new IllegalArgumentException("duration [" + duration + "] must be greater than zero");
            } else if (duration % resolution != 0) {
                throw new IllegalArgumentException("duration [" + duration + "] must divisible by resolution [" + resolution + "]");
            }
            this.resolution = resolution;
            this.duration = duration;
            this.buckets = new long[(int) (duration / resolution)];
            this.startOfCurrentEpoch = 0;
            assert buckets.length > 0;
        }

        /**
         * Increment the counter at time {@code now}, expressed in seconds.
         */
        public void inc(long now) {
            if (now < nextBucketStartTime()) {
                buckets[curBucket]++;
            } else if (now >= counterExpired()) {
                reset(now);
            } else {
                int dstBucket = bucket(now);
                for (int i = nextBucket(curBucket); i != dstBucket; i = nextBucket(i)) {
                    buckets[i] = 0;
                }
                curBucket = dstBucket;
                buckets[curBucket] = 1;
                if (now >= nextEpoch()) {
                    startOfCurrentEpoch = epoch(now);
                }
            }
        }

        /**
         * sum for the duration of the counter until {@code now}.
         */
        public long sum(long now) {
            long start = now - duration;
            if (start >= nextBucketStartTime()) {
                return 0;
            }

            if (start < earliestTimeInCounter()) {
                start = earliestTimeInCounter();
            }

            long total = 0;
            for (int i = bucket(start); i != curBucket; i = nextBucket(i)) {
                total += buckets[i];
            }
            return total + buckets[curBucket];
        }

        /**
         * Reset the counter.  Next counter begins at now.
         */
        void reset(long now) {
            Arrays.fill(buckets, 0);
            startOfCurrentEpoch = epoch(now);
            curBucket = bucket(now);
            buckets[curBucket] = 1;
        }

        // The time at bucket[0] for the given timestamp.
        long epoch(long t) {
            return (t / duration) * duration;
        }

        // What is the start time of the next epoch?
        long nextEpoch() {
            return startOfCurrentEpoch + duration;
        }

        // What is the earliest time covered by this counter? Counters do not extend before zero.
        long earliestTimeInCounter() {
            long time = nextBucketStartTime() - duration;
            return time <= 0 ? 0 : time;
        }

        // When does this entire counter expire?
        long counterExpired() {
            return startOfCurrentEpoch + (curBucket * resolution) + duration;
        }

        // bucket for the given time
        int bucket(long t) {
            return (int) (t / resolution) % buckets.length;
        }

        // the next bucket in the circular bucket array
        int nextBucket(int i) {
            return (i + 1) % buckets.length;
        }

        // When does the next bucket start?
        long nextBucketStartTime() {
            return startOfCurrentEpoch + ((curBucket + 1) * resolution);
        }
    }
}
