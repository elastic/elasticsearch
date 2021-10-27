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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Provides a counter with a history of 5m/15m/24h.
 *
 * Callers increment the counter and query
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

    /**
     * Increment counters at timestamp t, any increment more than 24hours before the current time
     * series resets all historical counters, but the total counter is still increments.
     */
    public void inc(long t) {
        adder.increment();
        lock.writeLock().lock();
        try {
            if (t < twentyFourHours.earliestTimeInCounter()) {
                fiveMinutes.reset(t);
                fifteenMinutes.reset(t);
                twentyFourHours.reset(t);
            } else {
                fiveMinutes.inc(t);
                fifteenMinutes.inc(t);
                twentyFourHours.inc(t);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Get the value of the counters for the last 5 minutes, last 15 minutes and the last 24 hours from
     * t.  May include events up to resolution before those durations due to counter granularity.
     */
    public TimeSeries timeSeries(long t) {
        lock.readLock().lock();
        try {
            return new TimeSeries(fiveMinutes.sum(t), fifteenMinutes.sum(t), twentyFourHours.sum(t), count());
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * The total number of events for all time covered gby the counters.
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
         *  |_______________________________________|
         * duration = 100
         *  |_______|_______|_______|_______|_______|
         * buckets = 5
         *  |_______|
         * resolution = 20
         *
         * Action: inc(235)
         *
         *  Past
         *       [_]         [_]         [_]         [3]         [4]
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
         * startOfEpoch = 200            = (t / duration) * duration                          = (235 / 100) * 100
         * Start time of bucket zero, this is used to anchor the bucket ring in time.  Without `startOfEpoch`,
         * it would be impossible to distinguish between two times that are `duration` away from each other.
         * In this example, the first inc used time 235, since startOfEpoch is rounded down to the nearest
         * duration (100), it is 200.
         *
         * [b] The current bucket
         * curBucket = 1                 = (t / resolution) % buckets.length                  = (235 / 20) % 5
         * The latest active bucket in the bucket ring.  The bucket of a timestamp is determined by the `resolution`.
         * In this case the `resolution` is 20, so each bucket covers 20 seconds, the first covers 200-219, the
         * second covers 220->239, the third 240->259 and so on.  235 is in the second bucket, at index 1.
         *
         * [c] Beginning of the next epoch
         * nextEpoch() = 300             = startOfEpoch + duration                            = 200 + 100
         * The first time of the next epoch, this indicates when `startOfEpoch` should be updated.  When `curBucket`
         * advances to or past zero, `startOfEpoch` must be updated to `nextEpoch()`
         *
         * [d] Beginning of the next bucket
         * nextBucketStartTime() = 240   = startOfEpoch + ((curBucket + 1) * resolution)      = 200 + ((1 + 1) * 20
         * The first time of the next bucket, when a timestamp is greater than or equal to this time, we must update
         * the `curBucket` and potentially the `startOfEpoch`.
         *
         * [e] The earliest time to sum
         * earliestTimeInCounter() = 140 = nextBucketStartTime() - duration                   = 240 - 100
         * `curBucket` covers the latest timestamp seen by the `Counter`.  Since the counter keeps a history, when a
         * caller calls `sum(t)`, the `Counter` must clamp the range to the earliest time covered by its current state.
         * The times proceed backwards for `buckets.length - 1`.
         * **Important** this is likely _before_ the `startOfEpoch`. `startOfEpoch` is the timestamp of bucket[0].
         *
         * [f] The counter is no longer valid at this time
         * counterExpired() = 320        = startOfEpoch + (curBucket * resolution) + duration = 200 + (1 * 20) + 100
         * Where `earliestTimeInCounter()` is looking backwards, to the history covered by the counter, `counterExpired()`
         * looks forward to when current counter has expired.  Since `curBucket` represents the latest time in this counter,
         * `counterExpired()` is `duration` from the start of the time covered from `curBucket`
         *
         * [g] The next bucket in the bucket ring
         * nextBucket(curBucket) = 2       = (i + 1) % buckets.length                           = (1 + 1) % 5
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
         *                    The `startOfEpoch`[a], does not change while `curBucket`[b] is now bucket 3.
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
         *                    The `startOfEpoch`[a], is now 300 as the `Counter` has rolled through bucket 0.
         *
         *                    `curBucket`[b] is now bucket 0.
         *
         *                    `nextEpoch()`[c] is now 400 because `startOfEpoch`[a] has changed.
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
         *  |_____0_____|___________|___________|___________|___________|
         * 400[c][f]->
         *
         * ------------------------------------------------------------------------------------------------------------------
         *
         * Action: inc(321) - 321 is in bucket 1, so the previous contents of bucket 1 is replaced with the value 1.
         *
         *                    The `startOfEpoch`[a] remains 300.
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
         * 400[c]->   420[f]->
         *
         */
        protected final long resolution;
        protected final long duration;
        protected final long[] buckets;

        // The start time of buckets[0]. bucket(t + (i * duration)) is the same for all i. startOfEpoch
        protected long startOfEpoch;
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
            assert buckets.length > 0;
        }

        /**
         * Increment the counter at time {@code t}, expressed in seconds.
         */
        public void inc(long t) {
            if (t < nextBucketStartTime()) {
                buckets[curBucket]++;
            } else if (t >= counterExpired()) {
                reset(t);
            } else {
                int dstBucket = bucket(t);
                for (int i = nextBucket(curBucket); i != dstBucket; i = nextBucket(i)) {
                    buckets[i] = 0;
                }
                curBucket = dstBucket;
                buckets[curBucket] = 1;
                if (t >= nextEpoch()) {
                    startOfEpoch = epoch(t);
                }
            }
        }

        /**
         * sum for the duration of the counter until {@code end}.
         */
        public long sum(long end) {
            long start = end - duration;
            if (start >= nextBucketStartTime() || start < 0) {
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
         * Reset the counter.  Next counter begins at t.
         */
        void reset(long t) {
            Arrays.fill(buckets, 0);
            startOfEpoch = epoch(t);
            curBucket = bucket(t);
            buckets[curBucket] = 1;
        }

        // The time at bucket[0] for the given timestamp.
        long epoch(long t) {
            return (t / duration) * duration;
        }

        // What is the start time of the next epoch?
        long nextEpoch() {
            return startOfEpoch + duration;
        }

        // What is the earliest time covered by this counter?
        long earliestTimeInCounter() {
            return nextBucketStartTime() - duration;
        }

        // When does this entire counter expire?
        long counterExpired() {
            return startOfEpoch + (curBucket * resolution) + duration;
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
            return startOfEpoch + ((curBucket + 1) * resolution);
        }
    }
}
