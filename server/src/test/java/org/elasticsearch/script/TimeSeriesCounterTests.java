/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.stream.IntStream;

public class TimeSeriesCounterTests extends ESTestCase {
    protected final int RES = 15;
    protected final long NOW = randomLongBetween(1632935764L, 16329357645L + randomLongBetween(1L, RES * 1_000_000));
    protected final TimeProvider time = new TimeProvider();
    protected final int NUM_ENTRIES = 5;
    protected TimeSeriesCounter ts;
    protected boolean denseSeries;

    @Override
    public void setUp() throws Exception {
        setUpLongAdder();
        denseSeries = randomBoolean();
        super.setUp();
    }

    protected void setUpLongAdder() {
        ts = new TimeSeriesCounter(NUM_ENTRIES, RES, time);
    }

    protected void setUpSubCounter() {
        ts = new TimeSeriesCounter(NUM_ENTRIES, RES, time, RES / 3, 3);
    }

    // The start of the time series
    protected long start() {
        return (NOW / RES) * RES;
    }

    // Generate a sorted random series of events for a given bucket
    protected int bucket(int bucket, int count) {
        List<Long> longs = new ArrayList<>();
        long start = start() + ((long) bucket * RES);
        long end = start + RES - 1;
        for (int i = 0; i < count; i++) {
            longs.add(randomLongBetween(start, end));
        }
        longs.sort(Long::compare);
        time.times.addAll(longs);
        return count;
    }

    protected int[] randomSeries() {
        int[] counts = new int[NUM_ENTRIES];
        counts[0] = bucket(0, randomIntBetween(0, 5));
        for (int i = 1; i < NUM_ENTRIES; i += denseSeries ? 1 : randomIntBetween(1, NUM_ENTRIES)) {
            counts[i] = bucket(i, randomIntBetween(1, 20));
        }
        return counts;
    }

    /**
     * Test that increments in the current bucket count correctly
     */
    public void testCurrentBucket() {
        bucket(0, 2);
        ts.inc();
        ts.inc();
        assertEquals(2, ts.count());
    }

    public void testCurrentBucketSubCounter() {
        setUpSubCounter();
        bucket(0, 2);
        ts.inc();
        ts.inc();
        assertEquals(2, ts.count());
    }

    /**
     * Test that increments that roll over to the next bucket count correctly
     */
    public void testNextBucket() {
        int total = bucket(0, randomIntBetween(1, 20));
        total += bucket(1, randomIntBetween(1, 20));
        incTS(total);
        assertEquals(total, ts.count());
    }

    /**
     * Test that increments that roll over to the next bucket count correctly
     */
    public void testNextBucketSubCounter() {
        setUpSubCounter();
        int total = bucket(0, randomIntBetween(1, 20));
        total += bucket(1, randomIntBetween(1, 20));
        incTS(total);
        assertEquals(total, ts.count());
    }

    /**
     * Test that buckets are skipped
     */
    public void testGapBucket() {
        int total = bucket(0, randomIntBetween(1, 20));
        for (int i = 1; i < NUM_ENTRIES; i += randomIntBetween(1, 3)) {
            total += bucket(i, randomIntBetween(1, 5));
        }
        for (long t : time.times) {
            ts.inc(t);
        }
        assertEquals(total, ts.count());
    }

    /**
     * Test that buckets are skipped
     */
    public void testGapBucketSubCounter() {
        setUpSubCounter();
        int total = bucket(0, randomIntBetween(1, 20));
        for (int i = 1; i < NUM_ENTRIES; i += randomIntBetween(1, 3)) {
            total += bucket(i, randomIntBetween(1, 5));
        }
        for (long t : time.times) {
            ts.inc(t);
        }
        assertEquals(total, ts.count());
    }

    /**
     * Test that a big gap forward in time clears the old history
     */
    public void testHistoryExpired() {
        int[] oldCount = randomSeries();
        incTS(oldCount);
        int nextBucket = randomIntBetween(2 * NUM_ENTRIES - 1, 3 * NUM_ENTRIES);
        int total = bucket(nextBucket, randomIntBetween(0, 20));
        incTS(total);
        assertEquals(total, ts.count());
    }

    /**
     * Test that a big gap forward in time clears the old history
     */
    public void testHistoryExpiredSubCounter() {
        setUpSubCounter();
        int[] oldCount = randomSeries();
        incTS(oldCount);
        int nextBucket = randomIntBetween(2 * NUM_ENTRIES - 1, 3 * NUM_ENTRIES);
        int total = bucket(nextBucket, randomIntBetween(0, 20));
        incTS(total);
        assertEquals(total, ts.count());
    }

    /**
     * Test that epochs roll out of a full history as later epochs are added
     */
    public void testHistoryRollover() {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            // Fill history
            int[] oldCount = randomSeries();
            incTS(oldCount);
            // Add more epochs
            int[] updates = new int[i + 1];
            int total = 0;
            for (int j = 0; j <= i; j++) {
                updates[j] = bucket(NUM_ENTRIES + j, randomIntBetween(1, 20));
                total += updates[j];
            }
            incTS(updates);
            // New epochs should cause old ones to be skipped
            assertEquals(IntStream.of(oldCount).skip(i + 1).sum() + total, ts.count());
        }
    }

    /**
     * Test that epochs roll out of a full history as later epochs are added
     */
    public void testHistoryRolloverSubCounter() {
        setUpSubCounter();
        for (int i = 0; i < NUM_ENTRIES; i++) {
            // Fill history
            int[] oldCount = randomSeries();
            incTS(oldCount);
            // Add more epochs
            int[] updates = new int[i + 1];
            int total = 0;
            for (int j = 0; j <= i; j++) {
                updates[j] = bucket(NUM_ENTRIES + j, randomIntBetween(1, 20));
                total += updates[j];
            }
            incTS(updates);
            // New epochs should cause old ones to be skipped
            assertEquals(IntStream.of(oldCount).skip(i + 1).sum() + total, ts.count());
        }
    }

    /**
     * Test that a gap backwards of more than one epoch resets
     */
    public void testExcessiveNegativeGap() {
        int[] count = randomSeries();
        incTS(count);
        int total = IntStream.of(count).sum();
        assertEquals(total, ts.count());
        time.times.add(ts.getCurrentEpochStart() + randomIntBetween(-4 * RES, -1 * RES - 1));
        ts.inc();
        assertEquals(1, ts.count());
    }

    /**
     * Test that a gap backwards of more than one epoch resets
     */
    public void testExcessiveNegativeGapSubCounter() {
        setUpSubCounter();
        int[] count = randomSeries();
        incTS(count);
        int total = IntStream.of(count).sum();
        assertEquals(total, ts.count());
        time.times.add(ts.getCurrentEpochStart() + randomIntBetween(-4 * RES, -1 * RES - 1));
        ts.inc();
        assertEquals(1, ts.count());
    }

    /**
     * Test that a gap backwards of more at most one epoch resets
     */
    public void testSmallNegativeGap() {
        int[] count = randomSeries();
        incTS(count);
        int total = IntStream.of(count).sum();
        assertEquals(total, ts.count());
        int backwards = randomIntBetween(1, 5);
        for (int i = 0; i < backwards; i ++) {
            time.times.add(ts.getCurrentEpochStart() + randomIntBetween(-1 * RES, 0));
        }
        incTS(backwards);
        assertEquals(total + backwards, ts.count());
    }

    /**
     * Test that a gap backwards of more at most one epoch resets
     */
    public void testSmallNegativeGapSubCounter() {
        setUpSubCounter();
        int[] count = randomSeries();
        incTS(count);
        int total = IntStream.of(count).sum();
        assertEquals(total, ts.count());
        int backwards = randomIntBetween(1, 5);
        for (int i = 0; i < backwards; i ++) {
            time.times.add(ts.getCurrentEpochStart() + randomIntBetween(-1 * RES, 0));
        }
        incTS(backwards);
        assertEquals(total + backwards, ts.count());
    }

    protected void incTS(int[] count) {
        for (int cnt : count) {
            incTS(cnt);
        }
    }

    protected void incTS(int count) {
        for (int i = 0; i < count; i++) {
            ts.inc();
        }
    }

    public static class TimeProvider implements LongSupplier {
        public final List<Long> times = new ArrayList<>();
        public int i = 0;

        @Override
        public long getAsLong() {
            assert times.size() > 0;
            if (i >= times.size()) {
                return times.get(times.size() - 1);
            }
            return times.get(i++);
        }
    }
}
