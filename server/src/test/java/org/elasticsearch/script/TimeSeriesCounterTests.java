/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import static org.elasticsearch.script.TimeSeriesCounter.Snapshot;
import static org.elasticsearch.script.TimeSeriesCounter.MINUTE;
import static org.elasticsearch.script.TimeSeriesCounter.HOUR;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

public class TimeSeriesCounterTests extends ESTestCase {
    protected static final int totalDuration = 24 * HOUR;
    protected static final int lowResSecPerEpoch = 30 * MINUTE;
    protected static final int highResSecPerEpoch = 15;
    protected static final int FIVE = 5 * MINUTE;
    protected static final int FIFTEEN = 15 * MINUTE;
    protected static final int TWENTY_FOUR = 24 * HOUR;
    protected long now;
    protected TimeSeriesCounter ts;
    protected TimeProvider t;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        now = 16345080831234L;
        t = new TimeProvider();
        ts = new TimeSeriesCounter(totalDuration, lowResSecPerEpoch, highResSecPerEpoch, t);
    }

    public void testIncAdder() {
        long start = ts.adderAuthorityStart(now);
        t.add(now);
        long highSec = ts.getHighSec();
        for (int i = 0; i < highSec; i++) {
            t.add(start + i);
        }
        inc();
        assertEquals(highSec + 1, ts.count(now + highSec - 1, highSec - 1));
        assertEquals(highSec + 1, ts.getAdder());
    }

    public void testIncAdderRollover() {
        long start = ts.adderAuthorityStart(now);
        long highSec = ts.getHighSec();
        t.add(now);
        for (int i = 0; i < 2 * highSec; i++) {
            t.add(start + i);
        }
        inc();
        assertEquals(2 * highSec + 1, ts.count(now + 2 * highSec - 1, 2 * highSec - 1));
        assertEquals(highSec, ts.getAdder());
    }

    public void testIncHighRollover() {
        long start = ts.adderAuthorityStart(now);
        long highSec = ts.getHighSec();
        int highLength = ts.getHighLength();
        int count = 0;
        t.add(now);
        for (int i = 0; i < highLength + 1; i++) {
            t.add(start + (i * highSec));
            if (i == highLength / 2 + 1) {
                count = i + 1;
            }
        }
        inc();
        assertEquals(highLength + 2, ts.count(now + (highSec * highLength), (highSec * highLength)));
        assertEquals(1, ts.getAdder());
        assertEquals(count, ts.count(now + (highSec * (highLength / 2)), highSec * (highLength / 2)));
    }

    public void testSnapshot() {
        t.add(now);
        inc();
        t.add(now + 10);
        Snapshot s = ts.snapshot(FIVE, FIFTEEN, TWENTY_FOUR);
        assertEquals(1, s.getTime(FIVE));
        assertEquals(1, s.getTime(FIFTEEN));
        assertEquals(1, s.getTime(TWENTY_FOUR));
    }

    public void testRolloverCount() {
        t.add(now);
        inc();
        assertEquals(1, ts.count(now + 1, FIVE));
        assertEquals(0, ts.count(now + (2 * FIVE) + highResSecPerEpoch, FIVE));
        assertEquals(1, ts.count(now + 1, FIFTEEN));
        assertEquals(0, ts.count(now + (2 * FIFTEEN) + highResSecPerEpoch, FIFTEEN));
        assertEquals(1, ts.count(now + 1, HOUR));
        assertEquals(0, ts.count(now + (2 * HOUR) + highResSecPerEpoch, HOUR));
    }

    void inc() {
        for (int i = 0; i < t.times.size(); i++) {
            ts.inc();
        }
    }

    public static class TimeProvider implements LongSupplier {
        public final List<Long> times = new ArrayList<>();
        public int i = 0;

        public void add(long time) {
            times.add(time * 1000);
        }

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
