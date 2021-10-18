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

    public void testRolloverHigh() {
        for (int i = 0; i < ts.getHighLength(); i++) {
            t.add(now + ((long) i * highResSecPerEpoch));
        }
        inc();
        assertEquals(ts.getHighLength(), ts.count(now + lowResSecPerEpoch, lowResSecPerEpoch));
    }

    public void testRolloverHighWithGaps() {
        long gap = 3;
        for (int i = 0; i < ts.getHighLength(); i++) {
            t.add(now + (gap * i * highResSecPerEpoch));
        }
        inc();
        assertEquals(ts.getHighLength(), ts.count(now + (gap * lowResSecPerEpoch), (gap * lowResSecPerEpoch)));
    }

    public void testRolloverLow() {
        for (int i = 0; i < ts.getLowLength(); i++) {
            t.add(now + ((long) i * lowResSecPerEpoch));
        }
        inc();
        assertEquals(ts.getLowLength(), ts.count(now + totalDuration, totalDuration));
    }

    public void testRolloverLowWithGaps() {
        long gap = 3;
        for (int i = 0; i < ts.getLowLength() / 4; i++) {
            t.add(now + (gap * i * lowResSecPerEpoch));
        }
        inc();
        assertEquals(ts.getLowLength() / 4, ts.count(now + totalDuration, totalDuration));
    }

    public void testHighLowOverlap() {
        int highPerLow = ts.getHighLength() / 5;
        int numLow = ts.getLowLength() / 5;
        long latest = 0;
        for (long i = 0; i < numLow; i++) {
            for (long j = 0; j < highPerLow; j++) {
                latest = now + (i * lowResSecPerEpoch) + (j * highResSecPerEpoch);
                t.add(latest);
            }
        }
        inc();
        assertEquals(highPerLow * numLow, ts.count(latest, totalDuration));
    }

    public void testBackwardsInc() {
        t.add(now);
        t.add(now - highResSecPerEpoch);
        t.add(now - lowResSecPerEpoch);
        inc();
        assertEquals(3, ts.count(now + highResSecPerEpoch, totalDuration));
    }

    public void testBackwardsIncReset() {
        long twoDays = now + 2 * totalDuration;
        ts.inc(twoDays);
        assertEquals(1, ts.count(twoDays, totalDuration));
        ts.inc(now);
        assertEquals(0, ts.count(twoDays, totalDuration));
        assertEquals(1, ts.count(now, totalDuration));
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
