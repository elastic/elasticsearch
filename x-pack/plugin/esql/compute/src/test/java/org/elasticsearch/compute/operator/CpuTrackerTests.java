/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.test.FakeCpuClock;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class CpuTrackerTests extends ESTestCase {

    public void testSingleMeasurement() {
        FakeCpuClock clock = new FakeCpuClock();
        CpuTracker tracker = new CpuTracker(clock);
        clock.burn(5);
        tracker.meteredCpu(() -> clock.burn(100));
        clock.burn(7);
        assertEquals(100L, tracker.cpuNanos());
    }

    public void testNestedSameTrackerCountsOnce() {
        FakeCpuClock clock = new FakeCpuClock();
        CpuTracker tracker = new CpuTracker(clock);
        tracker.meteredCpu(() -> {
            clock.burn(10);
            tracker.meteredCpu(() -> clock.burn(20));
            clock.burn(30);
        });
        assertEquals(60L, tracker.cpuNanos());
    }

    public void testForeignTrackerNestedInlinePausesOuter() {
        FakeCpuClock clock = new FakeCpuClock();
        CpuTracker a = new CpuTracker(clock);
        CpuTracker b = new CpuTracker(clock);
        a.meteredCpu(() -> {
            clock.burn(10);
            b.meteredCpu(() -> {
                clock.burn(20);
                a.meteredCpu(() -> clock.burn(40));
            });
            clock.burn(30);
        });
        assertEquals(80L, a.cpuNanos());
        assertEquals(20L, b.cpuNanos());
    }

    public void testCheckpointAddsOnlyOwnOpenMeasurement() {
        FakeCpuClock clock = new FakeCpuClock();
        CpuTracker outer = new CpuTracker(clock);
        CpuTracker inner = new CpuTracker(clock);
        outer.checkpoint();
        outer.meteredCpu(() -> {
            clock.burn(10);
            outer.checkpoint();
            assertEquals(10L, outer.cpuNanos());
            inner.meteredCpu(() -> {
                clock.burn(50);
                outer.checkpoint();
                assertEquals(10L, outer.cpuNanos());
                inner.checkpoint();
                assertEquals(50L, inner.cpuNanos());
                clock.burn(5);
            });
        });
        assertEquals(10L, outer.cpuNanos());
        assertEquals(55L, inner.cpuNanos());
    }

    public void testListenerMeteredOnCompletingThread() throws Exception {
        FakeCpuClock clock = new FakeCpuClock();
        CpuTracker tracker = new CpuTracker(clock);
        ActionListener<String> inner = ActionListener.wrap(r -> clock.burn(70), e -> clock.burn(80));
        ActionListener<String> metered = tracker.meteredCpu(inner);
        Thread t1 = new Thread(() -> metered.onResponse("ok"));
        t1.start();
        t1.join();
        assertEquals(70L, tracker.cpuNanos());
        Thread t2 = new Thread(() -> metered.onFailure(new RuntimeException("boom")));
        t2.start();
        t2.join();
        assertEquals(150L, tracker.cpuNanos());
    }

    public void testThrowingWorkCommitsAndRestores() {
        FakeCpuClock clock = new FakeCpuClock();
        CpuTracker tracker = new CpuTracker(clock);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> tracker.meteredCpu(() -> {
            clock.burn(11);
            throw new RuntimeException("boom");
        }));
        assertEquals("boom", thrown.getMessage());
        assertEquals(11L, tracker.cpuNanos());
        assertFalse(tracker.isMeteringCurrentThread());
    }

    /** A tracker whose clock is unsupported opens nothing, so it neither counts nor pauses an enclosing measurement. */
    public void testUnsupportedClock() {
        FakeCpuClock clock = new FakeCpuClock();
        CpuTracker outer = new CpuTracker(clock);
        CpuTracker unsupported = new CpuTracker(() -> -1L);
        outer.meteredCpu(() -> unsupported.meteredCpu(() -> clock.burn(9)));
        assertEquals(0L, unsupported.cpuNanos());
        assertEquals(9L, outer.cpuNanos());
        assertTrue(unsupported.isMeteringCurrentThread());
    }

    public void testRealClockSmoke() {
        assumeTrue("thread CPU time unsupported", ThreadCpuTimer.currentNanos() >= 0);
        long target = 5_000_000L;
        CpuTracker tracker = new CpuTracker();
        tracker.meteredCpu(() -> {
            long start = ThreadCpuTimer.currentNanos();
            while (ThreadCpuTimer.elapsedNanos(start) < target) {
                Thread.onSpinWait();
            }
        });
        assertThat(tracker.cpuNanos(), greaterThanOrEqualTo(target));
    }
}
