/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.operator.CpuTracker;
import org.elasticsearch.compute.test.FakeCpuClock;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class PlanningCpuTrackerTests extends ESTestCase {

    public void testFinishSettlesOpenMeasurementAndFreezes() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        long[] finished = new long[1];
        tracker.meteredCpu(() -> {
            clock.burn(10);
            finished[0] = tracker.finish();
            clock.burn(20); // execution continuing on the planning thread
        });
        assertEquals(10L, finished[0]);
        assertEquals(10L, tracker.cpuNanos());
        assertEquals(10L, tracker.finish());
    }

    /**
     * A commit racing with {@code finish()} can still land in the running sum after it returns, so the first
     * {@code finish()} must freeze what every later read reports. A single round hits the race only some of the time,
     * so the test races many fresh trackers.
     */
    public void testFinishFreezesTotalAgainstRacingCommits() {
        FakeCpuClock clock = new FakeCpuClock();
        int committers = between(2, 4);
        for (int round = 0; round < 50; round++) {
            PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
            CountDownLatch committing = new CountDownLatch(committers);
            AtomicBoolean stop = new AtomicBoolean();
            long[] finished = new long[1];
            runInParallel(committers + 1, i -> {
                if (i < committers) {
                    tracker.meteredCpu(() -> {
                        committing.countDown();
                        while (stop.get() == false) {
                            clock.burn(1);
                            tracker.checkpoint();
                        }
                    });
                } else {
                    safeAwait(committing);
                    finished[0] = tracker.finish();
                    stop.set(true);
                }
            });
            assertEquals(finished[0], tracker.finish());
            assertEquals(finished[0], tracker.cpuNanos());
        }
    }

    public void testCheckpointSurvivesFinishOnAnotherThread() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        CountDownLatch checkpointed = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        Thread worker = new Thread(() -> tracker.meteredCpu(() -> {
            clock.burn(50);
            tracker.checkpoint();
            checkpointed.countDown();
            safeAwait(finished);
            clock.burn(5); // after the signal: may be dropped
        }));
        worker.start();
        try {
            safeAwait(checkpointed);
            assertEquals(50L, tracker.finish());
        } finally {
            finished.countDown();
            safeJoin(worker);
        }
        assertEquals(50L, tracker.cpuNanos());
    }

    /** Documents the loss that {@code checkpoint()} exists to bound: a measurement still open when another thread finishes is dropped. */
    public void testMeasurementOpenAtFinishOnAnotherThreadIsDropped() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        CountDownLatch burned = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        Thread worker = new Thread(() -> tracker.meteredCpu(() -> {
            clock.burn(50);
            burned.countDown();
            safeAwait(finished);
        }));
        worker.start();
        try {
            safeAwait(burned);
            assertEquals(0L, tracker.finish());
        } finally {
            finished.countDown();
            safeJoin(worker);
        }
        assertEquals(0L, tracker.cpuNanos());
    }

    /**
     * A stage's handler does its work, then dispatches the next stage with a metered listener. The next stage completes
     * on another thread and finishes planning while the handler's thread is still unwinding. The handler's work must
     * still be counted, because wrapping the listener commits it.
     */
    public void testWrappingListenerCommitsWorkBeforeDispatch() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        long[] total = new long[1];
        CountDownLatch finished = new CountDownLatch(1);
        ActionListener<Void> nextStage = ActionListener.wrap(r -> {
            try {
                clock.burn(10);
                total[0] = tracker.finish();
            } finally {
                finished.countDown();
            }
        }, e -> fail("unexpected failure"));
        Thread handler = new Thread(() -> tracker.meteredCpu(() -> {
            clock.burn(1000);
            ActionListener<Void> dispatched = tracker.meteredCpu(nextStage);
            new Thread(() -> dispatched.onResponse(null)).start();
            safeAwait(finished);
            clock.burn(5); // the unwind after the dispatch is still dropped
        }));
        handler.start();
        safeJoin(handler);
        assertEquals(1010L, total[0]);
        assertEquals(1010L, tracker.cpuNanos());
    }

    public void testInheritMeteredCpu() throws Exception {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        ActionListener<Void> inner = ActionListener.wrap(r -> clock.burn(9), e -> fail("unexpected failure"));
        assertSame(inner, PlanningCpuTracker.inheritMeteredCpu(inner));
        ActionListener<Void> inherited = tracker.meteredCpu(() -> PlanningCpuTracker.inheritMeteredCpu(inner));
        assertNotSame(inner, inherited);
        Thread t = new Thread(() -> inherited.onResponse(null));
        t.start();
        t.join();
        assertEquals(9L, tracker.cpuNanos());
    }

    /**
     * {@link PlanningCpuTracker#UNMETERED} stands in for "no tracker": work run inside it stays counted by an enclosing
     * measurement, as if it were not wrapped at all, listeners are passed through unchanged, and finishing it neither
     * settles the enclosing measurement nor freezes the shared instance.
     */
    public void testUnmeteredIsTransparent() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker outer = new PlanningCpuTracker(clock);
        ActionListener<Void> listener = ActionListener.noop();
        assertSame(listener, PlanningCpuTracker.UNMETERED.meteredCpu(listener));
        outer.meteredCpu(() -> PlanningCpuTracker.UNMETERED.meteredCpu(() -> {
            clock.burn(7);
            PlanningCpuTracker.UNMETERED.checkpoint();
            assertEquals(0L, PlanningCpuTracker.UNMETERED.finish());
        }));
        assertEquals(7L, outer.cpuNanos());
        assertEquals(0L, PlanningCpuTracker.UNMETERED.cpuNanos());
    }

    public void testCheckpointCurrentThread() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker outer = new PlanningCpuTracker(clock);
        PlanningCpuTracker inner = new PlanningCpuTracker(clock);
        PlanningCpuTracker.checkpointCurrentThread();
        outer.meteredCpu(() -> inner.meteredCpu(() -> {
            clock.burn(50);
            PlanningCpuTracker.checkpointCurrentThread();
            assertEquals(50L, inner.cpuNanos());
            assertEquals(0L, outer.cpuNanos());
            clock.burn(5);
        }));
        assertEquals(55L, inner.cpuNanos());
        assertEquals(0L, outer.cpuNanos());
    }

    /**
     * Planning shares the per-thread stack with other kinds of trackers, such as a driver's. Work measured by one of
     * them inside planning is not planning's, and the static helpers do not reach through it to the planning tracker
     * underneath.
     */
    public void testOtherKindOfTrackerInsidePlanning() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker planning = new PlanningCpuTracker(clock);
        CpuTracker other = new CpuTracker(clock) {};
        ActionListener<Void> listener = ActionListener.noop();
        planning.meteredCpu(() -> {
            clock.burn(10);
            other.meteredCpu(() -> {
                clock.burn(20);
                assertSame(listener, PlanningCpuTracker.inheritMeteredCpu(listener));
                PlanningCpuTracker.checkpointCurrentThread();
                assertEquals(10L, planning.cpuNanos());
            });
            clock.burn(30);
        });
        assertEquals(40L, planning.cpuNanos());
        assertEquals(20L, other.cpuNanos());
    }

    public void testUnsupportedClock() {
        PlanningCpuTracker tracker = new PlanningCpuTracker(() -> -1L);
        tracker.meteredCpu(() -> {});
        assertEquals(0L, tracker.cpuNanos());
        assertTrue(tracker.isMeteringCurrentThread());
        assertEquals(0L, tracker.finish());
    }
}
