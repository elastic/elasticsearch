/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ThreadCpuTimer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class PlanningCpuTrackerTests extends ESTestCase {

    /** A per-thread CPU clock the test advances by hand. Each thread has its own counter, like ThreadMXBean. */
    private static final class FakeCpuClock implements LongSupplier {
        private final ThreadLocal<long[]> cpu = ThreadLocal.withInitial(() -> new long[1]);

        void burn(long nanos) {
            cpu.get()[0] += nanos;
        }

        @Override
        public long getAsLong() {
            return cpu.get()[0];
        }
    }

    public void testSingleMeasurement() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        clock.burn(5);
        tracker.meteredCpu(() -> clock.burn(100));
        clock.burn(7);
        assertEquals(100L, tracker.cpuNanos());
    }

    public void testNestedSameTrackerCountsOnce() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        tracker.meteredCpu(() -> {
            clock.burn(10);
            tracker.meteredCpu(() -> clock.burn(20));
            clock.burn(30);
        });
        assertEquals(60L, tracker.cpuNanos());
    }

    public void testForeignTrackerNestedInlinePausesOuter() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker a = new PlanningCpuTracker(clock);
        PlanningCpuTracker b = new PlanningCpuTracker(clock);
        a.meteredCpu(() -> {
            clock.burn(10);
            b.meteredCpu(() -> clock.burn(20));
            clock.burn(30);
        });
        assertEquals(40L, a.cpuNanos());
        assertEquals(20L, b.cpuNanos());
    }

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

    public void testListenerMeteredOnCompletingThread() throws Exception {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
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

    public void testThrowingWorkCommitsAndRestores() {
        FakeCpuClock clock = new FakeCpuClock();
        PlanningCpuTracker tracker = new PlanningCpuTracker(clock);
        RuntimeException thrown = expectThrows(RuntimeException.class, () -> tracker.meteredCpu(() -> {
            clock.burn(11);
            throw new RuntimeException("boom");
        }));
        assertEquals("boom", thrown.getMessage());
        assertEquals(11L, tracker.cpuNanos());
        assertFalse(tracker.isMeteringCurrentThread());
    }

    public void testUnsupportedClock() {
        PlanningCpuTracker tracker = new PlanningCpuTracker(() -> -1L);
        tracker.meteredCpu(() -> {});
        assertEquals(0L, tracker.cpuNanos());
        assertTrue(tracker.isMeteringCurrentThread());
        assertEquals(0L, tracker.finish());
    }

    public void testRealClockSmoke() {
        assumeTrue("thread CPU time unsupported", ThreadCpuTimer.currentNanos() >= 0);
        long target = 5_000_000L;
        PlanningCpuTracker tracker = new PlanningCpuTracker();
        tracker.meteredCpu(() -> {
            long start = ThreadCpuTimer.currentNanos();
            while (ThreadCpuTimer.elapsedNanos(start) < target) {
                Thread.onSpinWait();
            }
        });
        assertThat(tracker.cpuNanos(), greaterThanOrEqualTo(target));
    }
}
