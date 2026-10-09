/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;
import org.elasticsearch.xpack.esql.datasources.spi.NodeByteBudget;
import org.elasticsearch.xpack.esql.datasources.spi.RowGroupIo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class AdmissionStallWatchdogTests extends ESTestCase {

    public void testInjectedStallWarnsWithWaiterGraph() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate("bytes", 0, "used=100/200"));
        AdmissionTracker.Wait wait = watchdog.waitStarted("bytes", "worker-1");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "stall",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*bytes{waiters=1*worker-1*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        wait.granted();
        watchdog.close();
    }

    public void testHealthyMatrixIsSilent() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        for (int i = 0; i < 8; i++) {
            AdmissionTracker.Wait wait = watchdog.waitStarted("permits/s3", "t" + i);
            clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(50));
            wait.granted();
            AdmissionTracker.Wait next = watchdog.waitStarted("permits/s3", "queued-" + i);
            clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
            next.granted();
        }
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no stall",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(0, watchdog.stats().getFirst().waiters());
        watchdog.close();
    }

    public void testWaitersWithNoHoldersWarnEvenAfterRecentGrant() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate("budget/s3", 0, ""));
        watchdog.waitStarted("budget/s3", "stuck");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        AdmissionTracker.Wait recent = watchdog.waitStarted("budget/s3", "moving");
        recent.granted();
        clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "queued with no holders",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        watchdog.close();
    }

    public void testHoldersInUseStaySilent() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate("permits/s3", 2, ""));
        watchdog.waitStarted("permits/s3", "queued");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "holders progressing",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        watchdog.close();
    }

    public void testQuietPeriodSuppressesRepeatWarn() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(30));
        watchdog.waitStarted("segmentators", "pending");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation("first", AdmissionStallWatchdog.class.getCanonicalName(), Level.WARN, "*admission stall*")
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "quiet",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        watchdog.close();
    }

    public void testStatsSnapshot() {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate("permits/s3", 3, ""));
        watchdog.waitStarted("permits/s3", "w1");
        clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(2500));
        List<AdmissionStallWatchdog.GateStats> stats = watchdog.stats();
        assertEquals(1, stats.size());
        assertEquals("permits/s3", stats.get(0).name());
        assertEquals(1, stats.get(0).waiters());
        assertEquals(3, stats.get(0).holders());
        assertEquals(2500L, stats.get(0).oldestWaitMillis());
        watchdog.close();
    }

    public void testScheduledInspectRunsOnGeneric() throws Exception {
        AtomicLong clock = new AtomicLong();
        ThreadPool threadPool = new TestThreadPool(getTestName());
        AdmissionStallWatchdog watchdog = null;
        try {
            watchdog = new AdmissionStallWatchdog(
                threadPool,
                MeterRegistry.NOOP,
                TimeValue.timeValueMillis(10),
                TimeValue.timeValueMillis(1),
                TimeValue.timeValueHours(1),
                clock::get,
                threadPool.generic()
            );
            try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "scheduled stall",
                        AdmissionStallWatchdog.class.getCanonicalName(),
                        Level.WARN,
                        "*possible admission stall*"
                    )
                );
                watchdog.waitStarted("bytes", "parked");
                clock.set(TimeUnit.SECONDS.toNanos(1));
                mockLog.awaitAllExpectationsMatched();
            }
        } finally {
            if (watchdog != null) {
                watchdog.close();
            }
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    /**
     * U5: the byte gate warns on waiters plus holders when no grant has landed for the stall
     * window. Permit saturation stays silent in {@link #testHoldersInUseStaySilent}.
     */
    public void testByteGateWarnsOnGrantAgeEvenWithHolders() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate(AdmissionTracker.GATE_BYTES, 3, "used=80/100 owner=lease#1", AdmissionGate.StallPolicy.GRANT_AGE));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "lease#2:bytes=60");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "byte-gate grant-age stall",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission stall*bytes{waiters=1*used=80/100*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        watchdog.close();
    }

    public void testByteGateRecentGrantStaysSilentWithHolders() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate(AdmissionTracker.GATE_BYTES, 3, "used=80/100 owner=lease#1", AdmissionGate.StallPolicy.GRANT_AGE));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "old");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        AdmissionTracker.Wait recent = watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "moving");
        recent.granted();
        clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "grant-age keys on last grant, not oldest wait",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        watchdog.close();
    }

    public void testRescueClockIgnoresWarnQuiet() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger rescueCalls = new AtomicInteger();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(rescuingBytesGate(rescueCalls, AdmissionGate.RescueResult.OVER_CAP, "used=80/100 owner=lease#1"));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "lease#2:bytes=60");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "first stall",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "first rescue",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*granted FIFO*bytes{waiters=*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(1, rescueCalls.get());
        assertEquals(1, watchdog.rescueCount());

        clock.addAndGet(TimeUnit.SECONDS.toNanos(2));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "quiet suppresses stall warn",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "rescue clock not elapsed",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(1, watchdog.rescueCount());

        clock.addAndGet(TimeUnit.SECONDS.toNanos(3));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "still in warn quiet",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "second rescue 5s after the first, not 30s quiet",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*bytes{waiters=*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(2, rescueCalls.get());
        assertEquals(2, watchdog.rescueCount());
        watchdog.close();
    }

    public void testRescueFiresBeforeStallWarn() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger rescueCalls = new AtomicInteger();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(rescuingBytesGate(rescueCalls, AdmissionGate.RescueResult.OVER_CAP, "used=80/100 owner=none"));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "queued");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(6));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "stall warn still 15s",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "rescue at 5s",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(1, watchdog.rescueCount());
        watchdog.close();
    }

    public void testRescueDisabledLeavesWedge() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger rescueCalls = new AtomicInteger();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.setRescueEnabled(false);
        watchdog.register(rescuingBytesGate(rescueCalls, AdmissionGate.RescueResult.OVER_CAP, "used=80/100 owner=none"));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "queued");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "stall still warns",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no rescue",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(0, rescueCalls.get());
        assertEquals(0, watchdog.rescueCount());
        watchdog.close();
    }

    public void testPermitGateDoesNotRescue() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicBoolean rescued = new AtomicBoolean();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(new AdmissionGate() {
            @Override
            public String name() {
                return AdmissionTracker.permits("s3");
            }

            @Override
            public int holders() {
                return 0;
            }

            @Override
            public RescueResult rescueHead(Executor delivery) {
                rescued.set(true);
                return RescueResult.OVER_CAP;
            }
        });
        watchdog.waitStarted(AdmissionTracker.permits("s3"), "queued");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        watchdog.inspect();
        assertFalse("permit/budget gates keep the holders policy and must not rescue", rescued.get());
        assertEquals(0, watchdog.rescueCount());
        watchdog.close();
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.esql.datasources.AdmissionStallWatchdog:DEBUG",
        reason = "Stage 2 reads the per-tick byte-budget dump instead of waiting for a stall WARN"
    )
    public void testDebugDumpEveryInspectTick() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate(AdmissionTracker.GATE_BYTES, 1, "used=40/100 owner=none", AdmissionGate.StallPolicy.GRANT_AGE));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "debug dump",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.DEBUG,
                    "*byte budget*used=40/100*waiters=[0]*"
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "healthy grant-age is silent at WARN",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        watchdog.close();
    }

    public void testFinishedWaitDoesNotStall() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        AdmissionTracker.Wait wait = watchdog.waitStarted("budget", "timed-out");
        wait.finished();
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "finished wait",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(0, watchdog.stats().getFirst().waiters());
        watchdog.close();
    }

    public void testLostWakeupRegrantIsCountedSeparately() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger rescueCalls = new AtomicInteger();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(rescuingBytesGate(rescueCalls, AdmissionGate.RescueResult.REGRANT, "used=40/100 owner=none"));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "lost-wakeup");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(6));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "regrant warn",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*lost-wakeup*bytes{waiters=*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(1, rescueCalls.get());
        assertEquals(0, watchdog.rescueCount());
        assertEquals(1, watchdog.regrantCount());
        watchdog.close();
    }

    public void testHeldGrantDeliveryDoesNotSpuriousRescue() {
        AtomicLong clock = new AtomicLong();
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        budget.bindTracker(watchdog);
        watchdog.register(bytesGate(budget));

        NodeByteBudget.Hold residual = budget.tryAdmit(80);
        assertNotNull(residual);
        NodeByteBudget.Hold overshoot = occupyOvershoot(budget, 25);
        overshoot.close();
        List<Runnable> held = new ArrayList<>();
        Executor holding = held::add;
        SubscribableListener<NodeByteBudget.Hold> first = budget.admitAsync(50, new RowGroupIo(), () -> false, holding);
        assertFalse(first.isDone());

        clock.set(TimeUnit.SECONDS.toNanos(10));
        residual.close();
        assertEquals("grant decided, delivery sitting on the held executor", 1, held.size());
        assertFalse(first.isDone());
        assertEquals(50, budget.used());

        clock.set(TimeUnit.SECONDS.toNanos(11));
        SubscribableListener<NodeByteBudget.Hold> second = budget.admitAsync(60, new RowGroupIo(), () -> false, holding);
        assertFalse(second.isDone());

        clock.set(TimeUnit.SECONDS.toNanos(14));
        watchdog.inspect();
        assertEquals(0, watchdog.rescueCount());
        assertFalse("next head must wait; the undelivered grant already stamped lastGrant", second.isDone());
        assertEquals(50, budget.used());

        held.forEach(Runnable::run);
        assertTrue(first.isDone());
        budget.clearOwner(overshoot.lease());
        watchdog.close();
    }

    public void testWatchdogRescuesRealByteBudgetHead() throws Exception {
        AtomicLong clock = new AtomicLong();
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        budget.bindTracker(watchdog);
        watchdog.register(bytesGate(budget));

        NodeByteBudget.Hold residual = budget.tryAdmit(80);
        assertNotNull(residual);
        NodeByteBudget.Hold overshoot = occupyOvershoot(budget, 25);
        overshoot.close();
        assertEquals(80, budget.used());

        SubscribableListener<NodeByteBudget.Hold> head = budget.admitAsync(50, new RowGroupIo(), () -> false, Runnable::run);
        assertFalse(head.isDone());
        clock.addAndGet(TimeUnit.SECONDS.toNanos(6));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "rescue before stall warn",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*possible admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "over-cap rescue",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*granted FIFO*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertTrue(head.isDone());
        assertEquals(1, watchdog.rescueCount());
        assertEquals(0, watchdog.regrantCount());
        assertEquals(130, budget.used());
        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
        head.addListener(ActionListener.wrap(hold::set, e -> fail(e.toString())));
        assertFalse(hold.get().isOvershoot());
        hold.get().close();
        residual.close();
        budget.clearOwner(overshoot.lease());
        watchdog.close();
    }

    public void testRescueRedirectsSameWaitersOffInspectThread() {
        AtomicLong clock = new AtomicLong();
        NodeByteBudgetService budget = new NodeByteBudgetService(100);
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        budget.bindTracker(watchdog);
        watchdog.register(bytesGate(budget));
        List<Runnable> held = new ArrayList<>();
        watchdog.setRescueDelivery(held::add);

        NodeByteBudget.Hold residual = budget.tryAdmit(80);
        NodeByteBudget.Hold overshoot = occupyOvershoot(budget, 25);
        overshoot.close();
        SubscribableListener<NodeByteBudget.Hold> head = budget.admitAsync(50, new RowGroupIo(), () -> false, Runnable::run);
        assertFalse(head.isDone());
        clock.addAndGet(TimeUnit.SECONDS.toNanos(6));
        watchdog.inspect();
        assertEquals(1, watchdog.rescueCount());
        assertFalse("inspect must not run SAME waiters", head.isDone());
        assertFalse(held.isEmpty());
        held.forEach(Runnable::run);
        assertTrue(head.isDone());
        residual.close();
        budget.clearOwner(overshoot.lease());
        watchdog.close();
    }

    private static AdmissionStallWatchdog watchdog(AtomicLong clock, TimeValue stall, TimeValue quiet) {
        return new AdmissionStallWatchdog(null, MeterRegistry.NOOP, TimeValue.ZERO, stall, quiet, clock::get, Runnable::run);
    }

    private static AdmissionGate gate(String name, int holders, String summary) {
        return gate(name, holders, summary, AdmissionGate.StallPolicy.HOLDERS);
    }

    private static AdmissionGate gate(String name, int holders, String summary, AdmissionGate.StallPolicy policy) {
        return new AdmissionGate() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public int holders() {
                return holders;
            }

            @Override
            public String holderSummary() {
                return summary;
            }

            @Override
            public StallPolicy stallPolicy() {
                return policy;
            }
        };
    }

    private static AdmissionGate rescuingBytesGate(AtomicInteger rescueCalls, AdmissionGate.RescueResult result, String summary) {
        return new AdmissionGate() {
            @Override
            public String name() {
                return AdmissionTracker.GATE_BYTES;
            }

            @Override
            public int holders() {
                return 2;
            }

            @Override
            public String holderSummary() {
                return summary;
            }

            @Override
            public StallPolicy stallPolicy() {
                return StallPolicy.GRANT_AGE;
            }

            @Override
            public RescueResult rescueHead(Executor delivery) {
                rescueCalls.incrementAndGet();
                return result;
            }
        };
    }

    private static AdmissionGate bytesGate(NodeByteBudgetService budget) {
        return new AdmissionGate() {
            @Override
            public String name() {
                return AdmissionTracker.GATE_BYTES;
            }

            @Override
            public int holders() {
                return budget.used() > 0L ? 1 : 0;
            }

            @Override
            public String holderSummary() {
                return "used=" + budget.used() + "/" + budget.limit();
            }

            @Override
            public StallPolicy stallPolicy() {
                return StallPolicy.GRANT_AGE;
            }

            @Override
            public RescueResult rescueHead(Executor delivery) {
                return budget.rescueHeadOverCap(delivery);
            }
        };
    }

    private static NodeByteBudget.Hold occupyOvershoot(NodeByteBudgetService budget, long bytes) {
        SubscribableListener<NodeByteBudget.Hold> ticket = budget.admitAsync(bytes, new RowGroupIo(), () -> false, Runnable::run);
        assertTrue(ticket.isDone());
        AtomicReference<NodeByteBudget.Hold> hold = new AtomicReference<>();
        ticket.addListener(ActionListener.wrap(hold::set, e -> fail(e.toString())));
        assertTrue(hold.get().isOvershoot());
        return hold.get();
    }
}
