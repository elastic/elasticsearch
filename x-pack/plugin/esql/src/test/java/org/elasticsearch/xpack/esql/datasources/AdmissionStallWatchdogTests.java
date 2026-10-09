/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.apache.logging.log4j.Level;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
                    "*external-source admission stall*bytes{waiters=1*worker-1*"
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
                    "*admission stall*"
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
                    "*admission stall*"
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
                    "*admission stall*"
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
                    "*admission stall*"
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
                        "*admission stall*"
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
                    "*admission stall*"
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
        watchdog.register(rescuingBytesGate(rescueCalls, "used=80/100 owner=lease#1"));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "lease#2:bytes=60");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "first stall",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "first rescue",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*scheduling bug*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(1, rescueCalls.get());
        assertEquals(1, watchdog.rescueCount());

        clock.addAndGet(TimeUnit.SECONDS.toNanos(5));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "quiet suppresses stall warn",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission stall*"
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

        clock.addAndGet(TimeUnit.SECONDS.toNanos(10));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "still in warn quiet",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission stall*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "second rescue 15s after the first, not 30s quiet",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission rescue*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(2, rescueCalls.get());
        assertEquals(2, watchdog.rescueCount());
        watchdog.close();
    }

    public void testRescueDisabledLeavesWedge() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicInteger rescueCalls = new AtomicInteger();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.setRescueEnabled(false);
        watchdog.register(rescuingBytesGate(rescueCalls, "used=80/100 owner=none"));
        watchdog.waitStarted(AdmissionTracker.GATE_BYTES, "queued");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "stall still warns",
                    AdmissionStallWatchdog.class.getCanonicalName(),
                    Level.WARN,
                    "*admission stall*"
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
            public boolean rescueIfStalled() {
                rescued.set(true);
                return true;
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
                    "*admission stall*"
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
                    "*admission stall*"
                )
            );
            watchdog.inspect();
            mockLog.assertAllExpectationsMatched();
        }
        assertEquals(0, watchdog.stats().getFirst().waiters());
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

    private static AdmissionGate rescuingBytesGate(AtomicInteger rescueCalls, String summary) {
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
            public boolean rescueIfStalled() {
                rescueCalls.incrementAndGet();
                return true;
            }
        };
    }
}
