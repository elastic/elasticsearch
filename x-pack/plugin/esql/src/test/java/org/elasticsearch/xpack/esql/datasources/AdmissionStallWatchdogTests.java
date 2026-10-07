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
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionGate;
import org.elasticsearch.xpack.esql.datasources.spi.AdmissionTracker;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;

public class AdmissionStallWatchdogTests extends ESTestCase {

    public void testInjectedStallWarnsWithWaiterGraph() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.register(gate("bytes", 4, "used=100/200"));
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

    public void testWaitersWithRecentGrantStaySilent() throws Exception {
        AtomicLong clock = new AtomicLong();
        AdmissionStallWatchdog watchdog = watchdog(clock, TimeValue.timeValueSeconds(15), TimeValue.timeValueSeconds(30));
        watchdog.waitStarted("budget", "stuck");
        clock.addAndGet(TimeUnit.SECONDS.toNanos(16));
        AdmissionTracker.Wait recent = watchdog.waitStarted("budget", "moving");
        recent.granted();
        clock.addAndGet(TimeUnit.SECONDS.toNanos(1));
        try (MockLog mockLog = MockLog.capture(AdmissionStallWatchdog.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "grant keeps the queue live",
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

    public void testStatsAndCountersSnapshot() {
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
        Counters counters = new Counters();
        DataSourceCounters.populateAdmission(stats, counters);
        assertEquals(1L, counters.get("datasources.admission.waiters.current.permits.s3"));
        assertEquals(3L, counters.get("datasources.admission.holders.current.permits.s3"));
        watchdog.close();
    }

    public void testScheduledInspectRunsOnGeneric() throws Exception {
        AtomicLong clock = new AtomicLong();
        AtomicReference<String> inspectThread = new AtomicReference<>();
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
                command -> threadPool.generic().execute(() -> {
                    inspectThread.set(Thread.currentThread().getName());
                    command.run();
                })
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
            assertThat(inspectThread.get(), containsString("generic"));
        } finally {
            if (watchdog != null) {
                watchdog.close();
            }
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static AdmissionStallWatchdog watchdog(AtomicLong clock, TimeValue stall, TimeValue quiet) {
        return new AdmissionStallWatchdog(null, MeterRegistry.NOOP, TimeValue.ZERO, stall, quiet, clock::get, Runnable::run);
    }

    private static AdmissionGate gate(String name, int holders, String summary) {
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
        };
    }
}
