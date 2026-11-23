/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class AtomicUsageAccumulatorTests extends ESTestCase {

    private static final Instant NOW = Instant.parse("2026-11-23T10:00:00Z");

    private static Map<String, Long> metrics(long planning, long splitDiscovery, long execution, long read) {
        return Map.of(
            QueryMetricsListener.PLANNING_CPU_NANOS,
            planning,
            QueryMetricsListener.SPLIT_DISCOVERY_CPU_NANOS,
            splitDiscovery,
            QueryMetricsListener.CPU_NANOS,
            execution,
            QueryMetricsListener.READ_CPU_NANOS,
            read
        );
    }

    public void testSumsPerOutcomeAndComponent() {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        accumulator.record(Outcome.SUCCESS, metrics(1, 2, 3, 4));
        accumulator.record(Outcome.SUCCESS, metrics(10, 20, 30, 40));
        accumulator.record(Outcome.FAILED, metrics(100, 200, 300, 400));
        accumulator.record(Outcome.CANCELLED, metrics(1000, 2000, 3000, 4000));

        UsageSnapshot snapshot = accumulator.drain(NOW);
        assertThat(snapshot.drainTime(), equalTo(NOW));
        assertThat(
            snapshot.byOutcome(),
            equalTo(
                Map.of(
                    Outcome.SUCCESS,
                    new CpuComponents(11, 22, 33, 44),
                    Outcome.FAILED,
                    new CpuComponents(100, 200, 300, 400),
                    Outcome.CANCELLED,
                    new CpuComponents(1000, 2000, 3000, 4000)
                )
            )
        );
        assertThat(snapshot.isEmpty(), is(false));
    }

    public void testAbsentKeysCountZeroAndOtherKeysAreIgnored() {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        accumulator.record(Outcome.SUCCESS, Map.of(QueryMetricsListener.CPU_NANOS, 7L, "not_a_billed_key", 999L));
        accumulator.record(Outcome.SUCCESS, Map.of());

        assertThat(accumulator.drain(NOW).byOutcome(), equalTo(Map.of(Outcome.SUCCESS, new CpuComponents(0, 0, 7, 0))));
    }

    public void testNegativeValuesCountZero() {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        accumulator.record(Outcome.SUCCESS, metrics(-5, 2, -1, 4));
        accumulator.record(Outcome.SUCCESS, metrics(5, -2, 1, -4));

        assertThat(accumulator.drain(NOW).byOutcome(), equalTo(Map.of(Outcome.SUCCESS, new CpuComponents(5, 2, 1, 4))));
    }

    public void testOnlyNegativeValuesLeaveNothing() {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        accumulator.record(Outcome.SUCCESS, metrics(-5, -2, -1, -4));

        assertThat(accumulator.drain(NOW).isEmpty(), is(true));
    }

    public void testEmptyDrain() {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        UsageSnapshot snapshot = accumulator.drain(NOW);
        assertThat(snapshot.isEmpty(), is(true));
        assertThat(snapshot.byOutcome().isEmpty(), is(true));
        assertThat(snapshot.drainTime(), equalTo(NOW));
    }

    public void testDrainResets() {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        accumulator.record(Outcome.SUCCESS, metrics(1, 2, 3, 4));
        assertThat(accumulator.drain(NOW).isEmpty(), is(false));
        assertThat(accumulator.drain(NOW.plusSeconds(1)).isEmpty(), is(true));

        accumulator.record(Outcome.SUCCESS, metrics(5, 6, 7, 8));
        assertThat(accumulator.drain(NOW.plusSeconds(2)).byOutcome(), equalTo(Map.of(Outcome.SUCCESS, new CpuComponents(5, 6, 7, 8))));
    }

    public void testSnapshotIsNotAffectedByLaterRecords() {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        accumulator.record(Outcome.SUCCESS, metrics(1, 1, 1, 1));
        UsageSnapshot snapshot = accumulator.drain(NOW);
        accumulator.record(Outcome.SUCCESS, metrics(1, 1, 1, 1));

        assertThat(snapshot.byOutcome(), equalTo(Map.of(Outcome.SUCCESS, new CpuComponents(1, 1, 1, 1))));
    }

    /**
     * Writers add known values while a reader drains in a loop; whatever the interleaving, the sum of everything drained, plus a
     * last drain after the writers are done, must equal exactly what was added: nothing lost, nothing returned twice.
     */
    public void testConcurrentRecordAndDrainLoseNothing() throws Exception {
        AtomicUsageAccumulator accumulator = new AtomicUsageAccumulator();
        int writers = between(2, 8);
        int recordsPerWriter = between(2_000, 10_000);
        Outcome[] outcomes = Outcome.values();

        // Each writer adds a distinct, deterministic series, so that a duplicated or lost addend changes the totals.
        Map<Outcome, CpuComponents> expected = new EnumMap<>(Outcome.class);
        for (int w = 0; w < writers; w++) {
            for (int i = 0; i < recordsPerWriter; i++) {
                expected.merge(outcomes[i % outcomes.length], components(w, i), CpuComponents::plus);
            }
        }

        CountDownLatch start = new CountDownLatch(1);
        // Each writer pauses halfway until a drain has returned something: that drain necessarily ran while every writer still had
        // half of its records to add, so the test cannot pass with the whole workload drained only after the writers are done.
        CountDownLatch drainedDuringWrites = new CountDownLatch(1);
        AtomicBoolean writersDone = new AtomicBoolean();
        List<Thread> writerThreads = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            int writer = w;
            Thread thread = new Thread(() -> {
                safeAwait(start);
                for (int i = 0; i < recordsPerWriter; i++) {
                    if (i == recordsPerWriter / 2) {
                        safeAwait(drainedDuringWrites);
                    }
                    CpuComponents c = components(writer, i);
                    accumulator.record(
                        outcomes[i % outcomes.length],
                        metrics(c.planningNanos(), c.splitDiscoveryNanos(), c.executionNanos(), c.readNanos())
                    );
                }
            }, "writer-" + w);
            writerThreads.add(thread);
            thread.start();
        }

        Map<Outcome, CpuComponents> drained = new EnumMap<>(Outcome.class);
        Thread drainer = new Thread(() -> {
            safeAwait(start);
            do {
                UsageSnapshot snapshot = accumulator.drain(NOW);
                accumulate(drained, snapshot);
                if (snapshot.isEmpty() == false) {
                    drainedDuringWrites.countDown();
                }
            } while (writersDone.get() == false);
        }, "drainer");
        drainer.start();

        start.countDown();
        try {
            for (Thread thread : writerThreads) {
                thread.join();
            }
        } finally {
            // Also when the wait is interrupted, so that the drainer never spins on after the test and the writers are not left
            // waiting for a drain that will not come.
            writersDone.set(true);
            drainedDuringWrites.countDown();
        }
        drainer.join();
        accumulate(drained, accumulator.drain(NOW));

        assertThat(drained, equalTo(expected));
        assertThat(accumulator.drain(NOW).isEmpty(), is(true));
    }

    private static CpuComponents components(int writer, int i) {
        return new CpuComponents(writer + 1, i % 7 + 1, (long) i * 3 + 1, i % 11 + 1);
    }

    private static void accumulate(Map<Outcome, CpuComponents> into, UsageSnapshot snapshot) {
        snapshot.byOutcome().forEach((outcome, c) -> into.merge(outcome, c, CpuComponents::plus));
    }
}
