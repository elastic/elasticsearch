/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceRuntimeProfilerTests extends ESTestCase {
    public void testDisabledNeverReadsClock() {
        var profiler = new SequenceRuntimeProfiler(false, 2, () -> { throw new AssertionError("clock read"); });
        for (var phase : SequenceRuntimeProfiler.Phase.values()) {
            assertEquals(0L, profiler.start());
            profiler.finish(phase, 0);
        }
        profiler.addRowsFetched(0, 3);
        profiler.addCandidatesAdmitted(1, 2);
        profiler.observeKeys(4);
        profiler.observeBytesHeld(5);
        assertNull(profiler.snapshot());
    }

    public void testDisabledHooksDoNotAcquireProfilerLock() throws Exception {
        var profiler = new SequenceRuntimeProfiler(false, 1, () -> { throw new AssertionError("clock read"); });
        var calls = new FutureTask<Void>(() -> {
            profiler.finish(SequenceRuntimeProfiler.Phase.MATCHER, profiler.start());
            profiler.addRowsFetched(0, 1);
            profiler.addCandidatesAdmitted(0, 1);
            profiler.observeKeys(1);
            profiler.observeBytesHeld(1);
            assertNull(profiler.snapshot());
            return null;
        });
        var thread = new Thread(calls);
        try {
            synchronized (profiler) {
                thread.start();
                calls.get(10, TimeUnit.SECONDS);
            }
        } finally {
            thread.join();
        }
    }

    public void testTotalsStagesAndPeakSnapshots() {
        var clock = new AtomicLong();
        var profiler = new SequenceRuntimeProfiler(true, 3, clock::get);
        for (var phase : SequenceRuntimeProfiler.Phase.values()) {
            long start = profiler.start();
            clock.addAndGet(phase.ordinal() + 1);
            profiler.finish(phase, start);
        }
        long start = profiler.start();
        clock.addAndGet(10);
        profiler.finish(SequenceRuntimeProfiler.Phase.SEARCH_WAIT, start);
        profiler.finish(SequenceRuntimeProfiler.Phase.SEARCH_WAIT, clock.get() + 1);
        profiler.addRowsFetched(0, 12);
        profiler.addRowsFetched(0, 3);
        profiler.addRowsFetched(2, 7);
        profiler.addCandidatesAdmitted(1, 4);
        profiler.observeKeys(8);
        profiler.observeKeys(2);
        profiler.observeBytesHeld(100);
        profiler.observeBytesHeld(30);
        var snapshot = profiler.snapshot();
        assertEquals(
            new SequenceSourceStatus.RuntimeProfile(11, 2, 3, 4, 5, 6, List.of(15L, 0L, 7L), List.of(0L, 4L, 0L), 8, 100),
            snapshot
        );
        profiler.addRowsFetched(0, 1);
        profiler.addCandidatesAdmitted(1, 1);
        profiler.observeKeys(9);
        assertEquals(List.of(15L, 0L, 7L), snapshot.rowsFetched());
        assertEquals(List.of(0L, 4L, 0L), snapshot.candidatesAdmitted());
        assertEquals(8, snapshot.peakKeys());
        expectThrows(UnsupportedOperationException.class, () -> snapshot.rowsFetched().add(0L));
    }

    public void testInvalidInputs() {
        expectThrows(IllegalArgumentException.class, () -> new SequenceRuntimeProfiler(true, -1, () -> 0));
        for (boolean enabled : List.of(false, true)) {
            var profiler = new SequenceRuntimeProfiler(enabled, 2, () -> 0);
            expectThrows(NullPointerException.class, () -> profiler.finish(null, 0));
            expectThrows(IndexOutOfBoundsException.class, () -> profiler.addRowsFetched(-1, 1));
            expectThrows(IndexOutOfBoundsException.class, () -> profiler.addCandidatesAdmitted(2, 1));
            expectThrows(IllegalArgumentException.class, () -> profiler.addRowsFetched(0, -1));
            expectThrows(IllegalArgumentException.class, () -> profiler.addCandidatesAdmitted(0, -1));
            expectThrows(IllegalArgumentException.class, () -> profiler.observeKeys(-1));
            expectThrows(IllegalArgumentException.class, () -> profiler.observeBytesHeld(-1));
        }
    }

    public void testCountersSaturate() {
        var profiler = new SequenceRuntimeProfiler(true, 1, () -> 1);
        profiler.addRowsFetched(0, Long.MAX_VALUE);
        profiler.addRowsFetched(0, 1);
        profiler.addCandidatesAdmitted(0, Long.MAX_VALUE);
        profiler.addCandidatesAdmitted(0, 1);
        profiler.finish(SequenceRuntimeProfiler.Phase.MATCHER, Long.MIN_VALUE + 2);
        profiler.finish(SequenceRuntimeProfiler.Phase.MATCHER, 0);
        var snapshot = profiler.snapshot();
        assertEquals(List.of(Long.MAX_VALUE), snapshot.rowsFetched());
        assertEquals(List.of(Long.MAX_VALUE), snapshot.candidatesAdmitted());
        assertEquals(Long.MAX_VALUE, snapshot.matcherNanos());
    }
}
