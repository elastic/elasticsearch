/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/** Bounded aggregate instrumentation for a sequence source; disabled profiling never reads the clock. */
final class SequenceRuntimeProfiler {
    /** Matcher time includes condition time; condition time is a subset, not an additional duration. */
    enum Phase {
        SEARCH_WAIT,
        STAGE_RUN,
        CANDIDATE_BUILD,
        MATCHER,
        CONDITION,
        RESULT_BUILD
    }

    private final boolean enabled;
    private final LongSupplier clock;
    private final long[] nanos = new long[Phase.values().length];
    private final long[] rowsFetched;
    private final long[] candidatesAdmitted;
    private long peakKeys;
    private long peakBytesHeld;

    SequenceRuntimeProfiler(boolean enabled, int stageCount) {
        this(enabled, stageCount, System::nanoTime);
    }

    SequenceRuntimeProfiler(boolean enabled, int stageCount, LongSupplier clock) {
        if (stageCount < 0) {
            throw new IllegalArgumentException("stage count must be nonnegative");
        }
        this.enabled = enabled;
        this.clock = Objects.requireNonNull(clock);
        this.rowsFetched = new long[stageCount];
        this.candidatesAdmitted = new long[stageCount];
    }

    long start() {
        return enabled ? clock.getAsLong() : 0L;
    }

    void finish(Phase phase, long start) {
        Objects.requireNonNull(phase);
        if (enabled == false) {
            return;
        }
        synchronized (this) {
            int index = phase.ordinal();
            nanos[index] = saturatedAdd(nanos[index], Math.max(0L, clock.getAsLong() - start));
        }
    }

    void addRowsFetched(int stage, long count) {
        checkStageAndCount(stage, count);
        if (enabled == false) {
            return;
        }
        synchronized (this) {
            rowsFetched[stage] = saturatedAdd(rowsFetched[stage], count);
        }
    }

    void addCandidatesAdmitted(int stage, long count) {
        checkStageAndCount(stage, count);
        if (enabled == false) {
            return;
        }
        synchronized (this) {
            candidatesAdmitted[stage] = saturatedAdd(candidatesAdmitted[stage], count);
        }
    }

    void observeKeys(long currentKeys) {
        checkNonnegative(currentKeys);
        if (enabled == false) {
            return;
        }
        synchronized (this) {
            peakKeys = Math.max(peakKeys, currentKeys);
        }
    }

    void observeBytesHeld(long currentBytes) {
        checkNonnegative(currentBytes);
        if (enabled == false) {
            return;
        }
        synchronized (this) {
            peakBytesHeld = Math.max(peakBytesHeld, currentBytes);
        }
    }

    SequenceSourceStatus.RuntimeProfile snapshot() {
        if (enabled == false) {
            return null;
        }
        synchronized (this) {
            return new SequenceSourceStatus.RuntimeProfile(
                nanos[Phase.SEARCH_WAIT.ordinal()],
                nanos[Phase.STAGE_RUN.ordinal()],
                nanos[Phase.CANDIDATE_BUILD.ordinal()],
                nanos[Phase.MATCHER.ordinal()],
                nanos[Phase.CONDITION.ordinal()],
                nanos[Phase.RESULT_BUILD.ordinal()],
                asList(rowsFetched),
                asList(candidatesAdmitted),
                peakKeys,
                peakBytesHeld
            );
        }
    }

    private void checkStageAndCount(int stage, long count) {
        Objects.checkIndex(stage, rowsFetched.length);
        checkNonnegative(count);
    }

    private static void checkNonnegative(long value) {
        if (value < 0) {
            throw new IllegalArgumentException("measurement must be nonnegative");
        }
    }

    private static long saturatedAdd(long left, long right) {
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }

    private static List<Long> asList(long[] values) {
        var result = new ArrayList<Long>(values.length);
        for (long value : values) {
            result.add(value);
        }
        return result;
    }
}
