/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.core.TimeValue;

import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * How long a downsample operation spent in each phase, retries included, as seen by this master. It is kept in memory only, for
 * the log line written when the operation finishes: after a master failover, the phases before the failover are not known.
 */
final class PhaseTimings {

    private final Map<IndexTaskPhase, Long> durations = new EnumMap<>(IndexTaskPhase.class);
    private final boolean seenFromTheStart;
    private IndexTaskPhase phase;
    private long phaseStartedAt;

    /**
     * Starts timing an operation as first seen by this master.
     */
    PhaseTimings(IndexTaskDoc firstSeen, long now) {
        this.phase = firstSeen.phase();
        this.seenFromTheStart = phase == IndexTaskPhase.CREATE_TARGET;
        // the first phase started when the operation was requested
        this.phaseStartedAt = seenFromTheStart ? firstSeen.createdAt() : now;
    }

    synchronized void transition(IndexTaskPhase next, long now) {
        if (next != phase) {
            durations.merge(phase, now - phaseStartedAt, Long::sum);
            phase = next;
            phaseStartedAt = now;
        }
    }

    /**
     * For example {@code CREATE_TARGET [150ms], RUN_SHARDS [11.2s], FINALIZE_SETTINGS [40ms]}.
     */
    synchronized String summary() {
        String phases = durations.entrySet()
            .stream()
            .map(e -> e.getKey() + " [" + TimeValue.timeValueMillis(e.getValue()) + "]")
            .collect(Collectors.joining(", "));
        return seenFromTheStart ? phases : "earlier phases not seen by this master, " + phases;
    }

    // visible for testing
    synchronized Map<IndexTaskPhase, Long> durations() {
        return Map.copyOf(durations);
    }
}
