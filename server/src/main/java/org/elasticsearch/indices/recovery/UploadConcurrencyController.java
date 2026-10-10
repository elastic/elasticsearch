/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.core.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * Decides how many shard snapshot uploads a node runs at once. The aim is a fixed target per node, the ceiling: 10 below 8GiB of node
 * memory and 20 from 8GiB, capped by {@code indices.recovery.upload_concurrency.max}, see
 * {@link org.elasticsearch.threadpool.ThreadPool#getMaxSnapshotUploadThreadPoolSize(int)}. It is 10 below 8GiB because in QA the 4GiB
 * pods were CPU-throttled at 10 uploads, so there is no room for more on a node that small. It is 20 from 8GiB because in QA on GCP
 * large nodes, more than about 20 concurrent uploads crossed the CPU-pressure guard with no throughput gain. The floor is today's
 * concurrency, which is 10 and, on nodes with little heap, less. The current target starts at the floor and climbs to the ceiling, and
 * falls back when the node shows signs of strain:
 * <ul>
 *     <li>Upload errors in the interval halve the target, down to the floor, and are followed by a cooldown of
 *     {@value #ERROR_COOLDOWN_INTERVALS} intervals in which the target does not recover.</li>
 *     <li>Foreground work being delayed cuts the target to three quarters, down to the floor: the pod waits for CPU (pressure stall
 *     information above {@value #CONTENDED_CPU_PRESSURE}), or is throttled by its CPU quota for more than
 *     {@value #CONTENDED_THROTTLED_FRACTION} of the interval.</li>
 *     <li>Otherwise, while CPU is quiet (pressure below {@value #QUIET_CPU_PRESSURE} and no throttling) and no error cooldown runs, the
 *     target recovers by one per interval, never jumping straight back to the ceiling.</li>
 * </ul>
 * A signal that is not available counts as quiet when deciding to cut, but never as quiet when deciding to recover, so a node whose CPU
 * cannot be observed stays at today's concurrency. How long write tasks queue is deliberately not a signal: it is available as the
 * {@code es.thread_pool.write.queue.latency.histogram} metric. Throughput is not a signal either. Not thread-safe: called from a single
 * periodic task.
 */
class UploadConcurrencyController {

    /** Fraction of the interval in which runnable tasks waited for CPU, above which foreground work counts as being delayed. */
    static final double CONTENDED_CPU_PRESSURE = 0.05;
    /** Fraction of the interval in which runnable tasks waited for CPU, below which CPU counts as quiet enough to recover. */
    static final double QUIET_CPU_PRESSURE = 0.01;
    /**
     * Fraction of the interval the pod may be throttled by its CPU quota before foreground work counts as being delayed. Throttling for
     * less than this is common for short bursts and not worth cutting for, but any throttling stops recovery.
     */
    static final double CONTENDED_THROTTLED_FRACTION = 0.01;
    /** Intervals to wait before recovering again after upload errors. */
    static final int ERROR_COOLDOWN_INTERVALS = 6;

    record Decision(int target, String action, String reason) {}

    /**
     * What happened in the last interval.
     *
     * @param intervalNanos          length of the interval
     * @param cpuPressure            fraction of the interval in which runnable tasks of the pod waited for CPU, if known
     * @param throttledMicros        time the pod was throttled by its CPU quota, if known
     * @param readErrors             uploads that failed reading the source (shared with foreground work)
     * @param uploadErrors           uploads that failed writing to the repository
     */
    record Signals(long intervalNanos, OptionalDouble cpuPressure, OptionalLong throttledMicros, long readErrors, long uploadErrors) {}

    private final int floor;
    private int ceiling;

    private int target;
    private int cooldownRemaining;
    private Decision lastDecision;

    UploadConcurrencyController(int floor, int ceiling) {
        if (floor <= 0 || ceiling < floor) {
            throw new IllegalArgumentException("invalid bounds [" + floor + ", " + ceiling + "]");
        }
        this.floor = floor;
        this.ceiling = ceiling;
        reset();
    }

    /**
     * Go back to the floor and forget any cooldown.
     */
    void reset() {
        target = floor;
        cooldownRemaining = 0;
        lastDecision = new Decision(floor, "reset", "start at floor");
    }

    int getTarget() {
        return target;
    }

    int getFloor() {
        return floor;
    }

    int getCeiling() {
        return ceiling;
    }

    /**
     * Changes the most uploads the target may reach, never below the floor. A target above the new ceiling is cut to it by the next
     * {@link #onInterval}.
     */
    void setCeiling(int ceiling) {
        this.ceiling = Math.max(floor, ceiling);
    }

    Decision getLastDecision() {
        return lastDecision;
    }

    /**
     * Decides the target for the next interval from what happened in the last one.
     */
    Decision onInterval(Signals signals) {
        final boolean coolingDown = cooldownRemaining > 0;
        if (coolingDown) {
            cooldownRemaining--;
        }
        final Decision decision;
        final long errors = signals.readErrors() + signals.uploadErrors();
        final List<String> contention = contention(signals);
        if (target > ceiling) {
            target = ceiling;
            decision = new Decision(target, "cut", "ceiling lowered to " + ceiling);
        } else if (errors > 0) {
            target = Math.max(floor, target / 2);
            cooldownRemaining = Math.max(cooldownRemaining, ERROR_COOLDOWN_INTERVALS);
            decision = new Decision(
                target,
                "cut",
                Strings.format(
                    "upload errors: %d reading the source, %d writing to the repository",
                    signals.readErrors(),
                    signals.uploadErrors()
                )
            );
        } else if (contention.isEmpty() == false) {
            target = Math.max(floor, target * 3 / 4);
            decision = new Decision(target, "cut", String.join(", ", contention));
        } else {
            decision = recoverOrHold(signals, coolingDown);
        }
        lastDecision = decision;
        return decision;
    }

    /**
     * The signs in the last interval that foreground work was being delayed. A signal that is not available does not count.
     */
    private static List<String> contention(Signals signals) {
        final List<String> reasons = new ArrayList<>();
        if (signals.cpuPressure().isPresent() && signals.cpuPressure().getAsDouble() > CONTENDED_CPU_PRESSURE) {
            reasons.add(Strings.format("cpu pressure %.3f", signals.cpuPressure().getAsDouble()));
        }
        if (signals.throttledMicros().isPresent()
            && signals.throttledMicros().getAsLong() * 1000.0 > CONTENDED_THROTTLED_FRACTION * signals.intervalNanos()) {
            reasons.add("cpu throttled " + signals.throttledMicros().getAsLong() + "us");
        }
        return reasons;
    }

    private Decision recoverOrHold(Signals signals, boolean coolingDown) {
        final String reasonToHold;
        if (target >= ceiling) {
            reasonToHold = "at ceiling";
        } else if (coolingDown) {
            reasonToHold = "error cooldown";
        } else {
            reasonToHold = notQuiet(signals);
        }
        if (reasonToHold != null) {
            return new Decision(target, "hold", reasonToHold);
        }
        target = Math.min(ceiling, target + 1);
        return new Decision(target, "raise", Strings.format("cpu quiet, pressure %.3f", signals.cpuPressure().getAsDouble()));
    }

    /**
     * @return why recovering is not safe, or {@code null} if CPU pressure and throttling are both known and quiet
     */
    private static String notQuiet(Signals signals) {
        if (signals.cpuPressure().isEmpty()) {
            return "cpu pressure unavailable";
        } else if (signals.cpuPressure().getAsDouble() >= QUIET_CPU_PRESSURE) {
            return Strings.format("cpu pressure %.3f", signals.cpuPressure().getAsDouble());
        }
        if (signals.throttledMicros().isEmpty()) {
            return "cpu throttling unavailable";
        } else if (signals.throttledMicros().getAsLong() > 0L) {
            return "cpu throttled " + signals.throttledMicros().getAsLong() + "us";
        }
        return null;
    }
}
