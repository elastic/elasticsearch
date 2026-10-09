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
 * Decides how many shard snapshot uploads a node runs at once. Uploads are added one per interval, only while work is queued, the
 * bandwidth limiters are not what holds them back and the node shows no sign of CPU contention, and an added upload is kept only if it
 * raised throughput. The target is cut multiplicatively when uploads fail, or when foreground work is being delayed: the pod waits for
 * CPU (pressure stall information), is throttled by its CPU quota, or its write tasks queue for long. This stops the target from
 * climbing to the ceiling when latency, connections or CPU are the limit rather than the number of uploads. A signal that is not
 * available counts as quiet when deciding to cut, but never as quiet when deciding to raise. Not thread-safe: called from a single
 * periodic task.
 */
class UploadConcurrencyController {

    /** A raise is kept only if throughput grew by at least this factor. */
    static final double MIN_THROUGHPUT_GAIN = 1.05;
    /** Raise only if uploads spent less than this fraction of their time paused in a rate limiter. */
    static final double MAX_LIMITER_WAIT_FRACTION = 0.1;
    /** Fraction of the interval in which runnable tasks waited for CPU, above which foreground work counts as being delayed. */
    static final double CONTENDED_CPU_PRESSURE = 0.05;
    /** Fraction of the interval in which runnable tasks waited for CPU, below which CPU counts as quiet enough to add uploads. */
    static final double QUIET_CPU_PRESSURE = 0.01;
    /** Mean time in milliseconds that write tasks waited in the queue, above which foreground work counts as being delayed. */
    static final double CONTENDED_WRITE_QUEUE_WAIT_MILLIS = 10.0;
    /** Mean time in milliseconds that write tasks waited in the queue, below which writes count as quiet enough to add uploads. */
    static final double QUIET_WRITE_QUEUE_WAIT_MILLIS = 2.0;
    /** Intervals to wait before raising again after upload errors. */
    static final int ERROR_COOLDOWN_INTERVALS = 6;
    /** Intervals to wait before raising again after contention cut the target. */
    static final int CONTENTION_COOLDOWN_INTERVALS = 3;
    /** Intervals to wait before trying again after a raise that did not raise throughput was reverted. */
    static final int REVERT_COOLDOWN_INTERVALS = 6;

    record Decision(int target, String action, String reason) {}

    /**
     * What happened in the last interval.
     *
     * @param queued                 upload tasks waiting for a slot
     * @param running                upload tasks running
     * @param throughputBytesPerSec  upload bytes per second
     * @param limiterPauseNanos      total time uploads spent paused in rate limiters
     * @param intervalNanos          length of the interval
     * @param cpuPressure            fraction of the interval in which runnable tasks of the pod waited for CPU, if known
     * @param throttledMicros        time the pod was throttled by its CPU quota, if known
     * @param writeQueueWaitMillis   mean time write tasks waited in the queue, if known (zero if none started)
     * @param readErrors             uploads that failed reading the source (shared with foreground work)
     * @param uploadErrors           uploads that failed writing to the repository
     */
    record Signals(
        int queued,
        int running,
        double throughputBytesPerSec,
        long limiterPauseNanos,
        long intervalNanos,
        OptionalDouble cpuPressure,
        OptionalLong throttledMicros,
        OptionalDouble writeQueueWaitMillis,
        long readErrors,
        long uploadErrors
    ) {}

    private final int floor;
    private int ceiling;

    private int target;
    private boolean probePending;
    private int targetBeforeProbe;
    private double throughputBeforeProbe;
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
     * Go back to the floor and forget any probe or cooldown.
     */
    void reset() {
        target = floor;
        probePending = false;
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
        if (target > ceiling) {
            probePending = false;
            target = ceiling;
            lastDecision = new Decision(target, "cut", "ceiling lowered to " + ceiling);
            return lastDecision;
        }
        final Decision decision;
        final long errors = signals.readErrors() + signals.uploadErrors();
        final List<String> contention = contention(signals);
        if (errors > 0) {
            probePending = false;
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
            probePending = false;
            target = Math.max(floor, target * 3 / 4);
            cooldownRemaining = Math.max(cooldownRemaining, CONTENTION_COOLDOWN_INTERVALS);
            decision = new Decision(target, "cut", String.join(", ", contention));
        } else if (probePending) {
            probePending = false;
            if (signals.throughputBytesPerSec() >= throughputBeforeProbe * MIN_THROUGHPUT_GAIN) {
                decision = new Decision(target, "keep", throughputChange(signals.throughputBytesPerSec()));
            } else {
                target = targetBeforeProbe;
                cooldownRemaining = Math.max(cooldownRemaining, REVERT_COOLDOWN_INTERVALS);
                decision = new Decision(target, "revert", throughputChange(signals.throughputBytesPerSec()));
            }
        } else {
            decision = raiseOrHold(signals, coolingDown);
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
        if (signals.throttledMicros().isPresent() && signals.throttledMicros().getAsLong() > 0L) {
            reasons.add("cpu throttled " + signals.throttledMicros().getAsLong() + "us");
        }
        if (signals.writeQueueWaitMillis().isPresent()
            && signals.writeQueueWaitMillis().getAsDouble() > CONTENDED_WRITE_QUEUE_WAIT_MILLIS) {
            reasons.add(Strings.format("write queue wait %.1fms", signals.writeQueueWaitMillis().getAsDouble()));
        }
        return reasons;
    }

    private Decision raiseOrHold(Signals signals, boolean coolingDown) {
        final double waitFraction = (double) signals.limiterPauseNanos() / ((double) Math.max(signals.intervalNanos(), 1L) * Math.max(
            signals.running(),
            1
        ));
        final String reasonToHold;
        if (signals.queued() == 0) {
            reasonToHold = "nothing queued";
        } else if (waitFraction >= MAX_LIMITER_WAIT_FRACTION) {
            reasonToHold = Strings.format("limiter wait %.2f", waitFraction);
        } else if (coolingDown) {
            reasonToHold = "cooldown";
        } else if (target >= ceiling) {
            reasonToHold = "at ceiling";
        } else {
            reasonToHold = notQuiet(signals);
        }
        if (reasonToHold != null) {
            return new Decision(target, "hold", reasonToHold);
        }
        probePending = true;
        targetBeforeProbe = target;
        throughputBeforeProbe = signals.throughputBytesPerSec();
        target = Math.min(ceiling, target + 1);
        return new Decision(
            target,
            "raise",
            Strings.format(
                "queued %d, limiter wait %.2f, cpu pressure %.3f, write queue wait %.1fms",
                signals.queued(),
                waitFraction,
                signals.cpuPressure().getAsDouble(),
                signals.writeQueueWaitMillis().getAsDouble()
            )
        );
    }

    /**
     * @return why raising is not safe, or {@code null} if CPU pressure, throttling and write queue wait are all known and quiet
     */
    private static String notQuiet(Signals signals) {
        if (signals.cpuPressure().isEmpty()) {
            return "cpu pressure unavailable";
        } else if (signals.cpuPressure().getAsDouble() >= QUIET_CPU_PRESSURE) {
            return Strings.format("cpu pressure %.3f", signals.cpuPressure().getAsDouble());
        }
        if (signals.throttledMicros().isEmpty()) {
            return "cpu throttling unavailable";
        }
        if (signals.writeQueueWaitMillis().isEmpty()) {
            return "write queue wait unavailable";
        } else if (signals.writeQueueWaitMillis().getAsDouble() >= QUIET_WRITE_QUEUE_WAIT_MILLIS) {
            return Strings.format("write queue wait %.1fms", signals.writeQueueWaitMillis().getAsDouble());
        }
        return null;
    }

    private String throughputChange(double throughputBytesPerSec) {
        return Strings.format("throughput %.0fB/s -> %.0fB/s", throughputBeforeProbe, throughputBytesPerSec);
    }
}
