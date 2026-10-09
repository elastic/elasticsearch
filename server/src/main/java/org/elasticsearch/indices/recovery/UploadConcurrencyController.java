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

/**
 * Decides how many shard snapshot uploads a node runs at once. Uploads are added only while work is queued and the bandwidth limiters are
 * not what holds them back, and an added upload is kept only if it raised throughput. This stops the target from climbing to the ceiling
 * when latency, connections or CPU are the limit rather than the number of uploads. Not thread-safe: called from a single periodic task.
 */
class UploadConcurrencyController {

    /** Process CPU at or above this cuts the target. */
    static final int CPU_LIMIT_PERCENT = 90;
    /** A raise is kept only if throughput grew by at least this factor. */
    static final double MIN_THROUGHPUT_GAIN = 1.05;
    /** Raise only if uploads spent less than this fraction of their time paused in a rate limiter. */
    static final double MAX_LIMITER_WAIT_FRACTION = 0.1;
    /** Intervals to wait after a reverted raise before trying again. */
    static final int COOLDOWN_INTERVALS = 6;

    record Decision(int target, String action, String reason) {}

    private final int floor;
    private final int ceiling;

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

    Decision getLastDecision() {
        return lastDecision;
    }

    /**
     * Decides the target for the next interval from what happened in the last one.
     *
     * @param queued               upload tasks waiting for a slot
     * @param running              upload tasks running
     * @param throughputBytesPerSec upload bytes per second over the interval
     * @param limiterPauseNanos    total time uploads spent paused in rate limiters over the interval
     * @param intervalNanos        length of the interval
     * @param cpuPercent           process CPU percent, negative if unknown
     */
    Decision onInterval(int queued, int running, double throughputBytesPerSec, long limiterPauseNanos, long intervalNanos, int cpuPercent) {
        final boolean coolingDown = cooldownRemaining > 0;
        if (coolingDown) {
            cooldownRemaining--;
        }
        final Decision decision;
        if (cpuPercent >= CPU_LIMIT_PERCENT) {
            probePending = false;
            target = Math.max(floor, target * 3 / 4);
            decision = new Decision(target, "cut", "process cpu " + cpuPercent + "%");
        } else if (probePending) {
            probePending = false;
            if (throughputBytesPerSec >= throughputBeforeProbe * MIN_THROUGHPUT_GAIN) {
                decision = new Decision(target, "keep", throughputChange(throughputBytesPerSec));
            } else {
                target = targetBeforeProbe;
                cooldownRemaining = COOLDOWN_INTERVALS;
                decision = new Decision(target, "revert", throughputChange(throughputBytesPerSec));
            }
        } else {
            final double waitFraction = (double) limiterPauseNanos / ((double) Math.max(intervalNanos, 1L) * Math.max(running, 1));
            if (queued > 0 && waitFraction < MAX_LIMITER_WAIT_FRACTION && coolingDown == false && target < ceiling) {
                probePending = true;
                targetBeforeProbe = target;
                throughputBeforeProbe = throughputBytesPerSec;
                target = Math.min(ceiling, target + Math.max(1, target / 4));
                decision = new Decision(target, "raise", Strings.format("queued %d, limiter wait %.2f", queued, waitFraction));
            } else {
                final String reason;
                if (queued == 0) {
                    reason = "nothing queued";
                } else if (waitFraction >= MAX_LIMITER_WAIT_FRACTION) {
                    reason = Strings.format("limiter wait %.2f", waitFraction);
                } else if (coolingDown) {
                    reason = "cooldown";
                } else {
                    reason = "at ceiling";
                }
                decision = new Decision(target, "hold", reason);
            }
        }
        lastDecision = decision;
        return decision;
    }

    private String throughputChange(double throughputBytesPerSec) {
        return Strings.format("throughput %.0fB/s -> %.0fB/s", throughputBeforeProbe, throughputBytesPerSec);
    }
}
