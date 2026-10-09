/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import java.util.concurrent.atomic.LongAdder;

/**
 * Steers the pick rate towards a target by scaling γ, the acceptance scale of the sampler, up and down. How many
 * queries are picked depends on how many different queries are searched, which nobody can know when choosing γ, so
 * this lets γ follow the traffic instead.
 * <p>
 * The adjustment only ever changes the probability of arrivals that come after it. Every arrival records the
 * probability it was drawn with, so the inclusion probabilities, and with them the estimates, stay right however γ
 * moves. A pick budget works for the same reason, but it only cuts the picks off when they are used up, where this
 * keeps the rate even.
 * <p>
 * The pick rate is measured over a window long enough for the target to give about {@value #PICKS_PER_WINDOW} picks,
 * and that is the only time the adjustment moves, so that a handful of picks is not taken for a trend. The step
 * is the square root of how far off the rate is, and at most a factor of two, which settles without overshooting.
 */
final class ScaleRegulator {

    static final double MAX_ADJUSTMENT = 100.0;
    static final double MIN_ADJUSTMENT = 1.0 / MAX_ADJUSTMENT;
    static final double MAX_STEP = 2.0;
    static final double MIN_WINDOW_SECONDS = 30.0;
    static final double MAX_WINDOW_SECONDS = 3600.0;
    static final int PICKS_PER_WINDOW = 10;

    private final LongAdder picks = new LongAdder();
    private volatile double adjustment = 1.0;
    private volatile long targetPerHour;
    private long picksAtLastWindow;
    private double secondsInWindow;

    /**
     * @param perHour the pick rate to aim for, 0 to leave γ as it is set
     */
    void targetPerHour(long perHour) {
        this.targetPerHour = perHour;
    }

    /**
     * What γ is multiplied with.
     */
    double adjustment() {
        return adjustment;
    }

    /**
     * Counts a pick that the sampler had a say in, which is every one apart from those of the head queries.
     */
    void recordPick() {
        picks.increment();
    }

    /**
     * Looks at the picks since the last call, which was {@code seconds} ago, and moves the adjustment if a window
     * has passed.
     */
    synchronized void observe(double seconds) {
        long target = targetPerHour;
        long total = picks.sum();
        if (target <= 0) {
            adjustment = 1.0;
            picksAtLastWindow = total;
            secondsInWindow = 0;
            return;
        }
        secondsInWindow += seconds;
        double window = Math.min(MAX_WINDOW_SECONDS, Math.max(MIN_WINDOW_SECONDS, PICKS_PER_WINDOW * 3600.0 / target));
        if (secondsInWindow < window) {
            return;
        }
        double observedPerHour = (total - picksAtLastWindow) * 3600.0 / secondsInWindow;
        double step = observedPerHour == 0 ? MAX_STEP : Math.sqrt(target / observedPerHour);
        step = Math.min(MAX_STEP, Math.max(1.0 / MAX_STEP, step));
        adjustment = Math.min(MAX_ADJUSTMENT, Math.max(MIN_ADJUSTMENT, adjustment * step));
        picksAtLastWindow = total;
        secondsInWindow = 0;
    }
}
