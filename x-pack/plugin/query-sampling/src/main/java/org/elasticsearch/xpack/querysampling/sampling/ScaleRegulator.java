/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

/**
 * Steers the pick rate towards a target by scaling γ. It only affects later arrivals, each of which records the
 * probability it was drawn with, so estimates stay unbiased. The rate is measured over a window long enough for about
 * {@value #PICKS_PER_WINDOW} picks, and each step is the square root of the error, capped at a factor of two, to
 * settle without overshooting. Called by the pipeline thread only; the adjustment and the target are volatile as other
 * threads read the first and set the second.
 */
final class ScaleRegulator {

    static final double MAX_ADJUSTMENT = 100.0;
    static final double MIN_ADJUSTMENT = 1.0 / MAX_ADJUSTMENT;
    static final double MAX_STEP = 2.0;
    static final double MIN_WINDOW_SECONDS = 30.0;
    static final double MAX_WINDOW_SECONDS = 3600.0;
    static final int PICKS_PER_WINDOW = 10;

    private long picks;
    private volatile double adjustment = 1.0;
    private volatile long targetPerHour;
    private long picksAtLastWindow;
    private double secondsInWindow;

    /** 0 leaves γ as it is set. */
    void targetPerHour(long perHour) {
        this.targetPerHour = perHour;
    }

    double adjustment() {
        return adjustment;
    }

    /** Head-query picks are not recorded, as the sampler has no say in them. */
    void recordPick() {
        picks++;
    }

    void observe(double seconds) {
        long target = targetPerHour;
        long total = picks;
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
