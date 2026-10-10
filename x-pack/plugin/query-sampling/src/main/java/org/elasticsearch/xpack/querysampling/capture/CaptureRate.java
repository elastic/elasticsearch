/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

/**
 * The probability with which a search is captured: the configured capture rate, raised when the traffic is so
 * low that it would capture fewer searches per hour than the floor asks for.
 * <p>
 * The floor is met by looking at how many searches arrived recently, smoothed so that one quiet or busy second does
 * not make the rate jump, and capturing {@code floor / (searches expected in an hour)} of them. That only depends
 * on the searches before the one being decided on, so every search still has the probability that is recorded
 * with it, and the estimates that weight a captured search by that probability stay unbiased while the rate moves.
 */
public final class CaptureRate {

    /**
     * How much of the latest observation goes into the smoothed arrival rate.
     */
    private static final double SMOOTHING = 0.3;
    private static final double SECONDS_PER_HOUR = 3600;
    private static final double UNKNOWN = -1;

    private volatile double configured;
    private volatile long minPerHour;
    private double smoothedPerSecond = UNKNOWN; // guarded by this
    private volatile double effective;

    public CaptureRate(double configured, long minPerHour) {
        this.configured = configured;
        this.minPerHour = minPerHour;
        this.effective = configured;
    }

    public void configured(double configured) {
        this.configured = configured;
        recompute();
    }

    /**
     * @param minPerHour the number of searches per hour that should be captured when the traffic allows it, or
     *                   0 for no floor
     */
    public void minPerHour(long minPerHour) {
        this.minPerHour = minPerHour;
        recompute();
    }

    /**
     * Takes note of how many searches arrived in the last {@code seconds}.
     */
    public synchronized void observe(long arrivals, double seconds) {
        double perSecond = arrivals / seconds;
        smoothedPerSecond = smoothedPerSecond == UNKNOWN ? perSecond : SMOOTHING * perSecond + (1 - SMOOTHING) * smoothedPerSecond;
        recompute();
    }

    /**
     * Forgets what is known about the traffic, for when nothing is counted for a while: what was seen before says
     * nothing about what comes next.
     */
    public synchronized void forgetTraffic() {
        smoothedPerSecond = UNKNOWN;
        recompute();
    }

    /**
     * The probability to capture a search with right now.
     */
    public double effective() {
        return effective;
    }

    private synchronized void recompute() {
        double floorRate = 0;
        if (minPerHour > 0 && smoothedPerSecond != UNKNOWN) {
            // with no traffic at all, whatever arrives is wanted
            floorRate = smoothedPerSecond <= 0 ? 1.0 : minPerHour / (smoothedPerSecond * SECONDS_PER_HOUR);
        }
        effective = Math.min(1.0, Math.max(configured, floorRate));
    }
}
