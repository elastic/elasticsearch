/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.capture;

/**
 * The capture probability: the configured rate, raised so that low traffic still yields the per-hour floor. The floor
 * rate depends only on earlier arrivals, so the probability recorded with each search stays unbiased as the rate moves.
 */
public final class CaptureRate {

    private static final double SMOOTHING = 0.3;
    private static final double SECONDS_PER_HOUR = 3600;
    private static final double UNKNOWN = -1;

    // everything but the result is guarded by this; the result is read by every search, without the lock
    private double configured;
    private long minPerHour;
    private double smoothedPerSecond = UNKNOWN;
    private volatile double effective;

    public CaptureRate(double configured, long minPerHour) {
        this.configured = configured;
        this.minPerHour = minPerHour;
        this.effective = configured;
    }

    public synchronized void configured(double configured) {
        this.configured = configured;
        recompute();
    }

    public synchronized void minPerHour(long minPerHour) {
        this.minPerHour = minPerHour;
        recompute();
    }

    public synchronized void observe(long arrivals, double seconds) {
        double perSecond = arrivals / seconds;
        smoothedPerSecond = smoothedPerSecond == UNKNOWN ? perSecond : SMOOTHING * perSecond + (1 - SMOOTHING) * smoothedPerSecond;
        recompute();
    }

    public synchronized void forgetTraffic() {
        smoothedPerSecond = UNKNOWN;
        recompute();
    }

    public double effective() {
        return effective;
    }

    // the caller holds the lock
    private void recompute() {
        double floorRate = 0;
        if (minPerHour > 0 && smoothedPerSecond != UNKNOWN) {
            // with no traffic at all, whatever arrives is wanted
            floorRate = smoothedPerSecond <= 0 ? 1.0 : minPerHour / (smoothedPerSecond * SECONDS_PER_HOUR);
        }
        effective = Math.min(1.0, Math.max(configured, floorRate));
    }
}
