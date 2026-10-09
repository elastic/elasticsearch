/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.groundtruth;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

/**
 * How much exact searching a node may do, as a share of what its users' searches cost. Every exact search scans the
 * index, so computing ground truth must not compete with the searches it is meant to measure.
 * <p>
 * The budget is a credit in milliseconds of search time. It grows by {@code ratio} times the time live searches took
 * and is spent by the time exact searches take, so the budget follows the traffic: a busy node can afford more, a
 * quiet one very little. The credit is capped, so a quiet period does not store up enough for a burst of exact
 * searches later. An exact search that costs more than was expected leaves a debt, which later earnings pay off
 * before anything else is computed, down to the same cap.
 */
public final class CostBudget {

    private final double maxCreditMillis;
    private volatile double ratio;
    private double credit; // guarded by this

    /**
     * @param ratio           share of the time of live searches that exact searches may take, 0 for no budget
     * @param maxCreditMillis the most credit that is kept, and the most debt that is allowed
     */
    public CostBudget(double ratio, double maxCreditMillis) {
        this.ratio = ratio;
        this.maxCreditMillis = maxCreditMillis;
    }

    /**
     * Follows the setting for the ratio, now and when it changes.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.SAMPLING_COST_RATIO, this::ratio);
    }

    public void ratio(double ratio) {
        this.ratio = ratio;
    }

    public double ratio() {
        return ratio;
    }

    /**
     * Live searches took {@code liveMillis} of search time.
     */
    public synchronized void earn(double liveMillis) {
        credit = Math.min(maxCreditMillis, credit + ratio * liveMillis);
    }

    /**
     * An exact search took {@code millis} of search time.
     */
    public synchronized void spend(double millis) {
        credit = Math.max(-maxCreditMillis, credit - millis);
    }

    /**
     * How many exact searches of about {@code millisEach} fit in the credit, at most {@code max}.
     */
    public synchronized int affordable(double millisEach, int max) {
        if (credit <= 0 || millisEach <= 0) {
            return credit > 0 ? max : 0;
        }
        return (int) Math.min(max, Math.floor(credit / millisEach));
    }

    public synchronized double credit() {
        return credit;
    }
}
