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
 * Limits exact searching to a share of the time live searches took, so ground truth does not compete with them.
 * Credit is capped both ways: a quiet period cannot store up a burst, and an overspend is paid off before more is computed.
 */
public final class CostBudget {

    private final double maxCreditMillis;
    private volatile double ratio;
    private double credit; // guarded by this

    public CostBudget(double ratio, double maxCreditMillis) {
        this.ratio = ratio;
        this.maxCreditMillis = maxCreditMillis;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.SAMPLING_COST_RATIO, value -> this.ratio = value);
    }

    public double ratio() {
        return ratio;
    }

    public synchronized void earn(double liveMillis) {
        credit = Math.min(maxCreditMillis, credit + ratio * liveMillis);
    }

    public synchronized void spend(double millis) {
        credit = Math.max(-maxCreditMillis, credit - millis);
    }

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
