/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.Random;
import java.util.function.Supplier;

/**
 * Keeps a small uniform sample of captured searches as events, so each event weighs the same and the recall users
 * see (where a query counts as often as it is searched) needs no correction beyond the rate. Independent of the
 * query sampler; an arrival can be in both.
 */
public final class EventSlice {

    private final Supplier<Random> random;
    private volatile double rate;

    public EventSlice() {
        this(Randomness::get);
    }

    public EventSlice(Supplier<Random> random) {
        this.random = random;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.EVENT_SLICE_RATE, value -> this.rate = value);
    }

    /**
     * @return the inclusion probability (the weight) if the search was kept, or 0 if not
     */
    public double draw() {
        double rate = this.rate;
        return rate > 0 && random.get().nextDouble() < rate ? rate : 0.0;
    }
}
