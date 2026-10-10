/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;

import java.util.Random;
import java.util.function.Supplier;

/**
 * Decides which captured searches are also kept as events: a small uniform sample of the arrivals themselves, in
 * which a query is as likely to be there as often as it was searched, and appears again for each time it is picked.
 * <p>
 * The sampler picks distinct queries, and favours the rare ones, which is what is wanted to know how the search does
 * for the different kinds of queries. For the recall that users get, in which a query counts as often as it is
 * searched, a plain random sample of the searches is the better one: every event weighs the same, so nothing has
 * to be corrected for, but the rate of the capture. The two samples do not depend on each other, and an arrival can
 * be in both.
 */
public final class EventSlice {

    private final Supplier<Random> random;
    private volatile double rate;

    public EventSlice() {
        this(Randomness::get);
    }

    /**
     * @param random the source of the draws, which is looked up for every one
     */
    public EventSlice(Supplier<Random> random) {
        this.random = random;
    }

    /**
     * Follows the rate setting, now and when it changes.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.EVENT_SLICE_RATE, value -> this.rate = value);
    }

    /**
     * Draws for one captured search.
     *
     * @return the probability the search had of being kept if it was, which is what it is weighted with, or 0 if it was not
     */
    public double draw() {
        double rate = this.rate;
        return rate > 0 && random.get().nextDouble() < rate ? rate : 0.0;
    }

    /**
     * A new id for an event. Events of the same query are different documents, so it is unique whatever the source of the draws is.
     */
    public String newId() {
        return UUIDs.randomBase64UUID();
    }
}
