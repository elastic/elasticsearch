/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

/**
 * What is known about one distinct query: how often it has been captured, whether it has been picked for
 * the sample and how likely that was.
 * <p>
 * The pipeline updates it from a single thread, but it is read from other threads (stats, estimates), hence
 * the synchronization.
 */
public final class TrackedQuery {

    private long multiplicity;
    private double logSurvival;
    private boolean sampled;

    /**
     * @return how many times the query has been captured including this arrival
     */
    synchronized long recordArrival() {
        return ++multiplicity;
    }

    public synchronized long multiplicity() {
        return multiplicity;
    }

    /**
     * Accounts for one draw that picked the query with the given probability. It is called for every
     * arrival, also after the query was picked, so the inclusion probability reflects all of them.
     */
    public synchronized void recordDraw(double acceptProbability) {
        logSurvival += Math.log1p(-acceptProbability);
    }

    /**
     * The probability that the query is in the sample after all its arrivals so far, which is what is
     * needed to weight it correctly when estimating over the whole population.
     */
    public synchronized double inclusionProbability() {
        return -Math.expm1(logSurvival);
    }

    public synchronized boolean isSampled() {
        return sampled;
    }

    public synchronized void markSampled() {
        sampled = true;
    }
}
