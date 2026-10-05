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
    private double weightedMultiplicity;
    private double lastArrivalWeight;
    private double logUnseen;
    private double logSurvival;
    private boolean sampled;

    /**
     * @param weight how many arrivals of the query this captured one stands for: the inverse of the
     *               probability it had of being captured
     */
    synchronized void recordArrival(double weight) {
        multiplicity++;
        weightedMultiplicity += weight;
        lastArrivalWeight = weight;
        // each of the arrivals this one stands for had the chance of 1/weight of being captured and missed it
        // with the complement; for a weight of one that is -infinity, the query is certain to have been seen
        logUnseen += weight * Math.log1p(-1.0 / weight);
    }

    /**
     * Estimate of the probability that the query was captured at least once, given how often it arrived.
     * Queries that were never captured are not known to the tracker, so this is what tells how much of the
     * population the known ones stand for: a query whose arrivals were captured at 1% is seen with a
     * chance well below one, and queries like it are under-represented among the known ones.
     * <p>
     * The arrivals that were not captured are not known, so the estimate counts each captured one for as
     * many arrivals as it stands for.
     */
    public synchronized double seenProbability() {
        return -Math.expm1(logUnseen);
    }

    /**
     * How many times the query has been captured, which says nothing about how often it arrived when only a
     * fraction of the searches is captured. Use {@link #weightedMultiplicity()} for that.
     */
    public synchronized long multiplicity() {
        return multiplicity;
    }

    /**
     * Estimate of how many times the query arrived: every captured arrival counts as many as it stands for,
     * so the estimate stays unbiased when the capture rate changes over time.
     */
    public synchronized double weightedMultiplicity() {
        return weightedMultiplicity;
    }

    /**
     * The weight of the latest arrival, which is what the sampler needs to decide about it.
     */
    public synchronized double lastArrivalWeight() {
        return lastArrivalWeight;
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
