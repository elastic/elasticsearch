/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.core.Nullable;

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
    private Stratum stratum;
    private Hardness hardness;
    private Selectivity selectivity;

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
     * What is known about one arrival that was kept as an event: it counts once, however often the query was searched
     * otherwise, and the chance it had of being kept is the one of being captured and then drawn.
     *
     * @param captureRate the probability the search had of being captured
     * @param sliceRate   the probability a captured search had of being kept as an event
     */
    public static TrackedQuery event(double captureRate, double sliceRate) {
        TrackedQuery event = new TrackedQuery();
        event.recordArrival(1.0 / captureRate);
        event.recordDraw(captureRate * sliceRate);
        return event;
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
        return 0.0 - Math.expm1(logSurvival); // not negated, so that no chance is 0.0 and not -0.0
    }

    /**
     * What the counters said at one moment, read together so that they are consistent with each other.
     *
     * @param multiplicity          captured arrivals
     * @param weightedMultiplicity  estimated arrivals
     * @param inclusionProbability  chance of being in the sample, see {@link #inclusionProbability()}
     * @param seenProbability       chance of having been captured at all, see {@link #seenProbability()}
     * @param captureRate           capture rate of the latest arrival
     */
    public record Weights(
        long multiplicity,
        double weightedMultiplicity,
        double inclusionProbability,
        double seenProbability,
        double captureRate
    ) {}

    public synchronized Weights weights() {
        return new Weights(multiplicity, weightedMultiplicity, inclusionProbability(), seenProbability(), 1.0 / lastArrivalWeight);
    }

    /**
     * The part of the vector space the query is in, null if it has not been put anywhere.
     */
    @Nullable
    public synchronized Stratum stratum() {
        return stratum;
    }

    public synchronized void stratum(@Nullable Stratum stratum) {
        this.stratum = stratum;
    }

    /**
     * How hard the query is for the index to answer, null if that is not known.
     */
    @Nullable
    public synchronized Hardness hardness() {
        return hardness;
    }

    public synchronized void hardness(@Nullable Hardness hardness) {
        this.hardness = hardness;
    }

    /**
     * How much of the vectors the filters of the query leave, null if that is not known, which it is not until it was
     * counted.
     */
    @Nullable
    public synchronized Selectivity selectivity() {
        return selectivity;
    }

    public synchronized void selectivity(@Nullable Selectivity selectivity) {
        this.selectivity = selectivity;
    }

    public synchronized boolean isSampled() {
        return sampled;
    }

    public synchronized void markSampled() {
        sampled = true;
    }
}
