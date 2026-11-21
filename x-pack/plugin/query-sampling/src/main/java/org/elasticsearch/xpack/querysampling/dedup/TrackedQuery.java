/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.dedup;

import org.elasticsearch.core.Nullable;

/**
 * Counters of one distinct query: captures, whether it was picked and how likely that was. Updated from the
 * pipeline thread but read from others, hence the synchronization.
 */
public final class TrackedQuery {

    // the window of the last arrival, for the tracker that records from one thread only
    long lastWindow;

    private long multiplicity;
    private double weightedMultiplicity;
    private double lastArrivalWeight;
    private double logSurvival;
    private boolean sampled;
    private Stratum stratum;
    private Hardness hardness;
    private boolean placed;

    // weight is the inverse of the capture probability: how many arrivals this captured one stands for
    synchronized void recordArrival(double weight) {
        multiplicity++;
        weightedMultiplicity += weight;
        lastArrivalWeight = weight;
    }

    // called for every arrival, also after the query was picked, so the inclusion probability reflects all draws
    public synchronized void recordDraw(double acceptProbability) {
        logSurvival += Math.log1p(-acceptProbability);
    }

    private double inclusionProbability() {
        return 0.0 - Math.expm1(logSurvival); // not negated, so that no chance is 0.0 and not -0.0
    }

    /**
     * The counters read together so that they are consistent; the only way to read them, as they are updated by the
     * pipeline thread and read by others. {@code multiplicity} counts captures, {@code weightedMultiplicity} estimates
     * arrivals and is unbiased when the capture rate changes over time.
     */
    public record Weights(long multiplicity, double weightedMultiplicity, double inclusionProbability, double captureRate) {}

    public synchronized Weights weights() {
        return new Weights(multiplicity, weightedMultiplicity, inclusionProbability(), 1.0 / lastArrivalWeight);
    }

    @Nullable
    public synchronized Stratum stratum() {
        return stratum;
    }

    public synchronized void stratum(@Nullable Stratum stratum) {
        this.stratum = stratum;
    }

    @Nullable
    public synchronized Hardness hardness() {
        return hardness;
    }

    public synchronized void hardness(@Nullable Hardness hardness) {
        this.hardness = hardness;
    }

    /** Whether the query has been put in its strata, which is done once, on the first capture that has hits. */
    public synchronized boolean isPlaced() {
        return placed;
    }

    public synchronized void markPlaced() {
        placed = true;
    }

    public synchronized boolean isSampled() {
        return sampled;
    }

    public synchronized void markSampled() {
        sampled = true;
    }
}
