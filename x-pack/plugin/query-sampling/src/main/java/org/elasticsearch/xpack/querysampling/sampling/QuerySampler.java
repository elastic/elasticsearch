/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.Random;

/**
 * Decides which distinct queries go into the sample.
 * <p>
 * A query that arrives {@code w} times should not be sampled {@code w} times as often as one that arrives
 * once, otherwise a handful of popular queries would fill the sample. Its {@code w}-th arrival is
 * therefore accepted with probability {@code γ·ln(1 + 1/w)}, which adds up to {@code γ·ln(1 + w)} accepted
 * arrivals after {@code w} of them: sampling grows with the logarithm of popularity instead of with
 * popularity itself. Queries that reached the head threshold are always accepted; the hottest few queries
 * carry a large part of the traffic and leaving them to chance would make estimates swing on one coin flip.
 * <p>
 * Only a fraction of the searches is captured, so {@code w} is the estimated number of arrivals and one
 * captured arrival can stand for several. It is then accepted with probability
 * {@code γ·ln((1 + w) / (1 + w − weight))}, the part of {@code γ·ln(1 + w)} it adds. That makes the
 * expected number of accepted arrivals of a query depend on its estimated traffic only, not on what
 * fraction of it was captured.
 */
public final class QuerySampler {

    private final double scale;
    private final long headThreshold;
    private final Random random;

    /**
     * @param scale         γ, the probability scale: how likely a never-seen query is picked (about 0.69·γ)
     * @param headThreshold estimated multiplicity from which a query is always picked
     */
    public QuerySampler(double scale, long headThreshold, Random random) {
        this.scale = scale;
        this.headThreshold = headThreshold;
        this.random = random;
    }

    /**
     * Probability that the arrival of a query that has now been seen {@code multiplicity} times is accepted,
     * when every arrival counts for one.
     */
    double acceptanceProbability(long multiplicity) {
        return acceptanceProbability(multiplicity, 1.0);
    }

    /**
     * @param multiplicity estimated arrivals of the query including this one
     * @param weight       how many of those this arrival stands for
     */
    double acceptanceProbability(double multiplicity, double weight) {
        if (multiplicity >= headThreshold) {
            return 1.0;
        }
        return Math.min(1.0, scale * Math.log1p(weight / (1.0 + multiplicity - weight)));
    }

    /**
     * Handles one arrival of a query, after it has been counted.
     *
     * @return whether the query was picked by this arrival, which happens at most once per query
     */
    public boolean offer(TrackedQuery query) {
        double probability = acceptanceProbability(query.weightedMultiplicity(), query.lastArrivalWeight());
        query.recordDraw(probability);
        if (query.isSampled() || random.nextDouble() >= probability) {
            return false;
        }
        query.markSampled();
        return true;
    }
}
