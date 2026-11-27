/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Decides which distinct queries are picked. The {@code w}-th arrival is accepted with probability
 * {@code γ·ln(1 + 1/w)}, so picks grow with the log of popularity and a few popular queries cannot fill the sample.
 * Captured arrivals stand for several (weight), so the probability is {@code γ·ln((1 + w) / (1 + w − weight))}: over a
 * query's life the probabilities add up to {@code γ·ln(1 + w)} whatever the capture fraction, but one arrival's does
 * depend on it, and the first captured arrival is accepted with {@code γ·ln(1 + weight)}, capped at 1. Head queries,
 * captured at least the head threshold times, are always accepted so estimates do not swing on one coin flip; captures
 * are counted rather than estimated searches, since one capture stands for many searches and would make any captured
 * query look popular.
 */
public final class QuerySampler {

    // what is left of the chance of a pick while the pick limit is reached: never 0, so that every query keeps a chance
    static final double LIMITED_FACTOR = 0.01;

    private final double scale;
    private final long headThreshold;
    private final Supplier<Random> random;
    private final PickBudget budget;
    private final LongAdder starved = new LongAdder();

    /**
     * @param scale         γ, the probability scale
     * @param headThreshold captured arrivals from which a query is always picked
     */
    public QuerySampler(double scale, long headThreshold, Supplier<Random> random, PickBudget budget) {
        this.scale = scale;
        this.headThreshold = headThreshold;
        this.random = random;
        this.budget = budget;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.MAX_PICKS_PER_HOUR, budget::perHour);
    }

    /** Arrivals whose chance to be picked was cut because the pick limit was reached. */
    public long starved() {
        return starved.sum();
    }

    /** The chance of a pick before it is capped at 1. */
    private double uncappedAcceptance(double multiplicity, double weight) {
        return scale * Math.log1p(weight / (1.0 + multiplicity - weight));
    }

    /**
     * @return whether this arrival picked the query, which happens at most once per query
     */
    public boolean offer(TrackedQuery query) {
        TrackedQuery.Weights weights = query.weights();
        double multiplicity = weights.weightedMultiplicity();
        // The hottest queries are always picked, without using the limit. The damping gives a query that is seen a lot a small chance
        // of being picked, so its weight, one over that chance, is large, and one coin flip on it would swing the whole estimate. At a
        // chance of 1 its weight is 1 and it adds no variance. There are few of them (a query is head after headThreshold captures),
        // and a limit must not drop the queries that matter most.
        boolean head = weights.multiplicity() >= headThreshold;
        double probability = 1.0;
        if (head == false) {
            probability = Math.min(1.0, uncappedAcceptance(multiplicity, 1.0 / weights.captureRate()));
        }
        if (head == false && budget.available() == false) {
            // limit reached: the chance is cut, not taken away, so every query keeps one and estimates stay unbiased; a few
            // picks may pass the limit. Head queries are exempt. A query that is picked already is cut as well, also when its
            // own pick used the limit up, on purpose: the limit is shared, so without that pick another query would have used
            // it, and what the chance would have been is not known. Replaying recorded traffic, weights made this way add up
            // to 100.2% of the searches, and leaving the picked queries out of the cut gave 85%
            probability *= LIMITED_FACTOR;
            starved.increment();
        }
        query.recordDraw(probability);
        if (query.isSampled() || random.get().nextDouble() >= probability) {
            return false;
        }
        query.markSampled();
        if (head == false) {
            budget.take();
        }
        return true;
    }
}
