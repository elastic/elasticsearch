/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.sampling;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.querysampling.QuerySamplingSettings;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.Random;
import java.util.concurrent.Executor;

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
 * <p>
 * Queries of the sparse parts of the vector space are picked more often, and those of the dense parts less, see
 * {@link SpatialStrata}.
 * <p>
 * With a target for the pick rate, γ is multiplied with an adjustment that follows the traffic, see {@link ScaleRegulator}.
 */
public final class QuerySampler {

    private volatile double scale;
    private volatile long headThreshold;
    private final Random random;
    private final PickBudget budget;
    private final SpatialStrata spatial;
    private final ScaleRegulator regulator = new ScaleRegulator();

    /**
     * @param scale         γ, the probability scale: how likely a never-seen query is picked (about 0.69·γ)
     * @param headThreshold estimated multiplicity from which a query is always picked
     */
    public QuerySampler(double scale, long headThreshold, Random random) {
        this(scale, headThreshold, random, new PickBudget(System::nanoTime));
    }

    /**
     * @param budget limits how many queries are picked, apart from the head queries
     */
    public QuerySampler(double scale, long headThreshold, Random random, PickBudget budget) {
        this(scale, headThreshold, random, budget, new SpatialStrata(0));
    }

    /**
     * @param spatial balances the picks over the vector space
     */
    public QuerySampler(double scale, long headThreshold, Random random, PickBudget budget, SpatialStrata spatial) {
        this.spatial = spatial;
        this.scale = scale;
        this.headThreshold = headThreshold;
        this.random = random;
        this.budget = budget;
    }

    /**
     * Follows the settings that tune the sampler, now and when they change.
     */
    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.ACCEPTANCE_SCALE, value -> this.scale = value);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.HEAD_THRESHOLD, value -> this.headThreshold = value);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.MAX_PICKS_PER_HOUR, budget::perHour);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.TARGET_PICKS_PER_HOUR, regulator::targetPerHour);
        spatial.watch(clusterSettings);
    }

    /**
     * Keeps γ in line with the pick rate, every ten seconds until the thread pool shuts down. It has nothing to do
     * for a node that has no target, but is there for when one is set.
     */
    public Scheduler.Cancellable startRegulation(ThreadPool threadPool, Executor executor) {
        long[] last = { System.nanoTime() };
        return threadPool.scheduleWithFixedDelay(() -> {
            long now = System.nanoTime();
            regulate((now - last[0]) / 1_000_000_000.0);
            last[0] = now;
        }, TimeValue.timeValueSeconds(10), executor);
    }

    /**
     * Takes note of the picks of the last {@code seconds}, which is what the adjustment of γ is worked out from.
     */
    void regulate(double seconds) {
        regulator.observe(seconds);
    }

    /**
     * γ as it is now, the setting multiplied with the adjustment that gets the picks to their target.
     */
    public double effectiveScale() {
        return scale * regulator.adjustment();
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
        return Math.min(1.0, effectiveScale() * Math.log1p(weight / (1.0 + multiplicity - weight)));
    }

    /**
     * Puts a query that has just been seen for the first time in the part of the vector space it belongs to, which
     * the sampler needs to balance the picks over the space.
     */
    public void assignStratum(TrackedQuery query, String field, float[] vector) {
        query.stratum(spatial.assign(field, vector));
    }

    /**
     * Handles one arrival of a query, after it has been counted.
     *
     * @return whether the query was picked by this arrival, which happens at most once per query
     */
    public boolean offer(TrackedQuery query) {
        double multiplicity = query.weightedMultiplicity();
        boolean head = multiplicity >= headThreshold;
        double probability = acceptanceProbability(multiplicity, query.lastArrivalWeight());
        if (head == false) {
            probability = Math.min(1.0, probability * spatial.factor(query.stratum()));
        }
        if (head == false && budget.available() == false) {
            // the limit on the picks is reached: the query has no chance now, and that is what is recorded for it, as for
            // any other probability, which keeps the estimates right. The head queries are never held back
            probability = 0.0;
        }
        query.recordDraw(probability);
        if (query.isSampled() || random.nextDouble() >= probability) {
            return false;
        }
        query.markSampled();
        if (head == false) {
            budget.take();
            regulator.recordPick();
        }
        return true;
    }
}
