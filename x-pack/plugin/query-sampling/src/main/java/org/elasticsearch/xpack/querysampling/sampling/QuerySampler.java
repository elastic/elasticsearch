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
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;

/**
 * Decides which distinct queries are picked. The {@code w}-th arrival is accepted with probability
 * {@code γ·ln(1 + 1/w)}, so picks grow with the log of popularity and a few popular queries cannot fill the sample.
 * Captured arrivals stand for several (weight), so the probability is {@code γ·ln((1 + w) / (1 + w − weight))}, which
 * does not depend on the capture fraction. Head queries are always accepted so estimates do not swing on one coin
 * flip. Picks are tilted by {@link SpatialStrata} and {@link HardnessStrata}, and γ is adjusted by {@link ScaleRegulator}.
 */
public final class QuerySampler {

    private volatile double scale;
    private volatile long headThreshold;
    private final Random random;
    private final PickBudget budget;
    private final SpatialStrata spatial;
    private final HardnessStrata hardness;
    private final LongAdder starved = new LongAdder();
    private final ScaleRegulator regulator = new ScaleRegulator();

    /**
     * @param scale         γ, the probability scale
     * @param headThreshold estimated multiplicity from which a query is always picked
     */
    public QuerySampler(double scale, long headThreshold, Random random, PickBudget budget, SpatialStrata spatial) {
        this.spatial = spatial;
        this.hardness = new HardnessStrata();
        this.scale = scale;
        this.headThreshold = headThreshold;
        this.random = random;
        this.budget = budget;
    }

    public void watch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(QuerySamplingSettings.ACCEPTANCE_SCALE, value -> this.scale = value);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.HEAD_THRESHOLD, value -> this.headThreshold = value);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.MAX_PICKS_PER_HOUR, budget::perHour);
        clusterSettings.initializeAndWatch(QuerySamplingSettings.TARGET_PICKS_PER_HOUR, regulator::targetPerHour);
        spatial.watch(clusterSettings);
        hardness.watch(clusterSettings);
    }

    /**
     * Adjusts γ every ten seconds; a no-op while no target is set, so it can run unconditionally.
     */
    public Scheduler.Cancellable startRegulation(ThreadPool threadPool, Executor executor) {
        long[] last = { System.nanoTime() };
        return threadPool.scheduleWithFixedDelay(() -> {
            long now = System.nanoTime();
            regulator.observe((now - last[0]) / 1_000_000_000.0);
            last[0] = now;
        }, TimeValue.timeValueSeconds(10), executor);
    }

    /** Arrivals given no chance to be picked because the pick limit was reached. */
    public long starved() {
        return starved.sum();
    }

    public double effectiveScale() {
        return scale * regulator.adjustment();
    }

    /**
     * Uncapped on purpose: strata factors are applied before capping, otherwise a raised γ could not reach its target
     * for queries in crowded clusters.
     */
    private double uncappedAcceptance(double multiplicity, double weight) {
        return effectiveScale() * Math.log1p(weight / (1.0 + multiplicity - weight));
    }

    public void assignStratum(TrackedQuery query, String field, float[] vector) {
        spatial.assign(field, vector, stratum -> query.stratum(stratum));
    }

    public void assignHardness(TrackedQuery query, String field, int dims, List<CapturedSearch.Hit> hits) {
        query.hardness(hardness.assign(field, dims, hits));
    }

    /**
     * @return whether this arrival picked the query, which happens at most once per query
     */
    public boolean offer(TrackedQuery query) {
        double multiplicity = query.weightedMultiplicity();
        boolean head = multiplicity >= headThreshold;
        double probability = 1.0;
        if (head == false) {
            double strata = spatial.factor(query.stratum()) * hardness.factor(query.hardness());
            probability = Math.min(1.0, uncappedAcceptance(multiplicity, query.lastArrivalWeight()) * strata);
        }
        if (head == false && budget.available() == false) {
            // limit reached: record probability 0 like any other draw so estimates stay unbiased; head queries are exempt
            probability = 0.0;
            starved.increment();
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
