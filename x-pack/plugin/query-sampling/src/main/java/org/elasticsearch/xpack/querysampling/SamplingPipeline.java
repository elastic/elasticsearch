/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.CostBudget;
import org.elasticsearch.xpack.querysampling.sampling.EventSlice;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.sampling.SampleListener;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Everything that happens to a captured search after it left the search thread: it is recognised if it was
 * seen before, the sampler decides whether it joins the sample, and if so the listeners are told. It may also be kept as
 * an event. Must
 * only be driven from one thread at a time.
 */
public final class SamplingPipeline implements Consumer<CapturedSearch> {

    private static final Logger logger = LogManager.getLogger(SamplingPipeline.class);

    private final MultiplicityTracker tracker;
    private final QuerySampler sampler;
    private final List<SampleListener> listeners;
    private final CostBudget budget;
    private final EventSlice events;
    private final LongAdder picked = new LongAdder();
    private final LongAdder eventsKept = new LongAdder();

    public SamplingPipeline(MultiplicityTracker tracker, QuerySampler sampler, List<SampleListener> listeners) {
        this(tracker, sampler, listeners, new CostBudget(0.0, 0.0));
    }

    /**
     * @param budget earns from what the captured searches cost, which is how much exact searching can be afforded
     */
    public SamplingPipeline(MultiplicityTracker tracker, QuerySampler sampler, List<SampleListener> listeners, CostBudget budget) {
        this(tracker, sampler, listeners, budget, new EventSlice());
    }

    /**
     * @param events decides which captured searches are also kept as events, besides the queries that the sampler picks
     */
    public SamplingPipeline(
        MultiplicityTracker tracker,
        QuerySampler sampler,
        List<SampleListener> listeners,
        CostBudget budget,
        EventSlice events
    ) {
        this.events = events;
        this.tracker = tracker;
        this.sampler = sampler;
        this.listeners = List.copyOf(listeners);
        this.budget = budget;
    }

    /**
     * Distinct queries that were picked for the sample.
     */
    public long picked() {
        return picked.sum();
    }

    /**
     * Arrivals that were kept as events.
     */
    public long eventsKept() {
        return eventsKept.sum();
    }

    /**
     * Arrivals of queries that had no chance to be picked because the limit on the picks was reached.
     */
    public long starved() {
        return sampler.starved();
    }

    /**
     * γ as it is now, the setting multiplied with what keeps the picks at their target.
     */
    public double acceptanceScale() {
        return sampler.effectiveScale();
    }

    @Override
    public void accept(CapturedSearch captured) {
        // a search was captured with the probability it carries, so its time over that probability is an unbiased
        // estimate of the time of all the searches it stands for, those that were not captured included. The time is
        // in whole milliseconds, a search that took less than one counts as one, as an exact search does
        budget.earn(Math.max(1, captured.tookMillis()) / captured.captureRate());
        QueryFingerprint fingerprint = QueryFingerprint.of(captured.query());
        TrackedQuery tracked = tracker.record(fingerprint, captured.captureRate());
        if (tracked != null && tracked.multiplicity() == 1) {
            sampler.assignStratum(tracked, captured.query().field(), captured.query().queryVector());
            sampler.assignHardness(tracked, captured.query().field(), captured.query().queryVector().length, captured.hits());
        }
        if (tracked != null && sampler.offer(tracked)) {
            picked.increment();
            tell(new SampledQuery(fingerprint, captured, tracked));
        }
        // whether an arrival is kept as an event has nothing to do with the rest, not even with the query being tracked
        double sliceRate = events.draw();
        if (sliceRate > 0) {
            eventsKept.increment();
            tell(SampledQuery.event(fingerprint, captured, TrackedQuery.event(captured.captureRate(), sliceRate), events.newId()));
        }
    }

    private void tell(SampledQuery sampled) {
        for (SampleListener listener : listeners) {
            try {
                listener.onSampled(sampled);
            } catch (Exception e) {
                logger.debug("a sample listener failed", e);
            }
        }
    }
}
