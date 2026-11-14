/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.groundtruth.CostBudget;
import org.elasticsearch.xpack.querysampling.sampling.EventSlice;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.sampling.SelectivityStrata;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Processes captured searches off the search thread: dedup, sampling decision, listeners. The tracker and the strata keep
 * state that is not made for several threads. The pool that runs this has one thread, and the lock keeps one search at a
 * time even if someone gives the pool more; it is not contended, as nothing else calls it.
 */
public final class SamplingPipeline implements Consumer<CapturedSearch> {

    private static final Logger logger = LogManager.getLogger(SamplingPipeline.class);

    private final MultiplicityTracker tracker;
    private final QuerySampler sampler;
    private final List<Consumer<SampledQuery>> listeners;
    private final CostBudget budget;
    private final EventSlice events;
    private final SelectivityStrata selectivity;
    private final LongAdder picked = new LongAdder();

    public SamplingPipeline(
        MultiplicityTracker tracker,
        QuerySampler sampler,
        List<Consumer<SampledQuery>> listeners,
        CostBudget budget,
        EventSlice events,
        SelectivityStrata selectivity
    ) {
        this.events = events;
        this.selectivity = selectivity;
        this.tracker = tracker;
        this.sampler = sampler;
        this.listeners = List.copyOf(listeners);
        this.budget = budget;
    }

    public long picked() {
        return picked.sum();
    }

    public long starved() {
        return sampler.starved();
    }

    public double acceptanceScale() {
        return sampler.effectiveScale();
    }

    @Override
    public synchronized void accept(CapturedSearch captured) {
        // time over capture probability is an unbiased estimate of the time of all searches this one stands for;
        // sub-millisecond searches count as 1ms, as exact searches do
        budget.earn(Math.max(1, captured.tookMillis()) / captured.captureRate());
        QueryFingerprint fingerprint = QueryFingerprint.of(captured.query());
        TrackedQuery tracked = tracker.record(fingerprint, captured.captureRate());
        if (tracked != null && tracked.weights().multiplicity() == 1) {
            sampler.assignStratum(tracked, captured.query().field(), captured.query().queryVector());
            sampler.assignHardness(tracked, captured.query().field(), captured.query().queryVector().length, captured.hits());
            selectivity.assign(tracked, captured.query());
        }
        if (tracked != null && sampler.offer(tracked)) {
            picked.increment();
            tell(new SampledQuery(fingerprint, captured, tracked));
        }
        // event selection is independent of tracking and picking
        double sliceRate = events.draw();
        if (sliceRate > 0) {
            TrackedQuery event = TrackedQuery.event(captured.captureRate(), sliceRate);
            // events of the same query are distinct documents, so the id is random rather than derived from the query
            tell(SampledQuery.event(fingerprint, captured, event, UUIDs.randomBase64UUID()));
        }
    }

    private void tell(SampledQuery sampled) {
        for (Consumer<SampledQuery> listener : listeners) {
            try {
                listener.accept(sampled);
            } catch (Exception e) {
                logger.debug("a sample listener failed", e);
            }
        }
    }
}
