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
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.sampling.SampledQuery;

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
    private final LongAdder picked = new LongAdder();

    public SamplingPipeline(MultiplicityTracker tracker, QuerySampler sampler, List<Consumer<SampledQuery>> listeners) {
        this.tracker = tracker;
        this.sampler = sampler;
        this.listeners = List.copyOf(listeners);
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
        QueryFingerprint fingerprint = QueryFingerprint.of(captured.query());
        TrackedQuery tracked = tracker.record(fingerprint, captured.captureRate());
        // A search that found nothing says nothing of the data, its field may not even exist, and every field makes a space. So the
        // strata are given on the first capture that has hits, which is not always the first capture.
        if (tracked != null && tracked.isPlaced() == false && captured.hits().isEmpty() == false) {
            tracked.markPlaced();
            sampler.assignStratum(tracked, captured.query().field(), captured.query().queryVector());
            sampler.assignHardness(tracked, captured.query().field(), captured.query().queryVector().length, captured.hits());
        }
        if (tracked != null && sampler.offer(tracked)) {
            picked.increment();
            tell(new SampledQuery(fingerprint, captured, tracked));
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
