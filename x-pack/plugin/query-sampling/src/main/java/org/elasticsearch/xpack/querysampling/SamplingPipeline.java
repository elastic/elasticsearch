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
import org.elasticsearch.xpack.querysampling.sampling.SampleListener;
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;

import java.util.List;
import java.util.function.Consumer;

/**
 * Everything that happens to a captured search after it left the search thread: it is recognised if it was
 * seen before, the sampler decides whether it joins the sample, and if so the listeners are told. Must
 * only be driven from one thread at a time.
 */
public final class SamplingPipeline implements Consumer<CapturedSearch> {

    private static final Logger logger = LogManager.getLogger(SamplingPipeline.class);

    private final MultiplicityTracker tracker;
    private final QuerySampler sampler;
    private final List<SampleListener> listeners;

    public SamplingPipeline(MultiplicityTracker tracker, QuerySampler sampler, List<SampleListener> listeners) {
        this.tracker = tracker;
        this.sampler = sampler;
        this.listeners = List.copyOf(listeners);
    }

    @Override
    public void accept(CapturedSearch captured) {
        QueryFingerprint fingerprint = QueryFingerprint.of(captured.query());
        TrackedQuery tracked = tracker.record(fingerprint, captured.captureRate());
        if (tracked != null && sampler.offer(tracked)) {
            SampledQuery sampled = new SampledQuery(fingerprint, captured, tracked);
            for (SampleListener listener : listeners) {
                try {
                    listener.onSampled(sampled);
                } catch (Exception e) {
                    logger.debug("a sample listener failed", e);
                }
            }
        }
    }
}
