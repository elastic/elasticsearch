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
import org.elasticsearch.xpack.querysampling.storage.SampledQuery;
import org.elasticsearch.xpack.querysampling.storage.Tier1Buffer;

import java.util.function.Consumer;

/**
 * Everything that happens to a captured search after it left the search thread: it is recognised if it was
 * seen before, the sampler decides whether it joins the sample, and if so it is stored. Must only be
 * driven from one thread at a time.
 */
public final class SamplingPipeline implements Consumer<CapturedSearch> {

    private static final Logger logger = LogManager.getLogger(SamplingPipeline.class);

    private final MultiplicityTracker tracker;
    private final QuerySampler sampler;
    private final Tier1Buffer buffer;

    public SamplingPipeline(MultiplicityTracker tracker, QuerySampler sampler, Tier1Buffer buffer) {
        this.tracker = tracker;
        this.sampler = sampler;
        this.buffer = buffer;
    }

    @Override
    public void accept(CapturedSearch captured) {
        QueryFingerprint fingerprint = QueryFingerprint.of(captured.query());
        TrackedQuery tracked = tracker.record(fingerprint);
        if (tracked != null && sampler.offer(tracked)) {
            if (buffer.add(new SampledQuery(fingerprint, captured, tracked)) == false) {
                logger.debug("tier 1 buffer is full, dropping sampled kNN search on field [{}]", captured.query().field());
            }
        }
    }
}
