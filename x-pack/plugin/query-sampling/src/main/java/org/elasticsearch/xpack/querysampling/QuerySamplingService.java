/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureFilter;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.storage.Tier1Buffer;

/**
 * Node-local owner of the query sampling state. Each coordinating node samples the slice of traffic it
 * receives on its own; there is no coordination between nodes on the search path, and per-node state is
 * only combined when it is read.
 */
public class QuerySamplingService {

    private final QueryCaptureFilter filter;
    private final CaptureHandoff handoff;
    private final MultiplicityTracker tracker;
    private final Tier1Buffer buffer;

    public QuerySamplingService(QueryCaptureFilter filter, CaptureHandoff handoff, MultiplicityTracker tracker, Tier1Buffer buffer) {
        this.filter = filter;
        this.handoff = handoff;
        this.tracker = tracker;
        this.buffer = buffer;
    }

    public QuerySamplingStats stats() {
        // the sampler picks a query at most once and a pick ends up either buffered or rejected
        long rejected = buffer.rejected();
        int buffered = buffer.size();
        return new QuerySamplingStats(
            filter.knnSearches(),
            filter.captured(),
            handoff.dropped(),
            tracker.distinct(),
            tracker.untracked(),
            buffered + rejected,
            buffered,
            buffer.withGroundTruth(),
            rejected
        );
    }
}
