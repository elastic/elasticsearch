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
import org.elasticsearch.xpack.querysampling.groundtruth.CostBudget;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruthWorker;
import org.elasticsearch.xpack.querysampling.storage.SampleRetention;
import org.elasticsearch.xpack.querysampling.storage.SampleWriter;
import org.elasticsearch.xpack.querysampling.storage.WeightsRefresher;

/**
 * Node-local owner of the query sampling state. Each coordinating node samples the slice of traffic it
 * receives on its own; there is no coordination between nodes on the search path, and per-node state is
 * only combined when it is read.
 */
public class QuerySamplingService {

    private final QueryCaptureFilter filter;
    private final CaptureHandoff handoff;
    private final MultiplicityTracker tracker;
    private final SamplingPipeline pipeline;
    private final SampleWriter writer;
    private final WeightsRefresher refresher;
    private final SampleRetention retention;
    private final GroundTruthWorker groundTruthWorker;
    private final CostBudget budget;

    public QuerySamplingService(
        QueryCaptureFilter filter,
        CaptureHandoff handoff,
        MultiplicityTracker tracker,
        SamplingPipeline pipeline,
        SampleWriter writer,
        WeightsRefresher refresher,
        SampleRetention retention,
        GroundTruthWorker groundTruthWorker,
        CostBudget budget
    ) {
        this.filter = filter;
        this.handoff = handoff;
        this.tracker = tracker;
        this.pipeline = pipeline;
        this.writer = writer;
        this.refresher = refresher;
        this.retention = retention;
        this.groundTruthWorker = groundTruthWorker;
        this.budget = budget;
    }

    public QuerySamplingStats stats() {
        return new QuerySamplingStats(
            filter.knnSearches(),
            filter.captured(),
            handoff.dropped(),
            tracker.distinct(),
            tracker.untracked(),
            pipeline.picked(),
            writer.written(),
            writer.failed(),
            writer.dropped(),
            refresher.refreshed(),
            retention.deleted(),
            groundTruthWorker.computed(),
            groundTruthWorker.failed(),
            filter.effectiveCaptureRate(),
            budget.credit()
        );
    }
}
