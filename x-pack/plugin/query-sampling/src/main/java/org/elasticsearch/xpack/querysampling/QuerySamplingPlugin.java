/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureFilter;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;

import java.util.Collection;
import java.util.List;

/**
 * Keeps a small, continuously maintained sample of live kNN queries so that production recall can be
 * estimated without replaying all traffic. The pipeline runs on the coordinating node and is designed
 * to stay off the search critical path.
 */
public class QuerySamplingPlugin extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(QuerySamplingPlugin.class);

    static final String THREAD_POOL_NAME = "query_sampling";
    private static final int QUEUE_SIZE = 1000;
    private static final int MAX_DISTINCT_QUERIES = 100_000;
    private static final double ACCEPTANCE_SCALE = 1.0;
    private static final long HEAD_THRESHOLD = 100;

    private final SetOnce<QueryCaptureFilter> captureFilter = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return QuerySamplingSettings.getSettings();
    }

    /**
     * One thread is plenty: it only has to keep up with the captured fraction of the traffic, and a
     * burst beyond what the queue holds is dropped rather than slowing searches down.
     */
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                THREAD_POOL_NAME,
                1,
                QUEUE_SIZE,
                "xpack.query_sampling.thread_pool",
                EsExecutors.TaskTrackingConfig.DO_NOT_TRACK
            )
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        MultiplicityTracker tracker = new MultiplicityTracker(MAX_DISTINCT_QUERIES);
        QuerySampler sampler = new QuerySampler(ACCEPTANCE_SCALE, HEAD_THRESHOLD, Randomness.get());
        CaptureHandoff handoff = new CaptureHandoff(services.threadPool().executor(THREAD_POOL_NAME), captured -> {
            TrackedQuery tracked = tracker.record(QueryFingerprint.of(captured.query()));
            if (tracked != null && sampler.offer(tracked)) {
                logger.trace("sampled kNN search on field [{}], seen {} times", captured.query().field(), tracked.multiplicity());
            }
        });
        captureFilter.set(new QueryCaptureFilter(services.clusterService().getClusterSettings(), handoff));
        return List.of();
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        return List.of(captureFilter.get());
    }
}
