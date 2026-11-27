/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureSink;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.sampling.PickBudget;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;

import java.util.Collection;
import java.util.List;

/**
 * Samples live kNN queries on the coordinating node: the capture sink copies a few searches, and the sampling pipeline
 * picks distinct queries from them off the search path. Disabled by default: it needs the feature flag, and then the
 * {@code xpack.query_sampling.enabled} setting. For now it runs on clusters with a single project only.
 */
public class KnnQuerySamplingPlugin extends Plugin {

    public static final FeatureFlag QUERY_SAMPLING_FEATURE_FLAG = new FeatureFlag("query_sampling");

    private static final Logger logger = LogManager.getLogger(KnnQuerySamplingPlugin.class);

    static final String THREAD_POOL_NAME = "query_sampling";
    private static final int QUEUE_SIZE = 1000;
    // about 130 bytes each, so 6.5 MB; at the default capture rate it is reached by about 700 searches a second of queries that are all
    // new, and a query that does not fit is not sampled, so it is better not to go lower without making the capture rate back off
    private static final int MAX_DISTINCT_QUERIES = 50_000;
    // γ, the scale of the pick probability: the first captured arrival of a query is picked with min(1, γ·ln(1 + 1/capture_rate)),
    // about 0.69·γ when every search is captured, falling as the query is searched more
    private static final double ACCEPTANCE_SCALE = 1.0;
    // captured arrivals from which a query is always picked, so that estimates do not hinge on one coin flip on a hot query
    private static final long HEAD_THRESHOLD = 100;
    // the window that the arrivals of a query are counted in: a query is forgotten between one and two windows after its last search
    private static final TimeValue MULTIPLICITY_WINDOW = TimeValue.timeValueHours(1);

    private final SetOnce<QueryCaptureSink> captureSink = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return QuerySamplingSettings.getSettings();
    }

    // one thread suffices; a burst beyond the queue is dropped rather than slowing searches. The tasks are tracked, so that the node
    // stats show how long the pipeline takes for a capture, which costs two clock reads of this thread, not of the search
    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(
            new FixedExecutorBuilder(
                settings,
                THREAD_POOL_NAME,
                1,
                QUEUE_SIZE,
                "xpack.query_sampling.thread_pool",
                EsExecutors.TaskTrackingConfig.DEFAULT
            )
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        if (services.projectResolver().supportsMultipleProjects()) {
            // the same request to same-named indices of two projects would count as a repeat, so nothing is captured yet
            logger.info("query sampling does not support multiple projects yet, so it is not started");
            return List.of();
        }
        ClusterSettings clusterSettings = services.clusterService().getClusterSettings();
        MultiplicityTracker tracker = new MultiplicityTracker(MAX_DISTINCT_QUERIES, MULTIPLICITY_WINDOW, System::nanoTime);
        QuerySampler sampler = new QuerySampler(ACCEPTANCE_SCALE, HEAD_THRESHOLD, Randomness::get, new PickBudget(System::nanoTime));
        sampler.watch(clusterSettings);
        // what is picked goes to the listeners of the pipeline; none are registered yet, storing the samples comes later
        SamplingPipeline pipeline = new SamplingPipeline(tracker, sampler, List.of());
        CaptureHandoff handoff = new CaptureHandoff(
            services.threadPool().executor(THREAD_POOL_NAME),
            pipeline,
            services.threadPool()::relativeTimeInMillis
        );
        captureSink.set(new QueryCaptureSink(clusterSettings, handoff, services.threadPool()::relativeTimeInMillis));
        return List.of();
    }

    /**
     * The sink that the query interceptor hands completed queries to, see {@link KnnQuerySamplingSinkProvider}. Without the
     * feature flag there is none: the settings stay registered, but nothing may act on them.
     */
    @Nullable
    QueryCaptureSink captureSink() {
        return captureSink.get();
    }
}
