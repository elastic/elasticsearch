/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingStatsAction;
import org.elasticsearch.xpack.querysampling.action.TransportQuerySamplingStatsAction;
import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureFilter;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.rest.RestQuerySamplingStatsAction;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.storage.Tier1Buffer;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Keeps a small, continuously maintained sample of live kNN queries so that production recall can be
 * estimated without replaying all traffic. The pipeline runs on the coordinating node and is designed
 * to stay off the search critical path.
 */
public class QuerySamplingPlugin extends Plugin implements ActionPlugin {

    public static final FeatureFlag QUERY_SAMPLING_FEATURE_FLAG = new FeatureFlag("query_sampling");

    static final String THREAD_POOL_NAME = "query_sampling";
    private static final int QUEUE_SIZE = 1000;
    private static final int MAX_DISTINCT_QUERIES = 100_000;
    private static final TimeValue MULTIPLICITY_WINDOW = TimeValue.timeValueHours(1);
    private static final int TIER1_CAPACITY = 10_000;
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
        MultiplicityTracker tracker = new MultiplicityTracker(MAX_DISTINCT_QUERIES, MULTIPLICITY_WINDOW, System::nanoTime);
        Tier1Buffer buffer = new Tier1Buffer(TIER1_CAPACITY);
        SamplingPipeline pipeline = new SamplingPipeline(
            tracker,
            new QuerySampler(ACCEPTANCE_SCALE, HEAD_THRESHOLD, Randomness.get()),
            List.of(buffer)
        );
        CaptureHandoff handoff = new CaptureHandoff(services.threadPool().executor(THREAD_POOL_NAME), pipeline);
        QueryCaptureFilter filter = new QueryCaptureFilter(services.clusterService().getClusterSettings(), handoff);
        captureFilter.set(filter);
        return List.of(new QuerySamplingService(filter, handoff, tracker, buffer));
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(new ActionHandler(QuerySamplingStatsAction.INSTANCE, TransportQuerySamplingStatsAction.class));
    }

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(new RestQuerySamplingStatsAction());
    }

    @Override
    public Collection<MappedActionFilter> getMappedActionFilters() {
        // the settings stay registered either way, but without the flag nothing may act on them
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(captureFilter.get());
    }
}
