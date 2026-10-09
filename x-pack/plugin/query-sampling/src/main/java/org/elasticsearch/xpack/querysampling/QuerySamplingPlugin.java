/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.support.MappedActionFilter;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingGroundTruthAction;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingRecallAction;
import org.elasticsearch.xpack.querysampling.action.QuerySamplingStatsAction;
import org.elasticsearch.xpack.querysampling.action.TransportQuerySamplingGroundTruthAction;
import org.elasticsearch.xpack.querysampling.action.TransportQuerySamplingRecallAction;
import org.elasticsearch.xpack.querysampling.action.TransportQuerySamplingStatsAction;
import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureFilter;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.groundtruth.CostBudget;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruthWorker;
import org.elasticsearch.xpack.querysampling.rest.RestQuerySamplingGroundTruthAction;
import org.elasticsearch.xpack.querysampling.rest.RestQuerySamplingRecallAction;
import org.elasticsearch.xpack.querysampling.rest.RestQuerySamplingStatsAction;
import org.elasticsearch.xpack.querysampling.sampling.PickBudget;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.sampling.SpatialStrata;
import org.elasticsearch.xpack.querysampling.storage.QuerySamplingIndex;
import org.elasticsearch.xpack.querysampling.storage.SampleRetention;
import org.elasticsearch.xpack.querysampling.storage.SampleWriter;
import org.elasticsearch.xpack.querysampling.storage.WeightsRefresher;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/**
 * Keeps a small, continuously maintained sample of live kNN queries so that production recall can be
 * estimated without replaying all traffic. The pipeline runs on the coordinating node and is designed
 * to stay off the search critical path.
 */
public class QuerySamplingPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

    public static final FeatureFlag QUERY_SAMPLING_FEATURE_FLAG = new FeatureFlag("query_sampling");

    static final String THREAD_POOL_NAME = "query_sampling";
    private static final int QUEUE_SIZE = 1000;
    private static final int MAX_DISTINCT_QUERIES = 100_000;
    // the most exact searching that can be saved up or owed, in milliseconds of search time
    private static final double MAX_EXACT_SEARCH_CREDIT_MILLIS = 10_000;
    private static final int WRITE_BATCH_SIZE = 100;
    private static final int MAX_PENDING_WRITES = 1000;
    private static final TimeValue WRITE_INTERVAL = TimeValue.timeValueSeconds(1);

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
        ClusterSettings clusterSettings = services.clusterService().getClusterSettings();
        MultiplicityTracker tracker = new MultiplicityTracker(MAX_DISTINCT_QUERIES, TimeValue.timeValueHours(1), System::nanoTime);
        tracker.watch(clusterSettings);
        // a new id for every run of the sampler: its weights only make sense against the counts it keeps
        OriginSettingClient client = new OriginSettingClient(services.client(), QUERY_SAMPLING_ORIGIN);
        String samplerId = UUIDs.randomBase64UUID();
        WeightsRefresher refresher = new WeightsRefresher(
            samplerId,
            client::bulk,
            services.threadPool(),
            services.threadPool().generic(),
            services.threadPool()::absoluteTimeInMillis,
            tracker::isTracking,
            WRITE_BATCH_SIZE,
            QuerySamplingSettings.WEIGHTS_REFRESH_INTERVAL.get(services.clusterService().getSettings())
        );
        SampleWriter writer = new SampleWriter(
            samplerId,
            client::bulk,
            services.threadPool(),
            services.threadPool().generic(),
            services.threadPool()::absoluteTimeInMillis,
            refresher::written,
            WRITE_BATCH_SIZE,
            MAX_PENDING_WRITES,
            WRITE_INTERVAL
        );
        SampleRetention retention = new SampleRetention(
            (request, listener) -> client.execute(DeleteByQueryAction.INSTANCE, request, listener),
            () -> services.clusterService().state().nodes().isLocalNodeElectedMaster(),
            services.threadPool()::absoluteTimeInMillis,
            QuerySamplingSettings.RETENTION.get(services.clusterService().getSettings())
        );
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled()) {
            retention.start(services.threadPool(), services.threadPool().generic());
        }
        // the values below only stand until the settings are read
        QuerySampler sampler = new QuerySampler(
            1.0,
            100,
            Randomness.get(),
            new PickBudget(System::nanoTime),
            new SpatialStrata(QuerySamplingSettings.SPATIAL_CLUSTERS.get(services.clusterService().getSettings()))
        );
        sampler.watch(clusterSettings);
        CostBudget budget = new CostBudget(0.0, MAX_EXACT_SEARCH_CREDIT_MILLIS);
        budget.watch(clusterSettings);
        SamplingPipeline pipeline = new SamplingPipeline(tracker, sampler, List.of(writer), budget);
        // the exact searches are done as the plugin: nobody is asking for them
        GroundTruthWorker groundTruthWorker = new GroundTruthWorker(
            client::search,
            client::bulk,
            client::search,
            services.xContentRegistry(),
            budget,
            samplerId,
            services.threadPool()::absoluteTimeInMillis
        );
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled()) {
            groundTruthWorker.start(services.threadPool(), services.threadPool().generic());
            sampler.startRegulation(services.threadPool(), services.threadPool().generic());
        }
        CaptureHandoff handoff = new CaptureHandoff(services.threadPool().executor(THREAD_POOL_NAME), pipeline);
        QueryCaptureFilter filter = new QueryCaptureFilter(clusterSettings, handoff);
        captureFilter.set(filter);
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled()) {
            filter.startRateUpdates(services.threadPool(), services.threadPool().generic());
        }
        return List.of(
            new QuerySamplingService(filter, handoff, tracker, pipeline, writer, refresher, retention, groundTruthWorker, budget)
        );
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(QuerySamplingStatsAction.INSTANCE, TransportQuerySamplingStatsAction.class),
            new ActionHandler(QuerySamplingGroundTruthAction.INSTANCE, TransportQuerySamplingGroundTruthAction.class),
            new ActionHandler(QuerySamplingRecallAction.INSTANCE, TransportQuerySamplingRecallAction.class)
        );
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
        return List.of(new RestQuerySamplingStatsAction(), new RestQuerySamplingGroundTruthAction(), new RestQuerySamplingRecallAction());
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(QuerySamplingIndex.descriptor());
    }

    @Override
    public String getFeatureName() {
        return "query_sampling";
    }

    @Override
    public String getFeatureDescription() {
        return "Stores the sampled kNN queries used to estimate the quality of search";
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
