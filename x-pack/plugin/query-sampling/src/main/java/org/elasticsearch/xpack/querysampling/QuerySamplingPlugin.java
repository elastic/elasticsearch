/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchLogContext;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.logging.activity.ActivityLogListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.xpack.querysampling.capture.CaptureHandoff;
import org.elasticsearch.xpack.querysampling.capture.QueryCaptureListener;
import org.elasticsearch.xpack.querysampling.dedup.MultiplicityTracker;
import org.elasticsearch.xpack.querysampling.groundtruth.CostBudget;
import org.elasticsearch.xpack.querysampling.groundtruth.GroundTruthWorker;
import org.elasticsearch.xpack.querysampling.sampling.EventSlice;
import org.elasticsearch.xpack.querysampling.sampling.PickBudget;
import org.elasticsearch.xpack.querysampling.sampling.QuerySampler;
import org.elasticsearch.xpack.querysampling.sampling.SelectivityStrata;
import org.elasticsearch.xpack.querysampling.sampling.SpatialStrata;
import org.elasticsearch.xpack.querysampling.storage.GoldenIndex;
import org.elasticsearch.xpack.querysampling.storage.GoldenPromotionJob;
import org.elasticsearch.xpack.querysampling.storage.GoldenReader;
import org.elasticsearch.xpack.querysampling.storage.GoldenUpdater;
import org.elasticsearch.xpack.querysampling.storage.GoldenWriter;
import org.elasticsearch.xpack.querysampling.storage.MasterTasks;
import org.elasticsearch.xpack.querysampling.storage.QuerySamplingIndex;
import org.elasticsearch.xpack.querysampling.storage.SampleRetention;
import org.elasticsearch.xpack.querysampling.storage.SampleWriter;
import org.elasticsearch.xpack.querysampling.storage.StoredSamples;
import org.elasticsearch.xpack.querysampling.storage.WeightsRefresher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.core.ClientHelper.QUERY_SAMPLING_ORIGIN;

/** Samples live kNN queries to estimate production recall; runs on the coordinating node, off the search path. */
public class QuerySamplingPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

    public static final FeatureFlag QUERY_SAMPLING_FEATURE_FLAG = new FeatureFlag("query_sampling");

    static final String THREAD_POOL_NAME = "query_sampling";
    private static final int QUEUE_SIZE = 1000;
    private static final int MAX_DISTINCT_QUERIES = 100_000;
    // millis of exact-search time that can be saved up or owed
    private static final double MAX_EXACT_SEARCH_CREDIT_MILLIS = 10_000;
    private static final int WRITE_BATCH_SIZE = 100;
    private static final int MAX_PENDING_WRITES = 1000;
    private static final TimeValue WRITE_INTERVAL = TimeValue.timeValueSeconds(1);

    private final SetOnce<QueryCaptureListener> captureListener = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        return QuerySamplingSettings.getSettings();
    }

    // one thread suffices; a burst beyond the queue is dropped rather than slowing searches
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
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        ClusterSettings clusterSettings = services.clusterService().getClusterSettings();
        Executor generic = services.threadPool().generic();
        MultiplicityTracker tracker = new MultiplicityTracker(MAX_DISTINCT_QUERIES, TimeValue.timeValueHours(1), System::nanoTime);
        tracker.watch(clusterSettings);
        // new id per run: weights only make sense against the counts this run keeps
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
        TimeValue retentionPeriod = QuerySamplingSettings.RETENTION.get(services.clusterService().getSettings());
        SampleRetention retention = new SampleRetention(
            (request, listener) -> client.execute(DeleteByQueryAction.INSTANCE, request, listener),
            services.threadPool()::absoluteTimeInMillis,
            retentionPeriod
        );
        // what is read and written in bulk is parsed and built by the thread that completes the call, so that is an executor's
        BiConsumer<SearchRequest, ActionListener<SearchResponse>> search = ListenerThread.on(generic, client::search);
        GoldenReader goldenReader = new GoldenReader(search, services.xContentRegistry());
        // the schedule only adds; recomputing ground truth searches user data, so it runs as the caller
        GoldenUpdater goldenUpdater = new GoldenUpdater(
            goldenReader,
            new StoredSamples(search, services.xContentRegistry()),
            new GoldenWriter(
                search,
                ListenerThread.on(generic, client::index),
                ListenerThread.on(generic, client::bulk),
                ListenerThread.on(generic, client::update),
                services.threadPool()::absoluteTimeInMillis
            ),
            null,
            null,
            null
        );
        GoldenPromotionJob goldenPromotion = new GoldenPromotionJob(goldenUpdater::update);
        goldenPromotion.watch(clusterSettings);
        // the jobs of which one node is enough: the elected master runs them
        List<MasterTasks.Task> masterTasks = new ArrayList<>();
        masterTasks.add(new MasterTasks.Task(retention::run, SampleRetention.interval(retentionPeriod)));
        TimeValue goldenInterval = QuerySamplingSettings.GOLDEN_PROMOTION_INTERVAL.get(services.clusterService().getSettings());
        if (goldenInterval.millis() > 0) {
            masterTasks.add(new MasterTasks.Task(goldenPromotion::run, goldenInterval));
        }
        services.clusterService().addLocalNodeMasterListener(new MasterTasks(services.threadPool(), generic, masterTasks));
        // the values given here are replaced by the settings in the watch calls
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
        EventSlice eventSlice = new EventSlice();
        eventSlice.watch(clusterSettings);
        SelectivityStrata selectivity = new SelectivityStrata(client::search, services.threadPool()::relativeTimeInMillis, generic);
        selectivity.watch(clusterSettings);
        SamplingPipeline pipeline = new SamplingPipeline(tracker, sampler, List.of(writer::onSampled), budget, eventSlice, selectivity);
        // runs as the plugin since no user asked for these searches
        GroundTruthWorker groundTruthWorker = new GroundTruthWorker(
            client::search,
            client::bulk,
            client::search,
            services.xContentRegistry(),
            budget,
            samplerId,
            services.threadPool()::absoluteTimeInMillis,
            services.threadPool().generic()
        );
        groundTruthWorker.start(services.threadPool(), generic, clusterSettings);
        CaptureHandoff handoff = new CaptureHandoff(services.threadPool().executor(THREAD_POOL_NAME), pipeline);
        captureListener.set(new QueryCaptureListener(clusterSettings, handoff, services.threadPool()::relativeTimeInMillis));
        return List.of();
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(QuerySamplingIndex.descriptor(), GoldenIndex.descriptor());
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
    public Collection<ActivityLogListener<SearchLogContext, SearchRequest>> getSearchLogListeners() {
        // settings stay registered, but without the flag nothing may act on them
        if (QUERY_SAMPLING_FEATURE_FLAG.isEnabled() == false) {
            return List.of();
        }
        return List.of(captureListener.get());
    }
}
