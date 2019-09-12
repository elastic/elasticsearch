/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.Counter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

public class MachineLearningUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;
    private final XPackLicenseState licenseState;
    private final JobManagerHolder jobManagerHolder;
    private final boolean enabled;

    @Inject
    public MachineLearningUsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                               Environment environment, Client client,
                                               XPackLicenseState licenseState, JobManagerHolder jobManagerHolder) {
        super(XPackUsageFeatureAction.MACHINE_LEARNING.name(), transportService, clusterService,
              threadPool, actionFilters, indexNameExpressionResolver);
        this.client = client;
        this.licenseState = licenseState;
        this.jobManagerHolder = jobManagerHolder;
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(environment.settings());
    }

    @Override
    protected void masterOperation(Task task, XPackUsageRequest request, ClusterState state,
                                   ActionListener<XPackUsageFeatureResponse> listener) {
        if (enabled == false) {
            MachineLearningFeatureSetUsage usage = new MachineLearningFeatureSetUsage(licenseState.isMachineLearningAllowed(), enabled,
                Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), 0);
            listener.onResponse(new XPackUsageFeatureResponse(usage));
            return;
        }

        Map<String, Object> jobsUsage = new LinkedHashMap<>();
        Map<String, Object> datafeedsUsage = new LinkedHashMap<>();
        Map<String, Object> analyticsUsage = new LinkedHashMap<>();
        int nodeCount = mlNodeCount(state);

        // Step 3. Extract usage from data frame analytics stats and return usage response
        ActionListener<GetDataFrameAnalyticsStatsAction.Response> dataframeAnalyticsListener = ActionListener.wrap(
            response -> {
                addDataFrameAnalyticsUsage(response, analyticsUsage);
                MachineLearningFeatureSetUsage usage = new MachineLearningFeatureSetUsage(licenseState.isMachineLearningAllowed(),
                    enabled, jobsUsage, datafeedsUsage, analyticsUsage, nodeCount);
                listener.onResponse(new XPackUsageFeatureResponse(usage));
            },
            listener::onFailure
        );

        // Step 2. Extract usage from datafeeds stats and return usage response
        ActionListener<GetDatafeedsStatsAction.Response> datafeedStatsListener =
            ActionListener.wrap(response -> {
                addDatafeedsUsage(response, datafeedsUsage);
                GetDataFrameAnalyticsStatsAction.Request dataframeAnalyticsStatsRequest =
                    new GetDataFrameAnalyticsStatsAction.Request(GetDatafeedsStatsAction.ALL);
                dataframeAnalyticsStatsRequest.setPageParams(new PageParams(0, 10_000));
                client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, dataframeAnalyticsStatsRequest, dataframeAnalyticsListener);
            },
            listener::onFailure);

        // Step 1. Extract usage from jobs stats and then request stats for all datafeeds
        GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(MetaData.ALL);
        ActionListener<GetJobsStatsAction.Response> jobStatsListener = ActionListener.wrap(
            response -> {
                jobManagerHolder.getJobManager().expandJobs(MetaData.ALL, true, ActionListener.wrap(jobs -> {
                    addJobsUsage(response, jobs.results(), jobsUsage);
                    GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(
                        GetDatafeedsStatsAction.ALL);
                    client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
                }, listener::onFailure));
            }, listener::onFailure);

        // Step 0. Kick off the chain of callbacks by requesting jobs stats
        client.execute(GetJobsStatsAction.INSTANCE, jobStatsRequest, jobStatsListener);
    }

    private void addJobsUsage(GetJobsStatsAction.Response response, List<Job> jobs, Map<String, Object> jobsUsage) {
        StatsAccumulator allJobsDetectorsStats = new StatsAccumulator();
        StatsAccumulator allJobsModelSizeStats = new StatsAccumulator();
        ForecastStats allJobsForecastStats = new ForecastStats();

        Map<JobState, Counter> jobCountByState = new HashMap<>();
        Map<JobState, StatsAccumulator> detectorStatsByState = new HashMap<>();
        Map<JobState, StatsAccumulator> modelSizeStatsByState = new HashMap<>();
        Map<JobState, ForecastStats> forecastStatsByState = new HashMap<>();
        Map<JobState, Map<String, Long>> createdByByState = new HashMap<>();

        List<GetJobsStatsAction.Response.JobStats> jobsStats = response.getResponse().results();
        Map<String, Job> jobMap = jobs.stream().collect(Collectors.toMap(Job::getId, item -> item));
        Map<String, Long> allJobsCreatedBy = jobs.stream().map(this::jobCreatedBy)
            .collect(Collectors.groupingBy(item -> item, Collectors.counting()));;
        for (GetJobsStatsAction.Response.JobStats jobStats : jobsStats) {
            ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
            Job job = jobMap.get(jobStats.getJobId());
            int detectorsCount = job.getAnalysisConfig().getDetectors().size();
            double modelSize = modelSizeStats == null ? 0.0
                : jobStats.getModelSizeStats().getModelBytes();

            allJobsForecastStats.merge(jobStats.getForecastStats());
            allJobsDetectorsStats.add(detectorsCount);
            allJobsModelSizeStats.add(modelSize);

            JobState jobState = jobStats.getState();
            jobCountByState.computeIfAbsent(jobState, js -> Counter.newCounter()).addAndGet(1);
            detectorStatsByState.computeIfAbsent(jobState,
                js -> new StatsAccumulator()).add(detectorsCount);
            modelSizeStatsByState.computeIfAbsent(jobState,
                js -> new StatsAccumulator()).add(modelSize);
            forecastStatsByState.merge(jobState, jobStats.getForecastStats(), (f1, f2) -> f1.merge(f2));
            createdByByState.computeIfAbsent(jobState, js -> new HashMap<>())
                .compute(jobCreatedBy(job), (k, v) -> (v == null) ? 1L : (v + 1));
        }

        jobsUsage.put(MachineLearningFeatureSetUsage.ALL, createJobUsageEntry(jobs.size(), allJobsDetectorsStats,
            allJobsModelSizeStats, allJobsForecastStats, allJobsCreatedBy));
        for (JobState jobState : jobCountByState.keySet()) {
            jobsUsage.put(jobState.name().toLowerCase(Locale.ROOT), createJobUsageEntry(
                jobCountByState.get(jobState).get(),
                detectorStatsByState.get(jobState),
                modelSizeStatsByState.get(jobState),
                forecastStatsByState.get(jobState),
                createdByByState.get(jobState)));
        }
    }

    private String jobCreatedBy(Job job) {
        Map<String, Object> customSettings = job.getCustomSettings();
        if (customSettings == null || customSettings.containsKey(MachineLearningFeatureSetUsage.CREATED_BY) == false) {
            return "unknown";
        }
        // Replace non-alpha-numeric characters with underscores because
        // the values from custom settings become keys in the usage data
        return customSettings.get(MachineLearningFeatureSetUsage.CREATED_BY).toString().replaceAll("\\W", "_");
    }

    private Map<String, Object> createJobUsageEntry(long count, StatsAccumulator detectorStats,
                                                    StatsAccumulator modelSizeStats,
                                                    ForecastStats forecastStats, Map<String, Long> createdBy) {
        Map<String, Object> usage = new HashMap<>();
        usage.put(MachineLearningFeatureSetUsage.COUNT, count);
        usage.put(MachineLearningFeatureSetUsage.DETECTORS, detectorStats.asMap());
        usage.put(MachineLearningFeatureSetUsage.MODEL_SIZE, modelSizeStats.asMap());
        usage.put(MachineLearningFeatureSetUsage.FORECASTS, forecastStats.asMap());
        usage.put(MachineLearningFeatureSetUsage.CREATED_BY, createdBy);
        return usage;
    }

    private void addDatafeedsUsage(GetDatafeedsStatsAction.Response response, Map<String, Object> datafeedsUsage) {
        Map<DatafeedState, Counter> datafeedCountByState = new HashMap<>();

        List<GetDatafeedsStatsAction.Response.DatafeedStats> datafeedsStats = response.getResponse().results();
        for (GetDatafeedsStatsAction.Response.DatafeedStats datafeedStats : datafeedsStats) {
            datafeedCountByState.computeIfAbsent(datafeedStats.getDatafeedState(),
                ds -> Counter.newCounter()).addAndGet(1);
        }

        datafeedsUsage.put(MachineLearningFeatureSetUsage.ALL, createCountUsageEntry(response.getResponse().count()));
        for (DatafeedState datafeedState : datafeedCountByState.keySet()) {
            datafeedsUsage.put(datafeedState.name().toLowerCase(Locale.ROOT),
                createCountUsageEntry(datafeedCountByState.get(datafeedState).get()));
        }
    }

    private Map<String, Object> createCountUsageEntry(long count) {
        Map<String, Object> usage = new HashMap<>();
        usage.put(MachineLearningFeatureSetUsage.COUNT, count);
        return usage;
    }

    private void addDataFrameAnalyticsUsage(GetDataFrameAnalyticsStatsAction.Response response,
                                            Map<String, Object> dataframeAnalyticsUsage) {
        Map<DataFrameAnalyticsState, Counter> dataFrameAnalyticsStateCounterMap = new HashMap<>();

        for(GetDataFrameAnalyticsStatsAction.Response.Stats stats : response.getResponse().results()) {
            dataFrameAnalyticsStateCounterMap.computeIfAbsent(stats.getState(), ds -> Counter.newCounter()).addAndGet(1);
        }
        dataframeAnalyticsUsage.put(MachineLearningFeatureSetUsage.ALL, createCountUsageEntry(response.getResponse().count()));
        for (DataFrameAnalyticsState state : dataFrameAnalyticsStateCounterMap.keySet()) {
            dataframeAnalyticsUsage.put(state.name().toLowerCase(Locale.ROOT),
                createCountUsageEntry(dataFrameAnalyticsStateCounterMap.get(state).get()));
        }
    }

    private static int mlNodeCount(final ClusterState clusterState) {
        int mlNodeCount = 0;
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (MachineLearning.isMlNode(node)) {
                ++mlNodeCount;
            }
        }
        return mlNodeCount;
    }
}
