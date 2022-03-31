/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.Counter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.Maps;
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
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class MachineLearningUsageTransportAction extends XPackUsageFeatureTransportAction {

    private final Client client;
    private final XPackLicenseState licenseState;
    private final JobManagerHolder jobManagerHolder;
    private final boolean enabled;

    @Inject
    public MachineLearningUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Environment environment,
        Client client,
        XPackLicenseState licenseState,
        JobManagerHolder jobManagerHolder
    ) {
        super(
            XPackUsageFeatureAction.MACHINE_LEARNING.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver
        );
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.licenseState = licenseState;
        this.jobManagerHolder = jobManagerHolder;
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(environment.settings());
    }

    @Override
    protected void masterOperation(
        Task task,
        XPackUsageRequest request,
        ClusterState state,
        ActionListener<XPackUsageFeatureResponse> listener
    ) {
        if (enabled == false) {
            MachineLearningFeatureSetUsage usage = new MachineLearningFeatureSetUsage(
                MachineLearningField.ML_API_FEATURE.checkWithoutTracking(licenseState),
                enabled,
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptyMap(),
                0
            );
            listener.onResponse(new XPackUsageFeatureResponse(usage));
            return;
        }

        Map<String, Object> jobsUsage = new LinkedHashMap<>();
        Map<String, Object> datafeedsUsage = new LinkedHashMap<>();
        Map<String, Object> analyticsUsage = new LinkedHashMap<>();
        int nodeCount = mlNodeCount(state);

        // Step 5. return final ML usage
        ActionListener<Map<String, Object>> inferenceUsageListener = ActionListener.wrap(inferenceUsage -> {
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new MachineLearningFeatureSetUsage(
                        MachineLearningField.ML_API_FEATURE.checkWithoutTracking(licenseState),
                        enabled,
                        jobsUsage,
                        datafeedsUsage,
                        analyticsUsage,
                        inferenceUsage,
                        nodeCount
                    )
                )
            );
        }, listener::onFailure);

        // Step 4. Extract usage from data frame analytics configs and then get inference usage
        ActionListener<GetDataFrameAnalyticsAction.Response> dataframeAnalyticsListener = ActionListener.wrap(response -> {
            addDataFrameAnalyticsUsage(response, analyticsUsage);
            addInferenceUsage(inferenceUsageListener);
        }, listener::onFailure);

        // Step 3. Extract usage from data frame analytics stats and then request data frame analytics configs
        ActionListener<GetDataFrameAnalyticsStatsAction.Response> dataframeAnalyticsStatsListener = ActionListener.wrap(response -> {
            addDataFrameAnalyticsStatsUsage(response, analyticsUsage);
            GetDataFrameAnalyticsAction.Request getDfaRequest = new GetDataFrameAnalyticsAction.Request(Metadata.ALL);
            getDfaRequest.setPageParams(new PageParams(0, 10_000));
            client.execute(GetDataFrameAnalyticsAction.INSTANCE, getDfaRequest, dataframeAnalyticsListener);
        }, listener::onFailure);

        // Step 2. Extract usage from datafeeds stats and then request stats for data frame analytics
        ActionListener<GetDatafeedsStatsAction.Response> datafeedStatsListener = ActionListener.wrap(response -> {
            addDatafeedsUsage(response, datafeedsUsage);
            GetDataFrameAnalyticsStatsAction.Request dataframeAnalyticsStatsRequest = new GetDataFrameAnalyticsStatsAction.Request(
                Metadata.ALL
            );
            dataframeAnalyticsStatsRequest.setPageParams(new PageParams(0, 10_000));
            client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, dataframeAnalyticsStatsRequest, dataframeAnalyticsStatsListener);
        }, listener::onFailure);

        // Step 1. Extract usage from jobs stats and then request stats for all datafeeds
        GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(Metadata.ALL);
        ActionListener<GetJobsStatsAction.Response> jobStatsListener = ActionListener.wrap(
            response -> jobManagerHolder.getJobManager().expandJobs(Metadata.ALL, true, ActionListener.wrap(jobs -> {
                addJobsUsage(response, jobs.results(), jobsUsage);
                GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(Metadata.ALL);
                client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
            }, listener::onFailure)),
            listener::onFailure
        );

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
        Map<String, Long> allJobsCreatedBy = jobs.stream()
            .map(this::jobCreatedBy)
            .collect(Collectors.groupingBy(item -> item, Collectors.counting()));
        ;
        for (GetJobsStatsAction.Response.JobStats jobStats : jobsStats) {
            Job job = jobMap.get(jobStats.getJobId());
            if (job == null) {
                // It's possible we can get job stats without a corresponding job config, if a
                // persistent task is orphaned. Omit these corrupt jobs from the usage info.
                continue;
            }
            int detectorsCount = job.getAnalysisConfig().getDetectors().size();
            ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
            double modelSize = modelSizeStats == null ? 0.0 : jobStats.getModelSizeStats().getModelBytes();

            allJobsForecastStats.merge(jobStats.getForecastStats());
            allJobsDetectorsStats.add(detectorsCount);
            allJobsModelSizeStats.add(modelSize);

            JobState jobState = jobStats.getState();
            jobCountByState.computeIfAbsent(jobState, js -> Counter.newCounter()).addAndGet(1);
            detectorStatsByState.computeIfAbsent(jobState, js -> new StatsAccumulator()).add(detectorsCount);
            modelSizeStatsByState.computeIfAbsent(jobState, js -> new StatsAccumulator()).add(modelSize);
            forecastStatsByState.merge(jobState, jobStats.getForecastStats(), ForecastStats::merge);
            createdByByState.computeIfAbsent(jobState, js -> new HashMap<>())
                .compute(jobCreatedBy(job), (k, v) -> (v == null) ? 1L : (v + 1));
        }

        jobsUsage.put(
            MachineLearningFeatureSetUsage.ALL,
            createJobUsageEntry(jobs.size(), allJobsDetectorsStats, allJobsModelSizeStats, allJobsForecastStats, allJobsCreatedBy)
        );
        for (JobState jobState : jobCountByState.keySet()) {
            jobsUsage.put(
                jobState.name().toLowerCase(Locale.ROOT),
                createJobUsageEntry(
                    jobCountByState.get(jobState).get(),
                    detectorStatsByState.get(jobState),
                    modelSizeStatsByState.get(jobState),
                    forecastStatsByState.get(jobState),
                    createdByByState.get(jobState)
                )
            );
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

    private Map<String, Object> createJobUsageEntry(
        long count,
        StatsAccumulator detectorStats,
        StatsAccumulator modelSizeStats,
        ForecastStats forecastStats,
        Map<String, Long> createdBy
    ) {
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
            datafeedCountByState.computeIfAbsent(datafeedStats.getDatafeedState(), ds -> Counter.newCounter()).addAndGet(1);
        }

        datafeedsUsage.put(MachineLearningFeatureSetUsage.ALL, createCountUsageEntry(response.getResponse().count()));
        for (DatafeedState datafeedState : datafeedCountByState.keySet()) {
            datafeedsUsage.put(
                datafeedState.name().toLowerCase(Locale.ROOT),
                createCountUsageEntry(datafeedCountByState.get(datafeedState).get())
            );
        }
    }

    private Map<String, Object> createCountUsageEntry(long count) {
        Map<String, Object> usage = new HashMap<>();
        usage.put(MachineLearningFeatureSetUsage.COUNT, count);
        return usage;
    }

    private void addDataFrameAnalyticsStatsUsage(
        GetDataFrameAnalyticsStatsAction.Response response,
        Map<String, Object> dataframeAnalyticsUsage
    ) {
        Map<DataFrameAnalyticsState, Counter> dataFrameAnalyticsStateCounterMap = new HashMap<>();

        StatsAccumulator memoryUsagePeakBytesStats = new StatsAccumulator();
        for (GetDataFrameAnalyticsStatsAction.Response.Stats stats : response.getResponse().results()) {
            dataFrameAnalyticsStateCounterMap.computeIfAbsent(stats.getState(), ds -> Counter.newCounter()).addAndGet(1);
            MemoryUsage memoryUsage = stats.getMemoryUsage();
            if (memoryUsage != null && memoryUsage.getPeakUsageBytes() > 0) {
                memoryUsagePeakBytesStats.add(memoryUsage.getPeakUsageBytes());
            }
        }
        dataframeAnalyticsUsage.put(
            "memory_usage",
            Collections.singletonMap(MemoryUsage.PEAK_USAGE_BYTES.getPreferredName(), memoryUsagePeakBytesStats.asMap())
        );

        dataframeAnalyticsUsage.put(MachineLearningFeatureSetUsage.ALL, createCountUsageEntry(response.getResponse().count()));
        for (DataFrameAnalyticsState state : dataFrameAnalyticsStateCounterMap.keySet()) {
            dataframeAnalyticsUsage.put(
                state.name().toLowerCase(Locale.ROOT),
                createCountUsageEntry(dataFrameAnalyticsStateCounterMap.get(state).get())
            );
        }
    }

    private void addDataFrameAnalyticsUsage(GetDataFrameAnalyticsAction.Response response, Map<String, Object> dataframeAnalyticsUsage) {
        Map<String, Integer> perAnalysisTypeCounterMap = new HashMap<>();

        for (DataFrameAnalyticsConfig config : response.getResources().results()) {
            int count = perAnalysisTypeCounterMap.computeIfAbsent(config.getAnalysis().getWriteableName(), k -> 0);
            perAnalysisTypeCounterMap.put(config.getAnalysis().getWriteableName(), ++count);
        }
        dataframeAnalyticsUsage.put("analysis_counts", perAnalysisTypeCounterMap);
    }

    private void addInferenceUsage(ActionListener<Map<String, Object>> listener) {
        GetTrainedModelsAction.Request getModelsRequest = new GetTrainedModelsAction.Request(
            "*",
            Collections.emptyList(),
            Collections.emptySet()
        );
        getModelsRequest.setPageParams(new PageParams(0, 10_000));
        client.execute(GetTrainedModelsAction.INSTANCE, getModelsRequest, ActionListener.wrap(getModelsResponse -> {
            GetTrainedModelsStatsAction.Request getStatsRequest = new GetTrainedModelsStatsAction.Request("*");
            getStatsRequest.setPageParams(new PageParams(0, 10_000));
            client.execute(GetTrainedModelsStatsAction.INSTANCE, getStatsRequest, ActionListener.wrap(getStatsResponse -> {
                Map<String, Object> inferenceUsage = new LinkedHashMap<>();
                addInferenceIngestUsage(getStatsResponse, inferenceUsage);
                addTrainedModelStats(getModelsResponse, getStatsResponse, inferenceUsage);
                addDeploymentStats(getStatsResponse, inferenceUsage);
                listener.onResponse(inferenceUsage);
            }, listener::onFailure));
        }, listener::onFailure));
    }

    private void addDeploymentStats(GetTrainedModelsStatsAction.Response statsResponse, Map<String, Object> inferenceUsage) {
        StatsAccumulator modelSizes = new StatsAccumulator();
        int deploymentsCount = 0;
        double avgTimeSum = 0.0;
        StatsAccumulator nodeDistribution = new StatsAccumulator();
        for (var stats : statsResponse.getResources().results()) {
            AllocationStats deploymentStats = stats.getDeploymentStats();
            if (deploymentStats == null) {
                continue;
            }
            deploymentsCount++;
            TrainedModelSizeStats modelSizeStats = stats.getModelSizeStats();
            if (modelSizeStats != null) {
                modelSizes.add(modelSizeStats.getModelSizeBytes());
            }
            for (var nodeStats : deploymentStats.getNodeStats()) {
                long nodeInferenceCount = nodeStats.getInferenceCount().orElse(0L);
                avgTimeSum += nodeStats.getAvgInferenceTime().orElse(0.0) * nodeInferenceCount;
                nodeDistribution.add(nodeInferenceCount);
            }
        }

        inferenceUsage.put(
            "deployments",
            Map.of(
                "count",
                deploymentsCount,
                "time_ms",
                Map.of(StatsAccumulator.Fields.AVG, nodeDistribution.getTotal() == 0.0 ? 0.0 : avgTimeSum / nodeDistribution.getTotal()),
                "model_sizes_bytes",
                modelSizes.asMap(),
                "inference_counts",
                nodeDistribution.asMap()
            )
        );
    }

    private void addTrainedModelStats(
        GetTrainedModelsAction.Response modelsResponse,
        GetTrainedModelsStatsAction.Response statsResponse,
        Map<String, Object> inferenceUsage
    ) {
        List<TrainedModelConfig> trainedModelConfigs = modelsResponse.getResources().results();
        Map<String, GetTrainedModelsStatsAction.Response.TrainedModelStats> statsToModelId = statsResponse.getResources()
            .results()
            .stream()
            .collect(Collectors.toMap(GetTrainedModelsStatsAction.Response.TrainedModelStats::getModelId, Function.identity()));
        Map<String, Object> trainedModelsUsage = new HashMap<>();
        trainedModelsUsage.put(MachineLearningFeatureSetUsage.ALL, createCountUsageEntry(trainedModelConfigs.size()));

        StatsAccumulator estimatedOperations = new StatsAccumulator();
        StatsAccumulator estimatedMemoryUsageBytes = new StatsAccumulator();
        int createdByAnalyticsCount = 0;
        Map<String, Counter> inferenceConfigCounts = new LinkedHashMap<>();
        int prepackagedCount = 0;
        for (TrainedModelConfig trainedModelConfig : trainedModelConfigs) {
            if (trainedModelConfig.getTags().contains("prepackaged")) {
                prepackagedCount++;
                continue;
            }
            InferenceConfig inferenceConfig = trainedModelConfig.getInferenceConfig();
            if (inferenceConfig != null) {
                inferenceConfigCounts.computeIfAbsent(inferenceConfig.getName(), s -> Counter.newCounter()).addAndGet(1);
            }
            if (trainedModelConfig.getMetadata() != null && trainedModelConfig.getMetadata().containsKey("analytics_config")) {
                createdByAnalyticsCount++;
            }
            estimatedOperations.add(trainedModelConfig.getEstimatedOperations());
            if (statsToModelId.containsKey(trainedModelConfig.getModelId())) {
                estimatedMemoryUsageBytes.add(statsToModelId.get(trainedModelConfig.getModelId()).getModelSizeStats().getModelSizeBytes());
            }
        }

        Map<String, Object> counts = new HashMap<>();
        counts.put("total", trainedModelConfigs.size());
        inferenceConfigCounts.forEach((configName, count) -> counts.put(configName, count.get()));
        counts.put("prepackaged", prepackagedCount);
        counts.put("other", trainedModelConfigs.size() - createdByAnalyticsCount - prepackagedCount);

        trainedModelsUsage.put("count", counts);
        trainedModelsUsage.put(TrainedModelConfig.ESTIMATED_OPERATIONS.getPreferredName(), estimatedOperations.asMap());
        trainedModelsUsage.put(TrainedModelConfig.MODEL_SIZE_BYTES.getPreferredName(), estimatedMemoryUsageBytes.asMap());

        inferenceUsage.put("trained_models", trainedModelsUsage);
    }

    // TODO separate out ours and users models possibly regression vs classification
    private void addInferenceIngestUsage(GetTrainedModelsStatsAction.Response statsResponse, Map<String, Object> inferenceUsage) {
        int pipelineCount = 0;
        StatsAccumulator docCountStats = new StatsAccumulator();
        StatsAccumulator timeStats = new StatsAccumulator();
        StatsAccumulator failureStats = new StatsAccumulator();

        for (GetTrainedModelsStatsAction.Response.TrainedModelStats modelStats : statsResponse.getResources().results()) {
            pipelineCount += modelStats.getPipelineCount();
            modelStats.getIngestStats().getProcessorStats().values().stream().flatMap(List::stream).forEach(processorStat -> {
                if (processorStat.getName().equals(InferenceProcessor.TYPE)) {
                    docCountStats.add(processorStat.getStats().getIngestCount());
                    timeStats.add(processorStat.getStats().getIngestTimeInMillis());
                    failureStats.add(processorStat.getStats().getIngestFailedCount());
                }
            });
        }

        Map<String, Object> ingestUsage = Maps.newMapWithExpectedSize(6);
        ingestUsage.put("pipelines", createCountUsageEntry(pipelineCount));
        ingestUsage.put("num_docs_processed", getMinMaxSumAsLongsFromStats(docCountStats));
        ingestUsage.put("time_ms", getMinMaxSumAsLongsFromStats(timeStats));
        ingestUsage.put("num_failures", getMinMaxSumAsLongsFromStats(failureStats));
        inferenceUsage.put("ingest_processors", Collections.singletonMap(MachineLearningFeatureSetUsage.ALL, ingestUsage));
    }

    private Map<String, Object> getMinMaxSumAsLongsFromStats(StatsAccumulator stats) {
        Map<String, Object> asMap = Maps.newMapWithExpectedSize(3);
        asMap.put("sum", Double.valueOf(stats.getTotal()).longValue());
        asMap.put("min", Double.valueOf(stats.getMin()).longValue());
        asMap.put("max", Double.valueOf(stats.getMax()).longValue());
        return asMap;
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
