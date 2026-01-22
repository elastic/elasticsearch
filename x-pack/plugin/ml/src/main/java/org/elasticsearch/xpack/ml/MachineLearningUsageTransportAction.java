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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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
import org.elasticsearch.xpack.core.ml.action.MlMemoryAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TrainedModelSizeStats;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;

import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;

public class MachineLearningUsageTransportAction extends XPackUsageFeatureTransportAction {

    private static class DeploymentStats {

        private final String modelId;
        private final String taskType;
        private final StatsAccumulator inferenceCounts = new StatsAccumulator();
        private Instant lastAccess;
        private final int numThreads;
        private final int numAllocations;

        DeploymentStats(String modelId, String taskType, int numThreads, int numAllocations) {
            this.modelId = modelId;
            this.taskType = taskType;
            this.numThreads = numThreads;
            this.numAllocations = numAllocations;
        }

        void update(AssignmentStats.NodeStats stats) {
            inferenceCounts.add(stats.getInferenceCount().orElse(0L));
            if (stats.getLastAccess() != null && (lastAccess == null || stats.getLastAccess().isAfter(lastAccess))) {
                lastAccess = stats.getLastAccess();
            }
        }

        Map<String, Object> asMap() {
            Map<String, Object> result = new HashMap<>();
            result.put("model_id", modelId);
            result.put("task_type", taskType);
            result.put("num_allocations", numAllocations);
            result.put("num_threads", numThreads);
            result.put("inference_counts", inferenceCounts.asMap());
            if (lastAccess != null) {
                result.put("last_access", lastAccess.toString());
            }
            return result;
        }
    }

    private static final Logger logger = LogManager.getLogger(MachineLearningUsageTransportAction.class);

    private final Client client;
    private final XPackLicenseState licenseState;
    private final JobManagerHolder jobManagerHolder;
    private final MachineLearningExtension machineLearningExtension;
    private final boolean enabled;

    @Inject
    public MachineLearningUsageTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        Environment environment,
        Client client,
        XPackLicenseState licenseState,
        JobManagerHolder jobManagerHolder,
        MachineLearningExtensionHolder machineLearningExtensionHolder
    ) {
        super(XPackUsageFeatureAction.MACHINE_LEARNING.name(), transportService, clusterService, threadPool, actionFilters);
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.licenseState = licenseState;
        this.jobManagerHolder = jobManagerHolder;
        if (machineLearningExtensionHolder.isEmpty()) {
            this.machineLearningExtension = new DefaultMachineLearningExtension();
        } else {
            this.machineLearningExtension = machineLearningExtensionHolder.getMachineLearningExtension();
        }
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(environment.settings());
    }

    @Override
    protected void localClusterStateOperation(
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
                Collections.emptyMap(),
                0
            );
            listener.onResponse(new XPackUsageFeatureResponse(usage));
            return;
        }

        Map<String, Object> jobsUsage = new LinkedHashMap<>();
        Map<String, Object> datafeedsUsage = new LinkedHashMap<>();
        Map<String, Object> analyticsUsage = new LinkedHashMap<>();
        AtomicReference<Map<String, Object>> inferenceUsage = new AtomicReference<>(Map.of());

        int nodeCount = mlNodeCount(state);

        // Step 6. return final ML usage
        ActionListener<MlMemoryAction.Response> memoryUsageListener = ActionListener.wrap(memoryResponse -> {
            var memoryUsage = extractMemoryUsage(memoryResponse);
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new MachineLearningFeatureSetUsage(
                        MachineLearningField.ML_API_FEATURE.checkWithoutTracking(licenseState),
                        enabled,
                        jobsUsage,
                        datafeedsUsage,
                        analyticsUsage,
                        inferenceUsage.get(),
                        memoryUsage,
                        nodeCount
                    )
                )
            );
        }, e -> {
            logger.warn("Failed to get memory usage to include in ML usage", e);
            listener.onResponse(
                new XPackUsageFeatureResponse(
                    new MachineLearningFeatureSetUsage(
                        MachineLearningField.ML_API_FEATURE.checkWithoutTracking(licenseState),
                        enabled,
                        jobsUsage,
                        datafeedsUsage,
                        analyticsUsage,
                        inferenceUsage.get(),
                        Collections.emptyMap(),
                        nodeCount
                    )
                )
            );
        });

        // Step 5. Get
        ActionListener<Map<String, Object>> inferenceUsageListener = ActionListener.wrap(inference -> {
            inferenceUsage.set(inference);
            client.execute(MlMemoryAction.INSTANCE, new MlMemoryAction.Request("_all"), memoryUsageListener);
        }, e -> {
            logger.warn("Failed to get inference usage to include in ML usage", e);
            client.execute(MlMemoryAction.INSTANCE, new MlMemoryAction.Request("_all"), memoryUsageListener);
        });

        // Step 4. Extract usage from data frame analytics configs and then get inference usage
        ActionListener<GetDataFrameAnalyticsAction.Response> dataframeAnalyticsListener = ActionListener.wrap(response -> {
            addDataFrameAnalyticsUsage(response, analyticsUsage);
            addInferenceUsage(inferenceUsageListener);
        }, e -> {
            logger.warn("Failed to get data frame analytics configs to include in ML usage", e);
            addInferenceUsage(inferenceUsageListener);
        });

        // Step 3. Extract usage from data frame analytics stats and then request data frame analytics configs
        GetDataFrameAnalyticsAction.Request getDfaRequest = new GetDataFrameAnalyticsAction.Request(Metadata.ALL);
        getDfaRequest.setPageParams(new PageParams(0, 10_000));
        ActionListener<GetDataFrameAnalyticsStatsAction.Response> dataframeAnalyticsStatsListener = ActionListener.wrap(response -> {
            addDataFrameAnalyticsStatsUsage(response, analyticsUsage);
            client.execute(GetDataFrameAnalyticsAction.INSTANCE, getDfaRequest, dataframeAnalyticsListener);
        }, e -> {
            logger.warn("Failed to get data frame analytics stats to include in ML usage", e);
            client.execute(GetDataFrameAnalyticsAction.INSTANCE, getDfaRequest, dataframeAnalyticsListener);
        });

        // Step 2. Extract usage from datafeeds stats and then request stats for data frame analytics
        GetDataFrameAnalyticsStatsAction.Request dataframeAnalyticsStatsRequest = new GetDataFrameAnalyticsStatsAction.Request(
            Metadata.ALL
        );
        dataframeAnalyticsStatsRequest.setPageParams(new PageParams(0, 10_000));
        ActionListener<GetDatafeedsStatsAction.Response> datafeedStatsListener = ActionListener.wrap(response -> {
            addDatafeedsUsage(response, datafeedsUsage);
            if (machineLearningExtension.isDataFrameAnalyticsEnabled()) {
                client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, dataframeAnalyticsStatsRequest, dataframeAnalyticsStatsListener);
            } else {
                addInferenceUsage(inferenceUsageListener);
            }
        }, e -> {
            logger.warn("Failed to get datafeed stats to include in ML usage", e);
            if (machineLearningExtension.isDataFrameAnalyticsEnabled()) {
                client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, dataframeAnalyticsStatsRequest, dataframeAnalyticsStatsListener);
            } else {
                addInferenceUsage(inferenceUsageListener);
            }
        });

        // Step 1. Extract usage from jobs stats and then request stats for all datafeeds
        GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(Metadata.ALL);
        ActionListener<GetJobsStatsAction.Response> jobStatsListener = ActionListener.wrap(
            response -> jobManagerHolder.getJobManager().expandJobs(Metadata.ALL, true, ActionListener.wrap(jobs -> {
                addJobsUsage(response, jobs.results(), jobsUsage);
                client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
            }, e -> {
                logger.warn("Failed to get job configs to include in ML usage", e);
                client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
            })),
            e -> {
                logger.warn("Failed to get job stats to include in ML usage", e);
                client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
            }
        );

        // Step 0. Kick off the chain of callbacks by requesting jobs stats
        if (machineLearningExtension.isAnomalyDetectionEnabled()) {
            GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(Metadata.ALL);
            client.execute(GetJobsStatsAction.INSTANCE, jobStatsRequest, jobStatsListener);
        } else if (machineLearningExtension.isDataFrameAnalyticsEnabled()) {
            client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, dataframeAnalyticsStatsRequest, dataframeAnalyticsStatsListener);
        } else {
            addInferenceUsage(inferenceUsageListener);
        }
    }

    private void addJobsUsage(GetJobsStatsAction.Response response, List<Job> jobs, Map<String, Object> jobsUsage) {
        StatsAccumulator allJobsDetectorsStats = new StatsAccumulator();
        StatsAccumulator allJobsModelSizeStats = new StatsAccumulator();
        ForecastStats allJobsForecastStats = new ForecastStats();

        Map<JobState, Counter> jobCountByState = new EnumMap<>(JobState.class);
        Map<JobState, StatsAccumulator> detectorStatsByState = new EnumMap<>(JobState.class);
        Map<JobState, StatsAccumulator> modelSizeStatsByState = new EnumMap<>(JobState.class);
        Map<JobState, ForecastStats> forecastStatsByState = new EnumMap<>(JobState.class);
        Map<JobState, Map<String, Long>> createdByByState = new EnumMap<>(JobState.class);

        List<GetJobsStatsAction.Response.JobStats> jobsStats = response.getResponse().results();
        Map<String, Job> jobMap = jobs.stream().collect(Collectors.toMap(Job::getId, item -> item));
        Map<String, Long> allJobsCreatedBy = jobs.stream()
            .map(MachineLearningUsageTransportAction::jobCreatedBy)
            .collect(Collectors.groupingBy(item -> item, Collectors.counting()));

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

    private static String jobCreatedBy(Job job) {
        Map<String, Object> customSettings = job.getCustomSettings();
        if (customSettings == null || customSettings.containsKey(MachineLearningFeatureSetUsage.CREATED_BY) == false) {
            return "unknown";
        }
        // Replace non-alpha-numeric characters with underscores because
        // the values from custom settings become keys in the usage data
        return customSettings.get(MachineLearningFeatureSetUsage.CREATED_BY).toString().replaceAll("\\W", "_");
    }

    private static Map<String, Object> createJobUsageEntry(
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

    private static void addDatafeedsUsage(GetDatafeedsStatsAction.Response response, Map<String, Object> datafeedsUsage) {
        Map<DatafeedState, Counter> datafeedCountByState = new EnumMap<>(DatafeedState.class);

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

    private static Map<String, Object> createCountUsageEntry(long count) {
        Map<String, Object> usage = new HashMap<>();
        usage.put(MachineLearningFeatureSetUsage.COUNT, count);
        return usage;
    }

    private static void addDataFrameAnalyticsStatsUsage(
        GetDataFrameAnalyticsStatsAction.Response response,
        Map<String, Object> dataframeAnalyticsUsage
    ) {
        Map<DataFrameAnalyticsState, Counter> dataFrameAnalyticsStateCounterMap = new EnumMap<>(DataFrameAnalyticsState.class);

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

    private static void addDataFrameAnalyticsUsage(
        GetDataFrameAnalyticsAction.Response response,
        Map<String, Object> dataframeAnalyticsUsage
    ) {
        Map<String, Integer> perAnalysisTypeCounterMap = new HashMap<>();

        for (DataFrameAnalyticsConfig config : response.getResources().results()) {
            int count = perAnalysisTypeCounterMap.computeIfAbsent(config.getAnalysis().getWriteableName(), k -> 0);
            perAnalysisTypeCounterMap.put(config.getAnalysis().getWriteableName(), ++count);
        }
        dataframeAnalyticsUsage.put("analysis_counts", perAnalysisTypeCounterMap);
    }

    private void addInferenceUsage(ActionListener<Map<String, Object>> listener) {
        if (machineLearningExtension.isDataFrameAnalyticsEnabled() || machineLearningExtension.isNlpEnabled()) {
            GetTrainedModelsAction.Request getModelsRequest = new GetTrainedModelsAction.Request(
                "*",
                Collections.emptyList(),
                Collections.emptySet()
            );
            getModelsRequest.setPageParams(new PageParams(0, 10_000));
            client.execute(
                GetTrainedModelsAction.INSTANCE,
                getModelsRequest,
                listener.delegateFailureAndWrap((delegate, getModelsResponse) -> {
                    GetTrainedModelsStatsAction.Request getStatsRequest = new GetTrainedModelsStatsAction.Request("*");
                    getStatsRequest.setPageParams(new PageParams(0, 10_000));
                    client.execute(
                        GetTrainedModelsStatsAction.INSTANCE,
                        getStatsRequest,
                        delegate.delegateFailureAndWrap((l, getStatsResponse) -> {
                            Map<String, Object> inferenceUsage = new LinkedHashMap<>();
                            addInferenceIngestUsage(getStatsResponse, inferenceUsage);
                            addTrainedModelStats(getModelsResponse, getStatsResponse, inferenceUsage);
                            addDeploymentStats(getModelsResponse, getStatsResponse, inferenceUsage);
                            l.onResponse(inferenceUsage);
                        })
                    );
                })
            );
        } else {
            listener.onResponse(Map.of());
        }
    }

    private static void addDeploymentStats(
        GetTrainedModelsAction.Response modelsResponse,
        GetTrainedModelsStatsAction.Response statsResponse,
        Map<String, Object> inferenceUsage
    ) {
        Map<String, String> taskTypes = modelsResponse.getResources()
            .results()
            .stream()
            .collect(Collectors.toMap(TrainedModelConfig::getModelId, cfg -> cfg.getInferenceConfig().getName()));
        StatsAccumulator modelSizes = new StatsAccumulator();
        int deploymentsCount = 0;
        double avgTimeSum = 0.0;
        StatsAccumulator nodeDistribution = new StatsAccumulator();
        Map<String, DeploymentStats> statsByModel = new TreeMap<>();
        for (var stats : statsResponse.getResources().results()) {
            AssignmentStats deploymentStats = stats.getDeploymentStats();
            if (deploymentStats == null) {
                continue;
            }
            deploymentsCount++;
            TrainedModelSizeStats modelSizeStats = stats.getModelSizeStats();
            if (modelSizeStats != null) {
                modelSizes.add(modelSizeStats.getModelSizeBytes());
            }
            String modelId = deploymentStats.getModelId();
            String taskType = taskTypes.get(deploymentStats.getModelId());
            String mapKey = modelId + ":" + taskType;
            DeploymentStats modelStats = statsByModel.computeIfAbsent(
                mapKey,
                key -> new DeploymentStats(
                    modelId,
                    taskType,
                    deploymentStats.getThreadsPerAllocation(),
                    deploymentStats.getNumberOfAllocations()
                )
            );
            for (var nodeStats : deploymentStats.getNodeStats()) {
                long nodeInferenceCount = nodeStats.getInferenceCount().orElse(0L);
                avgTimeSum += nodeStats.getAvgInferenceTime().orElse(0.0) * nodeInferenceCount;
                nodeDistribution.add(nodeInferenceCount);
                modelStats.update(nodeStats);
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
                nodeDistribution.asMap(),
                "stats_by_model",
                statsByModel.values().stream().map(DeploymentStats::asMap).collect(Collectors.toList())
            )
        );
    }

    // Default for testing
    static void addTrainedModelStats(
        GetTrainedModelsAction.Response modelsResponse,
        GetTrainedModelsStatsAction.Response statsResponse,
        Map<String, Object> inferenceUsage
    ) {
        List<TrainedModelConfig> trainedModelConfigs = modelsResponse.getResources().results();
        Map<String, GetTrainedModelsStatsAction.Response.TrainedModelStats> statsToModelId = statsResponse.getResources()
            .results()
            .stream()
            .filter(Objects::nonNull)
            .collect(
                Collectors.toMap(
                    GetTrainedModelsStatsAction.Response.TrainedModelStats::getModelId,
                    Function.identity(),
                    // Addresses issue: https://github.com/elastic/elasticsearch/issues/108423
                    // There could be multiple deployments of the same model which would result in a collision, since all we need is the
                    // memory used by the model we can use either one
                    (stats1, stats2) -> stats1
                )
            );
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
    @FixForMultiProject // do not use default project
    private static void addInferenceIngestUsage(GetTrainedModelsStatsAction.Response statsResponse, Map<String, Object> inferenceUsage) {
        int pipelineCount = 0;
        StatsAccumulator docCountStats = new StatsAccumulator();
        StatsAccumulator timeStats = new StatsAccumulator();
        StatsAccumulator failureStats = new StatsAccumulator();

        for (GetTrainedModelsStatsAction.Response.TrainedModelStats modelStats : statsResponse.getResources().results()) {
            pipelineCount += modelStats.getPipelineCount();
            modelStats.getIngestStats()
                .processorStats()
                .getOrDefault(ProjectId.DEFAULT, Map.of())
                .values()
                .stream()
                .flatMap(List::stream)
                .forEach(processorStat -> {
                    if (processorStat.name().equals(InferenceProcessor.TYPE)) {
                        docCountStats.add(processorStat.stats().ingestCount());
                        timeStats.add(processorStat.stats().ingestTimeInMillis());
                        failureStats.add(processorStat.stats().ingestFailedCount());
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

    private static Map<String, Object> extractMemoryUsage(MlMemoryAction.Response memoryResponse) {
        var adMem = memoryResponse.getNodes().stream().mapToLong(mem -> mem.getMlAnomalyDetectors().getBytes()).sum();
        var dfaMem = memoryResponse.getNodes().stream().mapToLong(mem -> mem.getMlDataFrameAnalytics().getBytes()).sum();
        var pytorchMem = memoryResponse.getNodes().stream().mapToLong(mem -> mem.getMlNativeInference().getBytes()).sum();
        var nativeOverheadMem = memoryResponse.getNodes().stream().mapToLong(mem -> mem.getMlNativeCodeOverhead().getBytes()).sum();
        long totalUsedMem = adMem + dfaMem + pytorchMem + nativeOverheadMem;

        var memoryUsage = new LinkedHashMap<String, Object>();
        memoryUsage.put("anomaly_detectors_memory_bytes", adMem);
        memoryUsage.put("data_frame_analytics_memory_bytes", dfaMem);
        memoryUsage.put("pytorch_inference_memory_bytes", pytorchMem);
        memoryUsage.put("total_used_memory_bytes", totalUsedMem);
        return memoryUsage;
    }

    private static Map<String, Object> getMinMaxSumAsLongsFromStats(StatsAccumulator stats) {
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
