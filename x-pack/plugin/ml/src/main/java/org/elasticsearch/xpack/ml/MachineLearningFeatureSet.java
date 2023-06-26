/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.NativeControllerHolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class MachineLearningFeatureSet implements XPackFeatureSet {

    private static final Logger logger = LogManager.getLogger(MachineLearningFeatureSet.class);

    /**
     * List of platforms for which the native processes are available
     */
    private static final List<String> mlPlatforms = Collections.unmodifiableList(
        Arrays.asList("darwin-aarch64", "darwin-x86_64", "linux-aarch64", "linux-x86_64", "windows-x86_64")
    );

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;
    private final Client client;
    private final JobManagerHolder jobManagerHolder;
    private final Map<String, Object> nativeCodeInfo;

    @Inject
    public MachineLearningFeatureSet(
        Environment environment,
        ClusterService clusterService,
        Client client,
        @Nullable XPackLicenseState licenseState,
        JobManagerHolder jobManagerHolder
    ) {
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(environment.settings());
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.licenseState = licenseState;
        this.jobManagerHolder = jobManagerHolder;
        Map<String, Object> nativeCodeInfo = NativeController.UNKNOWN_NATIVE_CODE_INFO;
        // Don't try to get the native code version if ML is disabled - it causes too much controversy
        // if ML has been disabled because of some OS incompatibility. Also don't try to get the native
        // code version in the transport client - the controller process won't be running.
        if (enabled && XPackPlugin.transportClientMode(environment.settings()) == false) {
            try {
                if (isRunningOnMlPlatform(true)) {
                    NativeController nativeController = NativeControllerHolder.getNativeController(
                        clusterService.getNodeName(),
                        environment
                    );
                    if (nativeController != null) {
                        nativeCodeInfo = nativeController.getNativeCodeInfo();
                    }
                }
            } catch (IOException | TimeoutException e) {
                LogManager.getLogger(MachineLearningFeatureSet.class).error("Cannot get native code info for Machine Learning", e);
                throw new ElasticsearchException("Cannot communicate with Machine Learning native code");
            }
        }
        this.nativeCodeInfo = nativeCodeInfo;
    }

    static boolean isRunningOnMlPlatform(boolean fatalIfNot) {
        return isRunningOnMlPlatform(Constants.OS_NAME, Constants.OS_ARCH, fatalIfNot);
    }

    static boolean isRunningOnMlPlatform(String osName, String osArch, boolean fatalIfNot) {
        String platformName = Platforms.platformName(osName, osArch);
        if (mlPlatforms.contains(platformName)) {
            return true;
        }
        if (fatalIfNot) {
            throw new ElasticsearchException(
                "X-Pack is not supported and Machine Learning is not available for ["
                    + platformName
                    + "]; you can use the other X-Pack features (unsupported) by setting xpack.ml.enabled: false in elasticsearch.yml"
            );
        }
        return false;
    }

    @Override
    public String name() {
        return XPackField.MACHINE_LEARNING;
    }

    @Override
    public boolean available() {
        return licenseState != null && MachineLearningField.ML_API_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return nativeCodeInfo;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        ClusterState state = clusterService.state();
        new Retriever(client, jobManagerHolder, available(), enabled(), state).execute(listener);
    }

    private static int mlNodeCount(boolean enabled, final ClusterState clusterState) {
        if (enabled == false) {
            return 0;
        }

        int mlNodeCount = 0;
        for (DiscoveryNode node : clusterState.getNodes()) {
            if (MachineLearning.isMlNode(node)) {
                ++mlNodeCount;
            }
        }
        return mlNodeCount;
    }

    public static class Retriever {

        private final Client client;
        private final JobManagerHolder jobManagerHolder;
        private final boolean available;
        private final boolean enabled;
        private Map<String, Object> jobsUsage;
        private Map<String, Object> datafeedsUsage;
        private Map<String, Object> analyticsUsage;
        private Map<String, Object> inferenceUsage;
        private final ClusterState state;
        private int nodeCount;

        public Retriever(Client client, JobManagerHolder jobManagerHolder, boolean available, boolean enabled, ClusterState state) {
            this.client = Objects.requireNonNull(client);
            this.jobManagerHolder = jobManagerHolder;
            this.available = available;
            this.enabled = enabled;
            this.jobsUsage = new LinkedHashMap<>();
            this.datafeedsUsage = new LinkedHashMap<>();
            this.analyticsUsage = new LinkedHashMap<>();
            this.inferenceUsage = new LinkedHashMap<>();
            this.nodeCount = mlNodeCount(enabled, state);
            this.state = state;
        }

        private static void initializeStats(Map<String, Long> emptyStatsMap) {
            emptyStatsMap.put("sum", 0L);
            emptyStatsMap.put("min", 0L);
            emptyStatsMap.put("max", 0L);
        }

        private void addJobsUsage(GetJobsStatsAction.Response response, List<Job> jobs) {
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
                forecastStatsByState.merge(jobState, jobStats.getForecastStats(), (f1, f2) -> f1.merge(f2));
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

        private void addDatafeedsUsage(GetDatafeedsStatsAction.Response response) {
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

        private static void updateStats(Map<String, Long> statsMap, Long value) {
            statsMap.compute("sum", (k, v) -> v + value);
            statsMap.compute("min", (k, v) -> Math.min(v, value));
            statsMap.compute("max", (k, v) -> Math.max(v, value));
        }

        private static String[] ingestNodes(final ClusterState clusterState) {
            String[] ingestNodes = new String[clusterState.nodes().getIngestNodes().size()];
            Iterator<String> nodeIterator = clusterState.nodes().getIngestNodes().keysIt();
            int i = 0;
            while (nodeIterator.hasNext()) {
                ingestNodes[i++] = nodeIterator.next();
            }
            return ingestNodes;
        }

        public void execute(ActionListener<Usage> listener) {
            // empty holder means either ML disabled or transport client mode
            if (jobManagerHolder.isEmpty()) {
                listener.onResponse(
                    new MachineLearningFeatureSetUsage(
                        available,
                        enabled,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        0
                    )
                );
                return;
            }

            // Step 6. extract trained model config count and then return results
            ActionListener<GetTrainedModelsAction.Response> trainedModelsListener = ActionListener.wrap(response -> {
                addTrainedModelStats(response, inferenceUsage);
                MachineLearningFeatureSetUsage usage = new MachineLearningFeatureSetUsage(
                    available,
                    enabled,
                    jobsUsage,
                    datafeedsUsage,
                    analyticsUsage,
                    inferenceUsage,
                    nodeCount
                );
                listener.onResponse(usage);
            }, e -> {
                logger.warn("Failed to get trained models usage to include in ML usage", e);
                MachineLearningFeatureSetUsage usage = new MachineLearningFeatureSetUsage(
                    available,
                    enabled,
                    jobsUsage,
                    datafeedsUsage,
                    analyticsUsage,
                    inferenceUsage,
                    nodeCount
                );
                listener.onResponse(usage);
            });

            // Step 5. Extract usage from ingest statistics and gather trained model config count
            GetTrainedModelsAction.Request getModelsRequest = new GetTrainedModelsAction.Request(
                "*",
                Collections.emptyList(),
                Collections.emptySet()
            );
            getModelsRequest.setPageParams(new PageParams(0, 10_000));
            ActionListener<NodesStatsResponse> nodesStatsListener = ActionListener.wrap(response -> {
                addInferenceIngestUsage(response, inferenceUsage);
                client.execute(GetTrainedModelsAction.INSTANCE, getModelsRequest, trainedModelsListener);
            }, e -> {
                logger.warn("Failed to get inference ingest usage to include in ML usage", e);
                client.execute(GetTrainedModelsAction.INSTANCE, getModelsRequest, trainedModelsListener);
            });

            // Step 4. Extract usage from data frame analytics configs and then request ingest node stats
            String[] ingestNodes = ingestNodes(state);
            NodesStatsRequest nodesStatsRequest = new NodesStatsRequest(ingestNodes).clear()
                .addMetric(NodesStatsRequest.Metric.INGEST.metricName());
            ActionListener<GetDataFrameAnalyticsAction.Response> dataframeAnalyticsListener = ActionListener.wrap(response -> {
                addDataFrameAnalyticsUsage(response, analyticsUsage);
                client.execute(NodesStatsAction.INSTANCE, nodesStatsRequest, nodesStatsListener);
            }, e -> {
                logger.warn("Failed to get data frame analytics configs to include in ML usage", e);
                client.execute(NodesStatsAction.INSTANCE, nodesStatsRequest, nodesStatsListener);
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

            // Step 2. Extract usage from datafeeds stats and return usage response
            GetDataFrameAnalyticsStatsAction.Request dataframeAnalyticsStatsRequest = new GetDataFrameAnalyticsStatsAction.Request(
                GetDatafeedsStatsAction.ALL
            );
            dataframeAnalyticsStatsRequest.setPageParams(new PageParams(0, 10_000));
            ActionListener<GetDatafeedsStatsAction.Response> datafeedStatsListener = ActionListener.wrap(response -> {
                addDatafeedsUsage(response);
                client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, dataframeAnalyticsStatsRequest, dataframeAnalyticsStatsListener);
            }, e -> {
                logger.warn("Failed to get datafeed stats to include in ML usage", e);
                client.execute(GetDataFrameAnalyticsStatsAction.INSTANCE, dataframeAnalyticsStatsRequest, dataframeAnalyticsStatsListener);
            });

            // Step 1. Extract usage from jobs stats and then request stats for all datafeeds
            GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(GetDatafeedsStatsAction.ALL);
            ActionListener<GetJobsStatsAction.Response> jobStatsListener = ActionListener.wrap(response -> {
                jobManagerHolder.getJobManager().expandJobs(Metadata.ALL, true, ActionListener.wrap(jobs -> {
                    addJobsUsage(response, jobs.results());
                    client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
                }, e -> {
                    logger.warn("Failed to get job configs to include in ML usage", e);
                    client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
                }));
            }, e -> {
                logger.warn("Failed to get job stats to include in ML usage", e);
                client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
            });

            // Step 0. Kick off the chain of callbacks by requesting jobs stats
            GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(Metadata.ALL);
            client.execute(GetJobsStatsAction.INSTANCE, jobStatsRequest, jobStatsListener);
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

        private void addDataFrameAnalyticsUsage(
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

        // TODO separate out ours and users models possibly regression vs classification
        private void addTrainedModelStats(GetTrainedModelsAction.Response response, Map<String, Object> inferenceUsage) {
            List<TrainedModelConfig> trainedModelConfigs = response.getResources().results();
            Map<String, Object> trainedModelsUsage = new HashMap<>();
            trainedModelsUsage.put(MachineLearningFeatureSetUsage.ALL, createCountUsageEntry(trainedModelConfigs.size()));

            StatsAccumulator estimatedOperations = new StatsAccumulator();
            StatsAccumulator estimatedMemoryUsageBytes = new StatsAccumulator();
            int createdByAnalyticsCount = 0;
            int regressionCount = 0;
            int classificationCount = 0;
            int prepackagedCount = 0;
            for (TrainedModelConfig trainedModelConfig : trainedModelConfigs) {
                if (trainedModelConfig.getTags().contains("prepackaged")) {
                    prepackagedCount++;
                    continue;
                }
                InferenceConfig inferenceConfig = trainedModelConfig.getInferenceConfig();
                if (inferenceConfig instanceof RegressionConfig) {
                    regressionCount++;
                } else if (inferenceConfig instanceof ClassificationConfig) {
                    classificationCount++;
                }
                if (trainedModelConfig.getMetadata() != null && trainedModelConfig.getMetadata().containsKey("analytics_config")) {
                    createdByAnalyticsCount++;
                }
                estimatedOperations.add(trainedModelConfig.getEstimatedOperations());
                estimatedMemoryUsageBytes.add(trainedModelConfig.getModelSize());
            }

            Map<String, Object> counts = new HashMap<>();
            counts.put("total", trainedModelConfigs.size());
            counts.put("classification", classificationCount);
            counts.put("regression", regressionCount);
            counts.put("prepackaged", prepackagedCount);
            counts.put("other", trainedModelConfigs.size() - createdByAnalyticsCount - prepackagedCount);

            trainedModelsUsage.put("count", counts);
            trainedModelsUsage.put(TrainedModelConfig.ESTIMATED_OPERATIONS.getPreferredName(), estimatedOperations.asMap());
            trainedModelsUsage.put(
                TrainedModelConfig.DEPRECATED_ESTIMATED_HEAP_MEMORY_USAGE_BYTES.getPreferredName(),
                estimatedMemoryUsageBytes.asMap()
            );
            trainedModelsUsage.put(TrainedModelConfig.MODEL_SIZE_BYTES.getPreferredName(), estimatedMemoryUsageBytes.asMap());

            inferenceUsage.put("trained_models", trainedModelsUsage);
        }

        // TODO separate out ours and users models possibly regression vs classification
        private void addInferenceIngestUsage(NodesStatsResponse response, Map<String, Object> inferenceUsage) {
            Set<String> pipelines = new HashSet<>();
            Map<String, Long> docCountStats = new HashMap<>(3);
            Map<String, Long> timeStats = new HashMap<>(3);
            Map<String, Long> failureStats = new HashMap<>(3);
            initializeStats(docCountStats);
            initializeStats(timeStats);
            initializeStats(failureStats);

            response.getNodes()
                .stream()
                .map(NodeStats::getIngestStats)
                .map(IngestStats::getProcessorStats)
                .forEach(map -> map.forEach((pipelineId, processors) -> {
                    boolean containsInference = false;
                    for (IngestStats.ProcessorStat stats : processors) {
                        if (stats.getName().equals(InferenceProcessor.TYPE)) {
                            containsInference = true;
                            long ingestCount = stats.getStats().getIngestCount();
                            long ingestTime = stats.getStats().getIngestTimeInMillis();
                            long failureCount = stats.getStats().getIngestFailedCount();
                            updateStats(docCountStats, ingestCount);
                            updateStats(timeStats, ingestTime);
                            updateStats(failureStats, failureCount);
                        }
                    }
                    if (containsInference) {
                        pipelines.add(pipelineId);
                    }
                }));

            Map<String, Object> ingestUsage = new HashMap<>(6);
            ingestUsage.put("pipelines", createCountUsageEntry(pipelines.size()));
            ingestUsage.put("num_docs_processed", docCountStats);
            ingestUsage.put("time_ms", timeStats);
            ingestUsage.put("num_failures", failureStats);
            inferenceUsage.put("ingest_processors", Collections.singletonMap(MachineLearningFeatureSetUsage.ALL, ingestUsage));
        }
    }
}
