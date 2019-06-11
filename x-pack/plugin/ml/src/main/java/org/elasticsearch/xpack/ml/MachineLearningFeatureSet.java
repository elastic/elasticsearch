/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.StatsAccumulator;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.NativeControllerHolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class MachineLearningFeatureSet implements XPackFeatureSet {

    /**
     * List of platforms for which the native processes are available
     */
    private static final List<String> mlPlatforms =
            Arrays.asList("darwin-x86_64", "linux-x86_64", "windows-x86_64");

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;
    private final Client client;
    private final JobManagerHolder jobManagerHolder;
    private final Map<String, Object> nativeCodeInfo;

    @Inject
    public MachineLearningFeatureSet(Environment environment, ClusterService clusterService, Client client,
                                     @Nullable XPackLicenseState licenseState, JobManagerHolder jobManagerHolder) {
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(environment.settings());
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.licenseState = licenseState;
        this.jobManagerHolder = jobManagerHolder;
        Map<String, Object> nativeCodeInfo = NativeController.UNKNOWN_NATIVE_CODE_INFO;
        // Don't try to get the native code version if ML is disabled - it causes too much controversy
        // if ML has been disabled because of some OS incompatibility.
        if (enabled) {
            try {
                if (isRunningOnMlPlatform(true)) {
                    NativeController nativeController = NativeControllerHolder.getNativeController(clusterService.getNodeName(),
                        environment);
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
            throw new ElasticsearchException("X-Pack is not supported and Machine Learning is not available for [" + platformName
                    + "]; you can use the other X-Pack features (unsupported) by setting xpack.ml.enabled: false in elasticsearch.yml");
        }
        return false;
    }

    @Override
    public String name() {
        return XPackField.MACHINE_LEARNING;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isMachineLearningAllowed();
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
        new Retriever(client, jobManagerHolder, available(), enabled(), mlNodeCount(state)).execute(listener);
    }

    private int mlNodeCount(final ClusterState clusterState) {
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
        private int nodeCount;

        public Retriever(Client client, JobManagerHolder jobManagerHolder, boolean available, boolean enabled, int nodeCount) {
            this.client = Objects.requireNonNull(client);
            this.jobManagerHolder = jobManagerHolder;
            this.available = available;
            this.enabled = enabled;
            this.jobsUsage = new LinkedHashMap<>();
            this.datafeedsUsage = new LinkedHashMap<>();
            this.nodeCount = nodeCount;
        }

        public void execute(ActionListener<Usage> listener) {
            // empty holder means ML is disabled
            if (jobManagerHolder.isEmpty()) {
                listener.onResponse(
                    new MachineLearningFeatureSetUsage(available, enabled, Collections.emptyMap(), Collections.emptyMap(), 0));
                return;
            }

            // Step 2. Extract usage from datafeeds stats and return usage response
            ActionListener<GetDatafeedsStatsAction.Response> datafeedStatsListener =
                    ActionListener.wrap(response -> {
                                addDatafeedsUsage(response);
                                listener.onResponse(new MachineLearningFeatureSetUsage(
                                        available, enabled, jobsUsage, datafeedsUsage, nodeCount));
                            },
                            listener::onFailure
                    );

            // Step 1. Extract usage from jobs stats and then request stats for all datafeeds
            GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(MetaData.ALL);
            ActionListener<GetJobsStatsAction.Response> jobStatsListener = ActionListener.wrap(
                    response -> {
                        jobManagerHolder.getJobManager().expandJobs(MetaData.ALL, true, ActionListener.wrap(jobs -> {
                            addJobsUsage(response, jobs.results());
                            GetDatafeedsStatsAction.Request datafeedStatsRequest = new GetDatafeedsStatsAction.Request(
                                    GetDatafeedsStatsAction.ALL);
                            client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest, datafeedStatsListener);
                        }, listener::onFailure));
                    }, listener::onFailure);

            // Step 0. Kick off the chain of callbacks by requesting jobs stats
            client.execute(GetJobsStatsAction.INSTANCE, jobStatsRequest, jobStatsListener);
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

        private void addDatafeedsUsage(GetDatafeedsStatsAction.Response response) {
            Map<DatafeedState, Counter> datafeedCountByState = new HashMap<>();

            List<GetDatafeedsStatsAction.Response.DatafeedStats> datafeedsStats = response.getResponse().results();
            for (GetDatafeedsStatsAction.Response.DatafeedStats datafeedStats : datafeedsStats) {
                datafeedCountByState.computeIfAbsent(datafeedStats.getDatafeedState(),
                        ds -> Counter.newCounter()).addAndGet(1);
            }

            datafeedsUsage.put(MachineLearningFeatureSetUsage.ALL, createDatafeedUsageEntry(response.getResponse().count()));
            for (DatafeedState datafeedState : datafeedCountByState.keySet()) {
                datafeedsUsage.put(datafeedState.name().toLowerCase(Locale.ROOT),
                        createDatafeedUsageEntry(datafeedCountByState.get(datafeedState).get()));
            }
        }

        private Map<String, Object> createDatafeedUsageEntry(long count) {
            Map<String, Object> usage = new HashMap<>();
            usage.put(MachineLearningFeatureSetUsage.COUNT, count);
            return usage;
        }
    }
}
