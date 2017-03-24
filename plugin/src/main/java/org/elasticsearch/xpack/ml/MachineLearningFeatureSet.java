/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.NativeControllerHolder;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.ml.utils.StatsAccumulator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

public class MachineLearningFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;
    private final Client client;
    private final Map<String, Object> nativeCodeInfo;

    @Inject
    public MachineLearningFeatureSet(Settings settings, ClusterService clusterService, Client client,
                                     @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.client = Objects.requireNonNull(client);
        this.licenseState = licenseState;
        Map<String, Object> nativeCodeInfo = NativeController.UNKNOWN_NATIVE_CODE_INFO;
        // Don't try to get the native code version in the transport client - the controller process won't be running
        if (XPackPlugin.transportClientMode(settings) == false && XPackPlugin.isTribeClientNode(settings) == false) {
            try {
                NativeController nativeController = NativeControllerHolder.getNativeController(settings);
                if (nativeController != null) {
                    nativeCodeInfo = nativeController.getNativeCodeInfo();
                }
            } catch (IOException | TimeoutException e) {
                Loggers.getLogger(MachineLearningFeatureSet.class).error("Cannot get native code info for Machine Learning", e);
                if (enabled) {
                    throw new ElasticsearchException("Cannot communicate with Machine Learning native code "
                            + "- please check that you are running on a supported platform");
                }
            }
        }
        this.nativeCodeInfo = nativeCodeInfo;
    }

    @Override
    public String name() {
        return XPackPlugin.MACHINE_LEARNING;
    }

    @Override
    public String description() {
        return "Machine Learning for the Elastic Stack";
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
        MlMetadata mlMetadata = state.getMetaData().custom(MlMetadata.TYPE);
        new Usage.Retriever(client, mlMetadata, available(), enabled()).execute(listener);
    }

    public static class Usage extends XPackFeatureSet.Usage {

        private static final String ALL = "_all";
        private static final String JOBS_FIELD = "jobs";
        private static final String DATAFEEDS_FIELD = "datafeeds";
        private static final String COUNT = "count";
        private static final String DETECTORS = "detectors";
        private static final String MODEL_SIZE = "model_size";

        private final Map<String, Object> jobsUsage;
        private final Map<String, Object> datafeedsUsage;

        public Usage(boolean available, boolean enabled, Map<String, Object> jobsUsage,
                       Map<String, Object> datafeedsUsage) {
            super(XPackPlugin.MACHINE_LEARNING, available, enabled);
            this.jobsUsage = Objects.requireNonNull(jobsUsage);
            this.datafeedsUsage = Objects.requireNonNull(datafeedsUsage);
        }

        public Usage(StreamInput in) throws IOException {
            super(in);
            this.jobsUsage = in.readMap();
            this.datafeedsUsage = in.readMap();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeMap(jobsUsage);
            out.writeMap(datafeedsUsage);
        }

        @Override
        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
            if (jobsUsage != null) {
                builder.field(JOBS_FIELD, jobsUsage);
            }
            if (datafeedsUsage != null) {
                builder.field(DATAFEEDS_FIELD, datafeedsUsage);
            }
        }

        public static class Retriever {

            private final Client client;
            private final MlMetadata mlMetadata;
            private final boolean available;
            private final boolean enabled;
            private Map<String, Object> jobsUsage;
            private Map<String, Object> datafeedsUsage;

            public Retriever(Client client, MlMetadata mlMetadata, boolean available, boolean enabled) {
                this.client = Objects.requireNonNull(client);
                this.mlMetadata = mlMetadata;
                this.available = available;
                this.enabled = enabled;
                this.jobsUsage = new LinkedHashMap<>();
                this.datafeedsUsage = new LinkedHashMap<>();
            }

            public void execute(ActionListener<XPackFeatureSet.Usage> listener) {
                if (enabled == false) {
                    listener.onResponse(new Usage(available, enabled, Collections.emptyMap(), Collections.emptyMap()));
                    return;
                }

                // Step 2. Extract usage from datafeeds stats and return usage response
                ActionListener<GetDatafeedsStatsAction.Response> datafeedStatsListener =
                        ActionListener.wrap(response -> {
                                    addDatafeedsUsage(response);
                                    listener.onResponse(new Usage(
                                            available, enabled, jobsUsage, datafeedsUsage));
                                },
                                error -> {
                                    listener.onFailure(error);
                                }
                        );

                // Step 1. Extract usage from jobs stats and then request stats for all datafeeds
                GetJobsStatsAction.Request jobStatsRequest = new GetJobsStatsAction.Request(Job.ALL);
                ActionListener<GetJobsStatsAction.Response> jobStatsListener = ActionListener.wrap(
                        response -> {
                            addJobsUsage(response);
                            GetDatafeedsStatsAction.Request datafeedStatsRequest =
                                    new GetDatafeedsStatsAction.Request(GetDatafeedsStatsAction.ALL);
                            client.execute(GetDatafeedsStatsAction.INSTANCE, datafeedStatsRequest,
                                    datafeedStatsListener);
                        },
                        error -> {
                            listener.onFailure(error);
                        }
                );

                // Step 0. Kick off the chain of callbacks by requesting jobs stats
                client.execute(GetJobsStatsAction.INSTANCE, jobStatsRequest, jobStatsListener);
            }

            private void addJobsUsage(GetJobsStatsAction.Response response) {
                StatsAccumulator allJobsDetectorsStats = new StatsAccumulator();
                StatsAccumulator allJobsModelSizeStats = new StatsAccumulator();

                Map<JobState, Counter> jobCountByState = new HashMap<>();
                Map<JobState, StatsAccumulator> detectorStatsByState = new HashMap<>();
                Map<JobState, StatsAccumulator> modelSizeStatsByState = new HashMap<>();

                Map<String, Job> jobs = mlMetadata.getJobs();
                for (GetJobsStatsAction.Response.JobStats jobStats
                        : response.getResponse().results()) {
                    ModelSizeStats modelSizeStats = jobStats.getModelSizeStats();
                    int detectorsCount = jobs.get(jobStats.getJobId()).getAnalysisConfig()
                            .getDetectors().size();
                    double modelSize = modelSizeStats == null ? 0.0
                            : jobStats.getModelSizeStats().getModelBytes();

                    allJobsDetectorsStats.add(detectorsCount);
                    allJobsModelSizeStats.add(modelSize);

                    JobState jobState = jobStats.getState();
                    jobCountByState.computeIfAbsent(jobState, js -> Counter.newCounter()).addAndGet(1);
                    detectorStatsByState.computeIfAbsent(jobState,
                            js -> new StatsAccumulator()).add(detectorsCount);
                    modelSizeStatsByState.computeIfAbsent(jobState,
                            js -> new StatsAccumulator()).add(modelSize);
                }

                jobsUsage.put(ALL, createJobUsageEntry(jobs.size(), allJobsDetectorsStats,
                        allJobsModelSizeStats));
                for (JobState jobState : jobCountByState.keySet()) {
                    jobsUsage.put(jobState.name().toLowerCase(Locale.ROOT), createJobUsageEntry(
                            jobCountByState.get(jobState).get(),
                            detectorStatsByState.get(jobState),
                            modelSizeStatsByState.get(jobState)));
                }
            }

            private Map<String, Object> createJobUsageEntry(long count, StatsAccumulator detectorStats,
                                                            StatsAccumulator modelSizeStats) {
                Map<String, Object> usage = new HashMap<>();
                usage.put(COUNT, count);
                usage.put(DETECTORS, detectorStats.asMap());
                usage.put(MODEL_SIZE, modelSizeStats.asMap());
                return usage;
            }

            private void addDatafeedsUsage(GetDatafeedsStatsAction.Response response) {
                Map<DatafeedState, Counter> datafeedCountByState = new HashMap<>();

                for (GetDatafeedsStatsAction.Response.DatafeedStats datafeedStats
                        : response.getResponse().results()) {
                    datafeedCountByState.computeIfAbsent(datafeedStats.getDatafeedState(),
                            ds -> Counter.newCounter()).addAndGet(1);
                }

                datafeedsUsage.put(ALL, createDatafeedUsageEntry(response.getResponse().count()));
                for (DatafeedState datafeedState : datafeedCountByState.keySet()) {
                    datafeedsUsage.put(datafeedState.name().toLowerCase(Locale.ROOT),
                            createDatafeedUsageEntry(datafeedCountByState.get(datafeedState).get()));
                }
            }

            private Map<String, Object> createDatafeedUsageEntry(long count) {
                Map<String, Object> usage = new HashMap<>();
                usage.put(COUNT, count);
                return usage;
            }
        }
    }
}
