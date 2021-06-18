/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MachineLearningFeatureSetUsage;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetTrainedModelsAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfig;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ClassificationConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSizeStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStats;
import org.elasticsearch.xpack.core.ml.stats.ForecastStatsTests;
import org.elasticsearch.xpack.core.watcher.support.xcontent.XContentSource;
import org.elasticsearch.xpack.ml.inference.ingest.InferenceProcessor;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.JobManagerHolder;
import org.junit.Before;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MachineLearningInfoTransportActionTests extends ESTestCase {

    private Settings commonSettings;
    private ClusterService clusterService;
    private Client client;
    private JobManager jobManager;
    private JobManagerHolder jobManagerHolder;
    private XPackLicenseState licenseState;

    @Before
    public void init() {
        commonSettings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false)
                .build();
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        jobManager = mock(JobManager.class);
        jobManagerHolder = new JobManagerHolder(jobManager);
        licenseState = mock(XPackLicenseState.class);
        ClusterState clusterState = new ClusterState.Builder(ClusterState.EMPTY_STATE).build();
        when(clusterService.state()).thenReturn(clusterState);
        givenJobs(Collections.emptyList(), Collections.emptyList());
        givenDatafeeds(Collections.emptyList());
        givenDataFrameAnalytics(Collections.emptyList(), Collections.emptyList());
        givenProcessorStats(Collections.emptyList());
        givenTrainedModels(Collections.emptyList());
    }

    private MachineLearningUsageTransportAction newUsageAction(Settings settings) {
        return new MachineLearningUsageTransportAction(mock(TransportService.class), clusterService,
            null, mock(ActionFilters.class), mock(IndexNameExpressionResolver.class),
            TestEnvironment.newEnvironment(settings), client, licenseState, jobManagerHolder);
    }

    public void testAvailable() throws Exception {
        MachineLearningInfoTransportAction featureSet = new MachineLearningInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), commonSettings, licenseState);
        boolean available = randomBoolean();
        when(licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)).thenReturn(available);
        assertThat(featureSet.available(), is(available));
        var usageAction = newUsageAction(commonSettings);
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, ClusterState.EMPTY_STATE, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.available(), is(available));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new MachineLearningFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.available(), is(available));
    }

    public void testEnabled() throws Exception {
        boolean useDefault = randomBoolean();
        boolean enabled = true;
        Settings.Builder settings = Settings.builder().put(commonSettings);
        if (useDefault == false) {
            enabled = randomBoolean();
            settings.put("xpack.ml.enabled", enabled);
        }
        boolean expected = enabled || useDefault;
        MachineLearningInfoTransportAction featureSet = new MachineLearningInfoTransportAction(
            mock(TransportService.class), mock(ActionFilters.class), settings.build(), licenseState);
        assertThat(featureSet.enabled(), is(expected));
        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, ClusterState.EMPTY_STATE, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();
        assertThat(usage.enabled(), is(expected));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new MachineLearningFeatureSetUsage(out.bytes().streamInput());
        assertThat(serializedUsage.enabled(), is(expected));
    }

    public void testUsage() throws Exception {
        when(licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)).thenReturn(true);
        Settings.Builder settings = Settings.builder().put(commonSettings);
        settings.put("xpack.ml.enabled", true);

        Job opened1 = buildJob("opened1", Collections.singletonList(buildMinDetector("foo")),
                Collections.singletonMap("created_by", randomFrom("a-cool-module", "a_cool_module", "a cool module")));
        GetJobsStatsAction.Response.JobStats opened1JobStats = buildJobStats("opened1", JobState.OPENED, 100L, 3L);
        Job opened2 = buildJob("opened2", Arrays.asList(buildMinDetector("foo"), buildMinDetector("bar")));
        GetJobsStatsAction.Response.JobStats opened2JobStats = buildJobStats("opened2", JobState.OPENED, 200L, 8L);
        Job closed1 = buildJob("closed1", Arrays.asList(buildMinDetector("foo"), buildMinDetector("bar"), buildMinDetector("foobar")));
        GetJobsStatsAction.Response.JobStats closed1JobStats = buildJobStats("closed1", JobState.CLOSED, 300L, 0);
        givenJobs(Arrays.asList(opened1, opened2, closed1),
                Arrays.asList(opened1JobStats, opened2JobStats, closed1JobStats));

        givenDatafeeds(Arrays.asList(
                buildDatafeedStats(DatafeedState.STARTED),
                buildDatafeedStats(DatafeedState.STARTED),
                buildDatafeedStats(DatafeedState.STOPPED)
        ));

        DataFrameAnalyticsConfig dfa1 = DataFrameAnalyticsConfigTests.createRandom("dfa_1");
        DataFrameAnalyticsConfig dfa2 = DataFrameAnalyticsConfigTests.createRandom("dfa_2");
        DataFrameAnalyticsConfig dfa3 = DataFrameAnalyticsConfigTests.createRandom("dfa_3");

        List<DataFrameAnalyticsConfig> dataFrameAnalytics = Arrays.asList(dfa1, dfa2, dfa3);
        givenDataFrameAnalytics(dataFrameAnalytics, Arrays.asList(
            buildDataFrameAnalyticsStats(dfa1.getId(), DataFrameAnalyticsState.STOPPED, null),
            buildDataFrameAnalyticsStats(dfa2.getId(), DataFrameAnalyticsState.STOPPED, 100L),
            buildDataFrameAnalyticsStats(dfa3.getId(), DataFrameAnalyticsState.STARTED, 200L)
        ));

        Map<String, Integer> expectedDfaCountByAnalysis = new HashMap<>();
        dataFrameAnalytics.forEach(dfa -> {
            String analysisName = dfa.getAnalysis().getWriteableName();
            Integer analysisCount = expectedDfaCountByAnalysis.computeIfAbsent(analysisName, c -> 0);
            expectedDfaCountByAnalysis.put(analysisName, ++analysisCount);
        });

        givenProcessorStats(Arrays.asList(
            buildNodeStats(
                Arrays.asList("pipeline1", "pipeline2", "pipeline3"),
                Arrays.asList(
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(10, 1, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0)),
                        new IngestStats.ProcessorStat(
                            InferenceProcessor.TYPE,
                            InferenceProcessor.TYPE,
                            new IngestStats.Stats(100, 10, 0, 1))
                    ),
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(5, 1, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                    ),
                    Arrays.asList(
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                    )
                )),
            buildNodeStats(
                Arrays.asList("pipeline1", "pipeline2", "pipeline3"),
                Arrays.asList(
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(0, 0, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(0, 0, 0, 0)),
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(10, 1, 0, 0))
                    ),
                    Arrays.asList(
                        new IngestStats.ProcessorStat(InferenceProcessor.TYPE, InferenceProcessor.TYPE, new IngestStats.Stats(5, 1, 0, 0)),
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                    ),
                    Arrays.asList(
                        new IngestStats.ProcessorStat("grok", "grok", new IngestStats.Stats(10, 1, 0, 0))
                    )
                ))
        ));

        TrainedModelConfig trainedModel1 = TrainedModelConfigTests.createTestInstance("model_1")
            .setEstimatedHeapMemory(100)
            .setEstimatedOperations(200)
            .setMetadata(Collections.singletonMap("analytics_config", "anything"))
            .build();
        TrainedModelConfig trainedModel2 = TrainedModelConfigTests.createTestInstance("model_2")
            .setEstimatedHeapMemory(200)
            .setEstimatedOperations(400)
            .setMetadata(Collections.singletonMap("analytics_config", "anything"))
            .build();
        TrainedModelConfig trainedModel3 = TrainedModelConfigTests.createTestInstance("model_3")
            .setEstimatedHeapMemory(300)
            .setEstimatedOperations(600)
            .build();
        TrainedModelConfig trainedModel4 = TrainedModelConfigTests.createTestInstance("model_4")
            .setTags(Collections.singletonList("prepackaged"))
            .setEstimatedHeapMemory(1000)
            .setEstimatedOperations(2000)
            .build();
        givenTrainedModels(Arrays.asList(trainedModel1, trainedModel2, trainedModel3, trainedModel4));

        Map<String, Integer> trainedModelsCountByAnalysis = new HashMap<>();
        trainedModelsCountByAnalysis.put("classification", 0);
        trainedModelsCountByAnalysis.put("regression", 0);
        for (TrainedModelConfig trainedModel : Arrays.asList(trainedModel1, trainedModel2, trainedModel3)) {
            if (trainedModel.getInferenceConfig() instanceof ClassificationConfig) {
                trainedModelsCountByAnalysis.put("classification", trainedModelsCountByAnalysis.get("classification") + 1);
            } else if (trainedModel.getInferenceConfig() instanceof RegressionConfig) {
                trainedModelsCountByAnalysis.put("regression", trainedModelsCountByAnalysis.get("regression") + 1);
            }
        }

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, ClusterState.EMPTY_STATE, future);
        XPackFeatureSet.Usage mlUsage = future.get().getUsage();

        BytesStreamOutput out = new BytesStreamOutput();
        mlUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new MachineLearningFeatureSetUsage(out.bytes().streamInput());

        for (XPackFeatureSet.Usage usage : Arrays.asList(mlUsage, serializedUsage)) {
            assertThat(usage, is(notNullValue()));
            assertThat(usage.name(), is(XPackField.MACHINE_LEARNING));
            assertThat(usage.enabled(), is(true));
            assertThat(usage.available(), is(true));
            XContentSource source;
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
                source = new XContentSource(builder);
            }

            assertThat(source.getValue("jobs._all.count"), equalTo(3));
            assertThat(source.getValue("jobs._all.detectors.min"), equalTo(1.0));
            assertThat(source.getValue("jobs._all.detectors.max"), equalTo(3.0));
            assertThat(source.getValue("jobs._all.detectors.total"), equalTo(6.0));
            assertThat(source.getValue("jobs._all.detectors.avg"), equalTo(2.0));
            assertThat(source.getValue("jobs._all.model_size.min"), equalTo(100.0));
            assertThat(source.getValue("jobs._all.model_size.max"), equalTo(300.0));
            assertThat(source.getValue("jobs._all.model_size.total"), equalTo(600.0));
            assertThat(source.getValue("jobs._all.model_size.avg"), equalTo(200.0));
            assertThat(source.getValue("jobs._all.created_by.a_cool_module"), equalTo(1));
            assertThat(source.getValue("jobs._all.created_by.unknown"), equalTo(2));

            assertThat(source.getValue("jobs.opened.count"), equalTo(2));
            assertThat(source.getValue("jobs.opened.detectors.min"), equalTo(1.0));
            assertThat(source.getValue("jobs.opened.detectors.max"), equalTo(2.0));
            assertThat(source.getValue("jobs.opened.detectors.total"), equalTo(3.0));
            assertThat(source.getValue("jobs.opened.detectors.avg"), equalTo(1.5));
            assertThat(source.getValue("jobs.opened.model_size.min"), equalTo(100.0));
            assertThat(source.getValue("jobs.opened.model_size.max"), equalTo(200.0));
            assertThat(source.getValue("jobs.opened.model_size.total"), equalTo(300.0));
            assertThat(source.getValue("jobs.opened.model_size.avg"), equalTo(150.0));
            assertThat(source.getValue("jobs.opened.created_by.a_cool_module"), equalTo(1));
            assertThat(source.getValue("jobs.opened.created_by.unknown"), equalTo(1));

            assertThat(source.getValue("jobs.closed.count"), equalTo(1));
            assertThat(source.getValue("jobs.closed.detectors.min"), equalTo(3.0));
            assertThat(source.getValue("jobs.closed.detectors.max"), equalTo(3.0));
            assertThat(source.getValue("jobs.closed.detectors.total"), equalTo(3.0));
            assertThat(source.getValue("jobs.closed.detectors.avg"), equalTo(3.0));
            assertThat(source.getValue("jobs.closed.model_size.min"), equalTo(300.0));
            assertThat(source.getValue("jobs.closed.model_size.max"), equalTo(300.0));
            assertThat(source.getValue("jobs.closed.model_size.total"), equalTo(300.0));
            assertThat(source.getValue("jobs.closed.model_size.avg"), equalTo(300.0));
            assertThat(source.getValue("jobs.closed.created_by.a_cool_module"), is(nullValue()));
            assertThat(source.getValue("jobs.closed.created_by.unknown"), equalTo(1));

            assertThat(source.getValue("jobs.opening"), is(nullValue()));
            assertThat(source.getValue("jobs.closing"), is(nullValue()));
            assertThat(source.getValue("jobs.failed"), is(nullValue()));

            assertThat(source.getValue("datafeeds._all.count"), equalTo(3));
            assertThat(source.getValue("datafeeds.started.count"), equalTo(2));
            assertThat(source.getValue("datafeeds.stopped.count"), equalTo(1));

            assertThat(source.getValue("data_frame_analytics_jobs._all.count"), equalTo(3));
            assertThat(source.getValue("data_frame_analytics_jobs.started.count"), equalTo(1));
            assertThat(source.getValue("data_frame_analytics_jobs.stopped.count"), equalTo(2));
            assertThat(source.getValue("data_frame_analytics_jobs.analysis_counts"), equalTo(expectedDfaCountByAnalysis));
            assertThat(source.getValue("data_frame_analytics_jobs.memory_usage.peak_usage_bytes.min"), equalTo(100.0));
            assertThat(source.getValue("data_frame_analytics_jobs.memory_usage.peak_usage_bytes.max"), equalTo(200.0));
            assertThat(source.getValue("data_frame_analytics_jobs.memory_usage.peak_usage_bytes.total"), equalTo(300.0));
            assertThat(source.getValue("data_frame_analytics_jobs.memory_usage.peak_usage_bytes.avg"), equalTo(150.0));

            assertThat(source.getValue("jobs._all.forecasts.total"), equalTo(11));
            assertThat(source.getValue("jobs._all.forecasts.forecasted_jobs"), equalTo(2));

            assertThat(source.getValue("jobs.closed.forecasts.total"), equalTo(0));
            assertThat(source.getValue("jobs.closed.forecasts.forecasted_jobs"), equalTo(0));

            assertThat(source.getValue("jobs.opened.forecasts.total"), equalTo(11));
            assertThat(source.getValue("jobs.opened.forecasts.forecasted_jobs"), equalTo(2));

            assertThat(source.getValue("inference.trained_models._all.count"), equalTo(4));
            assertThat(source.getValue("inference.trained_models.estimated_heap_memory_usage_bytes.min"), equalTo(100.0));
            assertThat(source.getValue("inference.trained_models.estimated_heap_memory_usage_bytes.max"), equalTo(300.0));
            assertThat(source.getValue("inference.trained_models.estimated_heap_memory_usage_bytes.total"), equalTo(600.0));
            assertThat(source.getValue("inference.trained_models.estimated_heap_memory_usage_bytes.avg"), equalTo(200.0));
            assertThat(source.getValue("inference.trained_models.estimated_operations.min"), equalTo(200.0));
            assertThat(source.getValue("inference.trained_models.estimated_operations.max"), equalTo(600.0));
            assertThat(source.getValue("inference.trained_models.estimated_operations.total"), equalTo(1200.0));
            assertThat(source.getValue("inference.trained_models.estimated_operations.avg"), equalTo(400.0));
            assertThat(source.getValue("inference.trained_models.count.total"), equalTo(4));
            assertThat(source.getValue("inference.trained_models.count.classification"),
                equalTo(trainedModelsCountByAnalysis.get("classification")));
            assertThat(source.getValue("inference.trained_models.count.regression"),
                equalTo(trainedModelsCountByAnalysis.get("regression")));
            assertThat(source.getValue("inference.trained_models.count.prepackaged"), equalTo(1));
            assertThat(source.getValue("inference.trained_models.count.other"), equalTo(1));

            assertThat(source.getValue("inference.ingest_processors._all.pipelines.count"), equalTo(2));
            assertThat(source.getValue("inference.ingest_processors._all.num_docs_processed.sum"), equalTo(130));
            assertThat(source.getValue("inference.ingest_processors._all.num_docs_processed.min"), equalTo(0));
            assertThat(source.getValue("inference.ingest_processors._all.num_docs_processed.max"), equalTo(100));
            assertThat(source.getValue("inference.ingest_processors._all.time_ms.sum"), equalTo(14));
            assertThat(source.getValue("inference.ingest_processors._all.time_ms.min"), equalTo(0));
            assertThat(source.getValue("inference.ingest_processors._all.time_ms.max"), equalTo(10));
            assertThat(source.getValue("inference.ingest_processors._all.num_failures.sum"), equalTo(1));
            assertThat(source.getValue("inference.ingest_processors._all.num_failures.min"), equalTo(0));
            assertThat(source.getValue("inference.ingest_processors._all.num_failures.max"), equalTo(1));
        }
    }

    public void testUsageWithOrphanedTask() throws Exception {
        when(licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)).thenReturn(true);
        Settings.Builder settings = Settings.builder().put(commonSettings);
        settings.put("xpack.ml.enabled", true);

        Job opened1 = buildJob("opened1", Collections.singletonList(buildMinDetector("foo")),
            Collections.singletonMap("created_by", randomFrom("a-cool-module", "a_cool_module", "a cool module")));
        GetJobsStatsAction.Response.JobStats opened1JobStats = buildJobStats("opened1", JobState.OPENED, 100L, 3L);
        // NB: we have JobStats but no Job for "opened2"
        GetJobsStatsAction.Response.JobStats opened2JobStats = buildJobStats("opened2", JobState.OPENED, 200L, 8L);
        Job closed1 = buildJob("closed1", Arrays.asList(buildMinDetector("foo"), buildMinDetector("bar"), buildMinDetector("foobar")));
        GetJobsStatsAction.Response.JobStats closed1JobStats = buildJobStats("closed1", JobState.CLOSED, 300L, 0);
        givenJobs(Arrays.asList(opened1, closed1), Arrays.asList(opened1JobStats, opened2JobStats, closed1JobStats));

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, ClusterState.EMPTY_STATE, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();

        XContentSource source;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            source = new XContentSource(builder);
        }

        // The orphaned job should be excluded from the usage info
        assertThat(source.getValue("jobs._all.count"), equalTo(2));
        assertThat(source.getValue("jobs._all.detectors.min"), equalTo(1.0));
        assertThat(source.getValue("jobs._all.detectors.max"), equalTo(3.0));
        assertThat(source.getValue("jobs._all.detectors.total"), equalTo(4.0));
        assertThat(source.getValue("jobs._all.detectors.avg"), equalTo(2.0));
        assertThat(source.getValue("jobs._all.model_size.min"), equalTo(100.0));
        assertThat(source.getValue("jobs._all.model_size.max"), equalTo(300.0));
        assertThat(source.getValue("jobs._all.model_size.total"), equalTo(400.0));
        assertThat(source.getValue("jobs._all.model_size.avg"), equalTo(200.0));
        assertThat(source.getValue("jobs._all.created_by.a_cool_module"), equalTo(1));
        assertThat(source.getValue("jobs._all.created_by.unknown"), equalTo(1));
    }

    public void testUsageDisabledML() throws Exception {
        when(licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)).thenReturn(true);
        Settings.Builder settings = Settings.builder().put(commonSettings);
        settings.put("xpack.ml.enabled", false);

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, ClusterState.EMPTY_STATE, future);
        XPackFeatureSet.Usage mlUsage = future.get().getUsage();
        BytesStreamOutput out = new BytesStreamOutput();
        mlUsage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new MachineLearningFeatureSetUsage(out.bytes().streamInput());

        for (XPackFeatureSet.Usage usage : Arrays.asList(mlUsage, serializedUsage)) {
            assertThat(usage, is(notNullValue()));
            assertThat(usage.name(), is(XPackField.MACHINE_LEARNING));
            assertThat(usage.enabled(), is(false));
        }
    }

    public void testNodeCount() throws Exception {
        when(licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)).thenReturn(true);
        int nodeCount = randomIntBetween(1, 3);
        ClusterState clusterState = givenNodeCount(nodeCount);
        Settings.Builder settings = Settings.builder().put(commonSettings);
        settings.put("xpack.ml.enabled", true);

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, clusterState, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();

        assertThat(usage.available(), is(true));
        assertThat(usage.enabled(), is(true));

        BytesStreamOutput out = new BytesStreamOutput();
        usage.writeTo(out);
        XPackFeatureSet.Usage serializedUsage = new MachineLearningFeatureSetUsage(out.bytes().streamInput());

        XContentSource source;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            serializedUsage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            source = new XContentSource(builder);
        }
        assertThat(source.getValue("node_count"), equalTo(nodeCount));
    }

    public void testUsageGivenMlMetadataNotInstalled() throws Exception {
        when(licenseState.isAllowed(XPackLicenseState.Feature.MACHINE_LEARNING)).thenReturn(true);
        Settings.Builder settings = Settings.builder().put(commonSettings);
        settings.put("xpack.ml.enabled", true);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);

        var usageAction = newUsageAction(settings.build());
        PlainActionFuture<XPackUsageFeatureResponse> future = new PlainActionFuture<>();
        usageAction.masterOperation(null, null, ClusterState.EMPTY_STATE, future);
        XPackFeatureSet.Usage usage = future.get().getUsage();

        assertThat(usage.available(), is(true));
        assertThat(usage.enabled(), is(true));

        XContentSource source;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            usage.toXContent(builder, ToXContent.EMPTY_PARAMS);
            source = new XContentSource(builder);
        }

        assertThat(source.getValue("jobs._all.count"), equalTo(0));
        assertThat(source.getValue("jobs._all.detectors.min"), equalTo(0.0));
        assertThat(source.getValue("jobs._all.detectors.max"), equalTo(0.0));
        assertThat(source.getValue("jobs._all.detectors.total"), equalTo(0.0));
        assertThat(source.getValue("jobs._all.detectors.avg"), equalTo(0.0));
        assertThat(source.getValue("jobs._all.model_size.min"), equalTo(0.0));
        assertThat(source.getValue("jobs._all.model_size.max"), equalTo(0.0));
        assertThat(source.getValue("jobs._all.model_size.total"), equalTo(0.0));
        assertThat(source.getValue("jobs._all.model_size.avg"), equalTo(0.0));

        assertThat(source.getValue("jobs.opening"), is(nullValue()));
        assertThat(source.getValue("jobs.opened"), is(nullValue()));
        assertThat(source.getValue("jobs.closing"), is(nullValue()));
        assertThat(source.getValue("jobs.closed"), is(nullValue()));
        assertThat(source.getValue("jobs.failed"), is(nullValue()));

        assertThat(source.getValue("datafeeds._all.count"), equalTo(0));

        assertThat(source.getValue("datafeeds.started"), is(nullValue()));
        assertThat(source.getValue("datafeeds.stopped"), is(nullValue()));
    }

    private void givenJobs(List<Job> jobs, List<GetJobsStatsAction.Response.JobStats> jobsStats) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<QueryPage<Job>> jobListener =
                    (ActionListener<QueryPage<Job>>) invocationOnMock.getArguments()[2];
            jobListener.onResponse(
                    new QueryPage<>(jobs, jobs.size(), Job.RESULTS_FIELD));
            return Void.TYPE;
        }).when(jobManager).expandJobs(eq(Metadata.ALL), eq(true), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetJobsStatsAction.Response> listener =
                    (ActionListener<GetJobsStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(new GetJobsStatsAction.Response(
                    new QueryPage<>(jobsStats, jobsStats.size(), Job.RESULTS_FIELD)));
            return Void.TYPE;
        }).when(client).execute(same(GetJobsStatsAction.INSTANCE), any(), any());
    }

    private ClusterState givenNodeCount(int nodeCount) {
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < nodeCount; i++) {
            Map<String, String> attrs = new HashMap<>();
            attrs.put(MachineLearning.MAX_OPEN_JOBS_NODE_ATTR, Integer.toString(20));
            Set<DiscoveryNodeRole> roles = new HashSet<>();
            roles.add(DiscoveryNodeRole.DATA_ROLE);
            roles.add(DiscoveryNodeRole.MASTER_ROLE);
            roles.add(DiscoveryNodeRole.INGEST_ROLE);
            nodesBuilder.add(new DiscoveryNode("ml-feature-set-given-ml-node-" + i,
                new TransportAddress(TransportAddress.META_ADDRESS, 9100 + i),
                attrs,
                roles,
                Version.CURRENT));
        }
        for (int i = 0; i < randomIntBetween(1, 3); i++) {
            Map<String, String> attrs = new HashMap<>();
            Set<DiscoveryNodeRole> roles = new HashSet<>();
            roles.add(DiscoveryNodeRole.DATA_ROLE);
            roles.add(DiscoveryNodeRole.MASTER_ROLE);
            roles.add(DiscoveryNodeRole.INGEST_ROLE);
            nodesBuilder.add(new DiscoveryNode("ml-feature-set-given-non-ml-node-" + i,
                new TransportAddress(TransportAddress.META_ADDRESS, 9300 + i),
                attrs,
                roles,
                Version.CURRENT));
        }
        return new ClusterState.Builder(ClusterState.EMPTY_STATE).nodes(nodesBuilder.build()).build();
    }

    private void givenDatafeeds(List<GetDatafeedsStatsAction.Response.DatafeedStats> datafeedStats) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetDatafeedsStatsAction.Response> listener =
                    (ActionListener<GetDatafeedsStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(new GetDatafeedsStatsAction.Response(
                    new QueryPage<>(datafeedStats, datafeedStats.size(), DatafeedConfig.RESULTS_FIELD)));
            return Void.TYPE;
        }).when(client).execute(same(GetDatafeedsStatsAction.INSTANCE), any(), any());
    }

    private void givenDataFrameAnalytics(List<DataFrameAnalyticsConfig> configs,
                                         List<GetDataFrameAnalyticsStatsAction.Response.Stats> stats) {
        assert configs.size() == stats.size();

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetDataFrameAnalyticsAction.Response> listener =
                (ActionListener<GetDataFrameAnalyticsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(new GetDataFrameAnalyticsAction.Response(
                new QueryPage<>(configs,
                    configs.size(),
                    GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)));
            return Void.TYPE;
        }).when(client).execute(same(GetDataFrameAnalyticsAction.INSTANCE), any(), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetDataFrameAnalyticsStatsAction.Response> listener =
                (ActionListener<GetDataFrameAnalyticsStatsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(new GetDataFrameAnalyticsStatsAction.Response(
                new QueryPage<>(stats,
                    stats.size(),
                    GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)));
            return Void.TYPE;
        }).when(client).execute(same(GetDataFrameAnalyticsStatsAction.INSTANCE), any(), any());
    }

    private void givenProcessorStats(List<NodeStats> stats) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<NodesStatsResponse> listener =
                (ActionListener<NodesStatsResponse>) invocationOnMock.getArguments()[2];
            listener.onResponse(new NodesStatsResponse(new ClusterName("_name"), stats, Collections.emptyList()));
            return Void.TYPE;
        }).when(client).execute(same(NodesStatsAction.INSTANCE), any(), any());
    }

    private void givenTrainedModels(List<TrainedModelConfig> trainedModels) {
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetTrainedModelsAction.Response> listener =
                (ActionListener<GetTrainedModelsAction.Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(new GetTrainedModelsAction.Response(
                new QueryPage<>(trainedModels,
                    trainedModels.size(),
                    GetDataFrameAnalyticsAction.Response.RESULTS_FIELD)));
            return Void.TYPE;
        }).when(client).execute(same(GetTrainedModelsAction.INSTANCE), any(), any());
    }

    private static Detector buildMinDetector(String fieldName) {
        Detector.Builder detectorBuilder = new Detector.Builder();
        detectorBuilder.setFunction("min");
        detectorBuilder.setFieldName(fieldName);
        return detectorBuilder.build();
    }

    private static Job buildJob(String jobId, List<Detector> detectors) {
        return buildJob(jobId, detectors, null);
    }

    private static Job buildJob(String jobId, List<Detector> detectors, Map<String, Object> customSettings) {
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(detectors);
        return new Job.Builder(jobId)
                .setAnalysisConfig(analysisConfig)
                .setDataDescription(new DataDescription.Builder())
                .setCustomSettings(customSettings)
                .build(new Date(randomNonNegativeLong()));
    }

    private static GetJobsStatsAction.Response.JobStats buildJobStats(String jobId, JobState state, long modelBytes,
            long numberOfForecasts) {
        ModelSizeStats.Builder modelSizeStats = new ModelSizeStats.Builder(jobId);
        modelSizeStats.setModelBytes(modelBytes);
        GetJobsStatsAction.Response.JobStats jobStats = mock(GetJobsStatsAction.Response.JobStats.class);
        ForecastStats forecastStats = buildForecastStats(numberOfForecasts);

        when(jobStats.getJobId()).thenReturn(jobId);
        when(jobStats.getModelSizeStats()).thenReturn(modelSizeStats.build());
        when(jobStats.getForecastStats()).thenReturn(forecastStats);
        when(jobStats.getState()).thenReturn(state);
        return jobStats;
    }

    private static GetDatafeedsStatsAction.Response.DatafeedStats buildDatafeedStats(DatafeedState state) {
        GetDatafeedsStatsAction.Response.DatafeedStats stats = mock(GetDatafeedsStatsAction.Response.DatafeedStats.class);
        when(stats.getDatafeedState()).thenReturn(state);
        return stats;
    }

    private static GetDataFrameAnalyticsStatsAction.Response.Stats buildDataFrameAnalyticsStats(String jobId,
            DataFrameAnalyticsState state, @Nullable Long peakUsageBytes) {
        GetDataFrameAnalyticsStatsAction.Response.Stats stats = mock(GetDataFrameAnalyticsStatsAction.Response.Stats.class);
        when(stats.getState()).thenReturn(state);
        if (peakUsageBytes != null) {
            when(stats.getMemoryUsage()).thenReturn(new MemoryUsage(jobId, Instant.now(), peakUsageBytes, null, null));
        }
        return stats;
    }

    private static NodeStats buildNodeStats(List<String> pipelineNames, List<List<IngestStats.ProcessorStat>> processorStats) {
        IngestStats ingestStats = new IngestStats(
            new IngestStats.Stats(0,0,0,0),
            Collections.emptyList(),
            IntStream.range(0, pipelineNames.size()).boxed().collect(Collectors.toMap(pipelineNames::get, processorStats::get)));
        return new NodeStats(mock(DiscoveryNode.class),
            Instant.now().toEpochMilli(), null, null, null, null, null, null, null, null,
            null, null, null, ingestStats, null, null);

    }

    private static ForecastStats buildForecastStats(long numberOfForecasts) {
        return new ForecastStatsTests().createForecastStats(numberOfForecasts, numberOfForecasts);
    }
}
