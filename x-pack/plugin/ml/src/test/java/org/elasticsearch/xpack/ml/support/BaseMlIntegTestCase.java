/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.support;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.ingest.common.IngestCommonPlugin;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.script.IngestScript;
import org.elasticsearch.script.MockDeterministicScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockHttpTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ilm.IndexLifecycle;
import org.elasticsearch.xpack.ml.LocalStateMachineLearning;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.monitoring.MonitoringService;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A base class for testing datafeed and job lifecycle specifics.
 *
 * Note for other type of integration tests you should use the external test cluster created by the Gradle integTest task.
 * For example tests extending this base class test with the non native autodetect process.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false)
public abstract class BaseMlIntegTestCase extends ESIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false);
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true);
        settings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        settings.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        settings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        settings.put(XPackSettings.GRAPH_ENABLED.getKey(), false);
        settings.put(MonitoringService.ENABLED.getKey(), false);
        settings.put(MonitoringService.ELASTICSEARCH_COLLECTION_ENABLED.getKey(), false);
        settings.put(LifecycleSettings.LIFECYCLE_HISTORY_INDEX_ENABLED_SETTING.getKey(), false);
        return settings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            LocalStateMachineLearning.class,
            CommonAnalysisPlugin.class,
            IngestCommonPlugin.class,
            ReindexPlugin.class,
            ShutdownPlugin.class,
            // To remove warnings about painless not being supported
            MockPainlessScriptEngine.TestPlugin.class,
            // ILM is required for .ml-state template index settings
            IndexLifecycle.class,
            // Deprecation warnings go to a data stream, if we ever cause a deprecation warning the data streams plugin is required
            DataStreamsPlugin.class,
            // To remove errors from parsing build in templates that contain scaled_float
            MapperExtrasPlugin.class
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return Arrays.asList(TestSeedPlugin.class, MockHttpTransport.TestPlugin.class);
    }

    @Before
    public void ensureTemplatesArePresent() throws Exception {
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue("Timed out waiting for the ML templates to be installed", MachineLearning.criticalTemplatesInstalled(state));
        }, 20, TimeUnit.SECONDS);
    }

    protected Job.Builder createJob(String id) {
        return createJob(id, null);
    }

    protected Job.Builder createJob(String id, ByteSizeValue modelMemoryLimit) {
        return createJob(id, modelMemoryLimit, false);
    }

    protected Job.Builder createJob(String id, ByteSizeValue modelMemoryLimit, boolean allowLazyOpen) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId(id);
        if (modelMemoryLimit != null) {
            builder.setAnalysisLimits(new AnalysisLimits(modelMemoryLimit.getMb(), null));
        }
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        builder.setAllowLazyOpen(allowLazyOpen);
        return builder;
    }

    public static Job.Builder createFareQuoteJob(String id) {
        return createFareQuoteJob(id, null);
    }

    public static Job.Builder createFareQuoteJob(String id, ByteSizeValue modelMemoryLimit) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat(DataDescription.EPOCH);
        dataDescription.setTimeField("time");

        Detector.Builder d = new Detector.Builder("metric", "responsetime");
        d.setByFieldName("by_field_name");
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));

        Job.Builder builder = new Job.Builder();
        builder.setId(id);
        if (modelMemoryLimit != null) {
            builder.setAnalysisLimits(new AnalysisLimits(modelMemoryLimit.getMb(), null));
        }
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }

    public static Job.Builder createScheduledJob(String jobId) {
        return createScheduledJob(jobId, null);
    }

    public static Job.Builder createScheduledJob(String jobId, ByteSizeValue modelMemoryLimit) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat("yyyy-MM-dd HH:mm:ss");

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));
        analysisConfig.setBucketSpan(TimeValue.timeValueHours(1));

        Job.Builder builder = new Job.Builder();
        builder.setId(jobId);
        if (modelMemoryLimit != null) {
            builder.setAnalysisLimits(new AnalysisLimits(modelMemoryLimit.getMb(), null));
        }
        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }

    public static DatafeedConfig createDatafeed(String datafeedId, String jobId, List<String> indices) {
        return createDatafeed(datafeedId, jobId, indices, TimeValue.timeValueSeconds(1));
    }

    public static DatafeedConfig createDatafeed(String datafeedId, String jobId, List<String> indices, TimeValue frequency) {
        return createDatafeedBuilder(datafeedId, jobId, indices, frequency).build();
    }

    public static DatafeedConfig.Builder createDatafeedBuilder(String datafeedId, String jobId, List<String> indices) {
        return createDatafeedBuilder(datafeedId, jobId, indices, TimeValue.timeValueSeconds(1));
    }

    public static DatafeedConfig.Builder createDatafeedBuilder(String datafeedId, String jobId, List<String> indices, TimeValue frequency) {
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder(datafeedId, jobId);
        builder.setQueryDelay(TimeValue.timeValueSeconds(1));
        builder.setFrequency(frequency);
        builder.setIndices(indices);
        return builder;
    }

    @After
    public void cleanup() throws Exception {
        logger.info("[{}#{}]: Cleaning up datafeeds and jobs after test", getTestClass().getSimpleName(), getTestName());
        deleteAllDatafeeds(logger, client());
        deleteAllJobs(logger, client());
        deleteAllDataFrameAnalytics(client());
        waitForPendingTasks(client());
        assertBusy(() -> {
            RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries().setActiveOnly(true).get();
            for (List<RecoveryState> recoveryStates : recoveryResponse.shardRecoveryStates().values()) {
                assertThat(recoveryStates.size(), equalTo(0));
            }
        });
    }

    protected static ThreadPool mockThreadPool() {
        ThreadPool tp = mock(ThreadPool.class);
        ExecutorService executor = mock(ExecutorService.class);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[0]).run();
            return null;
        }).when(executor).execute(any(Runnable.class));
        when(tp.executor(any(String.class))).thenReturn(executor);
        doAnswer(invocationOnMock -> {
            ((Runnable) invocationOnMock.getArguments()[0]).run();
            return null;
        }).when(tp).schedule(any(Runnable.class), any(TimeValue.class), any(String.class));
        return tp;
    }

    public static void indexDocs(Logger logger, String index, long numDocs, long start, long end) {
        indexDocs(client(), logger, index, numDocs, start, end);
    }

    public static void indexDocs(Client client, Logger logger, String index, long numDocs, long start, long end) {
        int maxDelta = (int) (end - start - 1);
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            IndexRequest indexRequest = new IndexRequest(index);
            long timestamp = start + randomIntBetween(0, maxDelta);
            assert timestamp >= start && timestamp < end;
            indexRequest.source("time", timestamp, "@timestamp", timestamp).opType(DocWriteRequest.OpType.CREATE);
            bulkRequestBuilder.add(indexRequest);
        }
        BulkResponse bulkResponse = bulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        if (bulkResponse.hasFailures()) {
            int failures = 0;
            for (BulkItemResponse itemResponse : bulkResponse) {
                if (itemResponse.isFailed()) {
                    failures++;
                    logger.error("Item response failure [{}]", itemResponse.getFailureMessage());
                }
            }
            fail("Bulk response contained " + failures + " failures");
        }
        logger.info("Indexed [{}] documents", numDocs);
    }

    public static GetJobsStatsAction.Response.JobStats getJobStats(String jobId) {
        GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).actionGet();
        if (response.getResponse().results().isEmpty()) {
            return null;
        } else {
            return response.getResponse().results().get(0);
        }
    }

    public static DataCounts getDataCounts(String jobId) {
        GetJobsStatsAction.Response.JobStats jobStats = getJobStats(jobId);
        if (jobStats != null) {
            return jobStats.getDataCounts();
        } else {
            return new DataCounts(jobId);
        }
    }

    public static GetDatafeedsStatsAction.Response.DatafeedStats getDatafeedStats(String datafeedId) {
        GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
        GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
        if (response.getResponse().results().isEmpty()) {
            return null;
        } else {
            return response.getResponse().results().get(0);
        }
    }

    public static void deleteAllDatafeeds(Logger logger, Client client) throws Exception {
        final QueryPage<DatafeedConfig> datafeeds = client.execute(
            GetDatafeedsAction.INSTANCE,
            new GetDatafeedsAction.Request(GetDatafeedsAction.ALL)
        ).actionGet().getResponse();
        try {
            logger.info("Stopping all datafeeds (using _all)");
            StopDatafeedAction.Response stopResponse = client.execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request("_all"))
                .get();
            assertTrue(stopResponse.isStopped());
        } catch (ExecutionException e1) {
            try {
                StopDatafeedAction.Request request = new StopDatafeedAction.Request("_all");
                request.setForce(true);
                StopDatafeedAction.Response stopResponse = client.execute(StopDatafeedAction.INSTANCE, request).get();
                assertTrue(stopResponse.isStopped());
            } catch (ExecutionException e2) {
                logger.warn("Force-stopping datafeed with _all failed.", e2);
            }
            throw new RuntimeException("Had to resort to force-stopping datafeed, something went wrong?", e1);
        }

        for (final DatafeedConfig datafeed : datafeeds.results()) {
            assertBusy(() -> {
                try {
                    GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeed.getId());
                    GetDatafeedsStatsAction.Response r = client.execute(GetDatafeedsStatsAction.INSTANCE, request).get();
                    assertThat(r.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            AcknowledgedResponse deleteResponse = client.execute(
                DeleteDatafeedAction.INSTANCE,
                new DeleteDatafeedAction.Request(datafeed.getId())
            ).get();
            assertTrue(deleteResponse.isAcknowledged());
        }
    }

    public static void deleteAllJobs(Logger logger, Client client) throws Exception {
        final QueryPage<Job> jobs = client.execute(GetJobsAction.INSTANCE, new GetJobsAction.Request(Metadata.ALL))
            .actionGet()
            .getResponse();

        try {
            CloseJobAction.Request closeRequest = new CloseJobAction.Request(Metadata.ALL);
            // This usually takes a lot less than 90 seconds, but has been observed to be very slow occasionally
            // in CI and a 90 second timeout will avoid the cost of investigating these intermittent failures.
            // See https://github.com/elastic/elasticsearch/issues/48511
            closeRequest.setCloseTimeout(TimeValue.timeValueSeconds(90L));
            logger.info("Closing jobs using [{}]", Metadata.ALL);
            CloseJobAction.Response response = client.execute(CloseJobAction.INSTANCE, closeRequest).get();
            assertTrue(response.isClosed());
        } catch (Exception e1) {
            try {
                CloseJobAction.Request closeRequest = new CloseJobAction.Request(Metadata.ALL);
                closeRequest.setForce(true);
                closeRequest.setCloseTimeout(TimeValue.timeValueSeconds(30L));
                CloseJobAction.Response response = client.execute(CloseJobAction.INSTANCE, closeRequest).get();
                assertTrue(response.isClosed());
            } catch (Exception e2) {
                logger.warn("Force-closing jobs failed.", e2);
            }
            throw new RuntimeException("Had to resort to force-closing job, something went wrong?", e1);
        }

        for (final Job job : jobs.results()) {
            assertBusy(() -> {
                GetJobsStatsAction.Response statsResponse = client.execute(
                    GetJobsStatsAction.INSTANCE,
                    new GetJobsStatsAction.Request(job.getId())
                ).actionGet();
                assertEquals(JobState.CLOSED, statsResponse.getResponse().results().get(0).getState());
            });
            AcknowledgedResponse response = client.execute(DeleteJobAction.INSTANCE, new DeleteJobAction.Request(job.getId())).get();
            assertTrue(response.isAcknowledged());
        }
    }

    public static void deleteAllDataFrameAnalytics(Client client) throws Exception {
        final QueryPage<DataFrameAnalyticsConfig> analytics = client.execute(
            GetDataFrameAnalyticsAction.INSTANCE,
            new GetDataFrameAnalyticsAction.Request("_all")
        ).get().getResources();

        assertBusy(() -> {
            GetDataFrameAnalyticsStatsAction.Response statsResponse = client.execute(
                GetDataFrameAnalyticsStatsAction.INSTANCE,
                new GetDataFrameAnalyticsStatsAction.Request("_all")
            ).get();
            assertTrue(statsResponse.getResponse().results().stream().allMatch(s -> s.getState().equals(DataFrameAnalyticsState.STOPPED)));
        });
        for (final DataFrameAnalyticsConfig config : analytics.results()) {
            client.execute(DeleteDataFrameAnalyticsAction.INSTANCE, new DeleteDataFrameAnalyticsAction.Request(config.getId())).actionGet();
        }
    }

    public static void waitForPendingTasks(Client client) throws Exception {
        ListTasksRequest request = new ListTasksRequest().setDetailed(true);

        assertBusy(() -> {
            ListTasksResponse response = client.execute(ListTasksAction.INSTANCE, request).get();
            List<String> activeTasks = response.getTasks()
                .stream()
                .filter(t -> t.action().startsWith(ListTasksAction.NAME) == false)
                .map(TaskInfo::toString)
                .collect(Collectors.toList());
            assertThat(activeTasks, empty());
        });
    }

    protected static <T> void blockingCall(
        Consumer<ActionListener<T>> function,
        AtomicReference<T> response,
        AtomicReference<Exception> error
    ) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(r -> {
            response.set(r);
            latch.countDown();
        }, e -> {
            error.set(e);
            latch.countDown();
        });

        function.accept(listener);
        latch.await();
    }

    protected String awaitJobOpenedAndAssigned(String jobId, String queryNode) throws Exception {

        PersistentTasksClusterService persistentTasksClusterService = internalCluster().getInstance(
            PersistentTasksClusterService.class,
            internalCluster().getMasterName(queryNode)
        );
        // Speed up rechecks to a rate that is quicker than what settings would allow.
        // The check would work eventually without doing this, but the assertBusy() below
        // would need to wait 30 seconds, which would make the test run very slowly.
        // The 1 second refresh puts a greater burden on the master node to recheck
        // persistent tasks, but it will cope in these tests as it's not doing much
        // else.
        persistentTasksClusterService.setRecheckInterval(TimeValue.timeValueSeconds(1));

        AtomicReference<String> jobNode = new AtomicReference<>();
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse = client(queryNode).execute(
                GetJobsStatsAction.INSTANCE,
                new GetJobsStatsAction.Request(jobId)
            ).actionGet();
            GetJobsStatsAction.Response.JobStats jobStats = statsResponse.getResponse().results().get(0);
            assertEquals(JobState.OPENED, jobStats.getState());
            assertNotNull(jobStats.getNode());
            jobNode.set(jobStats.getNode().getName());
        });
        return jobNode.get();
    }

    /**
     * Sets delayed allocation to 0 to make sure we have tests are not delayed
     */
    protected void setMlIndicesDelayedNodeLeftTimeoutToZero() {
        OriginSettingClient originSettingClient = new OriginSettingClient(client(), ClientHelper.ML_ORIGIN);
        originSettingClient.admin()
            .indices()
            .updateSettings(
                new UpdateSettingsRequest(".ml-*").origin(ClientHelper.ML_ORIGIN)
                    .settings(Settings.builder().put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0).build())
            )
            .actionGet();
    }

    protected void ensureStableCluster() {
        ensureStableCluster(internalCluster().getNodeNames().length, TimeValue.timeValueSeconds(60));
    }

    public static class MockPainlessScriptEngine extends MockScriptEngine {

        public static final String NAME = "painless";

        public static class TestPlugin extends MockScriptPlugin {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new MlSingleNodeTestCase.MockPainlessScriptEngine();
            }

            @Override
            protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
                return Collections.emptyMap();
            }
        }

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public <T> T compile(String name, String script, ScriptContext<T> context, Map<String, String> options) {
            if (context.instanceClazz.equals(ScoreScript.class)) {
                return context.factoryClazz.cast(new MockScoreScript(MockDeterministicScript.asDeterministic(p -> 0.0)));
            }
            if (context.name.equals("ingest")) {
                IngestScript.Factory factory = vars -> new IngestScript(vars) {
                    @Override
                    public void execute(Map<String, Object> ctx) {}
                };
                return context.factoryClazz.cast(factory);
            }
            throw new IllegalArgumentException("mock painless does not know how to handle context [" + context.name + "]");
        }
    }
}
