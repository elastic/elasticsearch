/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryRequestBuilder;
import org.elasticsearch.xpack.core.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;
import org.elasticsearch.xpack.core.esql.action.internal.SharedSecrets;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.security.cloud.CloudCredentialManager;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.datafeed.TransportStartDatafeedAction;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractor;
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.datafeed.extractor.chunked.ChunkedDataExtractorFactory;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatafeedJobBuilderTests extends ESTestCase {

    private static final long HOUR_MS = TimeValue.timeValueHours(1).millis();
    private static final String AGGREGATING_ESQL_QUERY = "FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)";

    private Client client;
    private AnomalyDetectionAuditor auditor;
    private AnnotationPersister annotationPersister;
    private JobResultsPersister jobResultsPersister;
    private ClusterService clusterService;

    private DatafeedJobBuilder datafeedJobBuilder;

    @Before
    @SuppressWarnings("unchecked")
    public void init() {
        client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.settings()).thenReturn(Settings.EMPTY);
        auditor = mock(AnomalyDetectionAuditor.class);
        annotationPersister = mock(AnnotationPersister.class);
        jobResultsPersister = mock(JobResultsPersister.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            new HashSet<>(
                Arrays.asList(
                    MachineLearning.DELAYED_DATA_CHECK_FREQ,
                    MachineLearning.CCS_STABILIZATION_CYCLES,
                    MachineLearning.CCS_STABILIZATION_FLOOR,
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ClusterService.USER_DEFINED_METADATA,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_INTERVAL,
                    ClusterApplierService.CLUSTER_APPLIER_THREAD_WATCHDOG_QUIET_TIME
                )
            )
        );
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("test_node").roles(emptySet()).build();
        clusterService = new ClusterService(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test_node").build(),
            clusterSettings,
            threadPool,
            null
        );
        clusterService.getClusterApplierService()
            .setInitialState(
                ClusterState.builder(new ClusterName("DatafeedJobBuilderTests"))
                    .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId()))
                    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
                    .build()
            );

        datafeedJobBuilder = new DatafeedJobBuilder(
            client,
            xContentRegistry(),
            auditor,
            annotationPersister,
            System::currentTimeMillis,
            jobResultsPersister,
            Settings.EMPTY,
            clusterService,
            CloudCredentialManager.Noop::new,
            DatafeedSearchTelemetry.NOOP
        );
    }

    public void testBuild_GivenScrollDatafeedAndNewJob() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.isRunning(), is(true));
            assertThat(datafeedJob.isIsolated(), is(false));
            assertThat(datafeedJob.lastEndTimeMs(), is(nullValue()));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(null, null, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        buildDatafeedJob(newDatafeedTask("datafeed1"), datafeedContext, datafeedJobHandler);

        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    private void buildDatafeedJob(
        TransportStartDatafeedAction.DatafeedTask task,
        DatafeedContext context,
        ActionListener<DatafeedJob> listener
    ) {
        buildDatafeedJob(task, context, 0L, listener);
    }

    private void buildDatafeedJob(
        TransportStartDatafeedAction.DatafeedTask task,
        DatafeedContext context,
        long timeBucketMillis,
        ActionListener<DatafeedJob> listener
    ) {
        DataExtractorFactory dataExtractorFactory = mock(DataExtractorFactory.class);
        when(dataExtractorFactory.timeBucketMillis()).thenReturn(timeBucketMillis);
        datafeedJobBuilder.buildWithExtractorFactory(task, context, dataExtractorFactory, listener);
    }

    public void testBuild_GivenScrollDatafeedAndOldJobWithLatestRecordTimestampAfterLatestBucket() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.isRunning(), is(true));
            assertThat(datafeedJob.isIsolated(), is(false));
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(7_399_999L));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(3_800_000L, 7_200_000L, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        buildDatafeedJob(datafeedTask, datafeedContext, datafeedJobHandler);

        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testBuild_GivenScrollDatafeedAndOldJobWithLatestBucketAfterLatestRecordTimestamp() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.isRunning(), is(true));
            assertThat(datafeedJob.isIsolated(), is(false));
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(7_399_999L));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(3_800_000L, 3_600_000L, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        buildDatafeedJob(datafeedTask, datafeedContext, datafeedJobHandler);

        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testBuildGivenEsqlFactoryNotEmittingBucketTimesAndRestartShouldNotAdvanceLatestRecordTime() throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM logs");
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        long hour = TimeValue.timeValueHours(1).millis();
        long recordStamp = 9 * hour;

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(recordStamp));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(8 * hour, recordStamp, true),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        buildDatafeedJob(newDatafeedTask("datafeed1"), datafeedContext, datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testBuildGivenEsqlBucketShorterThanBucketSpanShouldResumeAfterTheDescribeBucket() throws Exception {
        long hour = TimeValue.timeValueHours(1).millis();
        long halfHour = TimeValue.timeValueMinutes(30).millis();
        assertEsqlRestartOffset(halfHour, 9 * hour + halfHour, 9 * hour + 2 * halfHour - 1);
    }

    public void testBuildGivenEsqlBucketLongerThanAnHourShouldResumeAfterTheDescribeBucket() throws Exception {
        long hour = TimeValue.timeValueHours(1).millis();
        assertEsqlRestartOffset(2 * hour, 10 * hour, 10 * hour + 2 * hour - 1);
    }

    public void testBuildGivenEsqlBucketAndNoFrequencyShouldScheduleOnMultipleOfDescribeBucket() throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 15 minutes)");
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            // a 15 minute frequency searches 96 times a day, the 10 minute default of a one hour job would search 144 times
            assertThat(datafeedJob.numberOfSearchesIn24Hours(), equalTo(96L));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(null, null, true),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        buildDatafeedJob(newDatafeedTask("datafeed1"), datafeedContext, TimeValue.timeValueMinutes(15).millis(), datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    private void assertEsqlRestartOffset(long timeBucketMillis, long recordStamp, long expectedLastEndTimeMs) throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)");
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(expectedLastEndTimeMs));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(recordStamp - TimeValue.timeValueHours(1).millis(), recordStamp, true),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        buildDatafeedJob(newDatafeedTask("datafeed1"), datafeedContext, timeBucketMillis, datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testBuildGivenAggregatingQueryTextButFactoryNotEmittingBucketTimesShouldNotAdvanceLatestRecordTime() throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)");
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        long hour = TimeValue.timeValueHours(1).millis();
        long recordStamp = 9 * hour;

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(recordStamp));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(8 * hour, recordStamp, true),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        buildDatafeedJob(newDatafeedTask("datafeed1"), datafeedContext, 0L, datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testBuildGivenAggregatingEsqlDatafeedAndRevertedSnapshotShouldResumeFromRevertedLatestRecord() throws Exception {
        // A revert with delete_intervening_results lowers both the latest final bucket and the latest record time.
        assertEsqlLastEndTime(AGGREGATING_ESQL_QUERY, new RestartTimeInfo(5 * HOUR_MS, 6 * HOUR_MS, true), 7 * HOUR_MS - 1, 7 * HOUR_MS);
    }

    public void testBuildGivenRawEsqlDatafeedAndRevertedSnapshotShouldResumeAtRevertedLatestRecord() throws Exception {
        long revertedRecord = 6 * HOUR_MS + TimeValue.timeValueMinutes(30).millis();
        assertEsqlLastEndTime("FROM logs", new RestartTimeInfo(5 * HOUR_MS, revertedRecord, true), revertedRecord, revertedRecord + 1);
    }

    public void testBuildGivenAggregatingEsqlDatafeedAndEndStampedLatestRecordShouldAdvancePastTheBucket() throws Exception {
        // The record closes the 9:00 bucket (bucket start + span - 1ms). The resume point is one ms before 11:00, and the extractor
        // factory floors the window start to 10:00, the first bucket that was not emitted yet.
        long endStampedRecord = 10 * HOUR_MS - 1;
        assertEsqlLastEndTime(
            AGGREGATING_ESQL_QUERY,
            new RestartTimeInfo(8 * HOUR_MS, endStampedRecord, true),
            11 * HOUR_MS - 2,
            10 * HOUR_MS
        );
    }

    public void testBuildGivenEsqlDatafeedAndFinalBucketEndAfterLatestRecordShouldResumeFromFinalBucketEnd() throws Exception {
        // The job's bucket span is one hour. Same rule as the DSL datafeed: the later of the two end times wins.
        long finalBucketEnd = 21 * HOUR_MS - 1;
        assertEsqlLastEndTime(AGGREGATING_ESQL_QUERY, new RestartTimeInfo(20 * HOUR_MS, 9 * HOUR_MS, true), finalBucketEnd, 21 * HOUR_MS);
        assertEsqlLastEndTime("FROM logs", new RestartTimeInfo(20 * HOUR_MS, 9 * HOUR_MS, true), finalBucketEnd, 21 * HOUR_MS);
    }

    public void testBuildGivenRawEsqlDatafeedAndRestartInsideBucketShouldResumeRightAfterLatestRecord() throws Exception {
        long latestRecord = 9 * HOUR_MS + TimeValue.timeValueMinutes(14).millis() + TimeValue.timeValueSeconds(53).millis();
        // The rows between the bucket start (09:00) and the latest record were already posted and must not be read again.
        assertEsqlLastEndTime("FROM logs", new RestartTimeInfo(8 * HOUR_MS, latestRecord, true), latestRecord, latestRecord + 1);
    }

    /**
     * Asserts the resume point the builder computes. It then runs lookback on the built job, over a
     * real {@link ChunkedDataExtractorFactory}, and asserts the start of the first query the chunked extractor issues.
     */
    private void assertEsqlLastEndTime(
        String esqlQuery,
        RestartTimeInfo restartTimeInfo,
        long expectedLastEndTimeMs,
        long expectedWindowStartMs
    ) throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery(esqlQuery);
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        DatafeedConfig datafeedConfig = datafeed.build();
        Job job = jobBuilder.build();
        DatafeedContext datafeedContext = new DatafeedContext(
            datafeedConfig,
            job,
            restartTimeInfo,
            new DatafeedTimingStats(jobBuilder.getId())
        );

        doAnswer(invocation -> {
            ActionListener<FlushJobAction.Response> listener = invocation.getArgument(2);
            listener.onResponse(new FlushJobAction.Response(true, Instant.now()));
            return null;
        }).when(client).execute(same(FlushJobAction.INSTANCE), any(FlushJobAction.Request.class), anyActionListener());

        DataExtractorFactory delegate = mock(DataExtractorFactory.class);
        // DESCRIBE reports a bucket interval only for the aggregating query, whose rows are stamped with the bucket start
        when(delegate.timeBucketMillis()).thenReturn(esqlQuery.equals(AGGREGATING_ESQL_QUERY) ? HOUR_MS : 0L);
        DataExtractor emptySummaryExtractor = mock(DataExtractor.class);
        when(emptySummaryExtractor.getSummary()).thenReturn(new DataExtractor.DataSummary(null, null, 0L));
        when(delegate.newExtractor(anyLong(), anyLong())).thenReturn(emptySummaryExtractor);
        DataExtractorFactory chunkedFactory = new ChunkedDataExtractorFactory(datafeedConfig, job, xContentRegistry(), delegate);

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(expectedLastEndTimeMs));
            // the empty-summary extractor yields no data, which ends the lookback after the first extractor query was issued
            expectThrows(
                DatafeedJob.EmptyDataCountException.class,
                () -> datafeedJob.runLookBack(0L, expectedLastEndTimeMs + 1 + TimeValue.timeValueHours(5).millis())
            );
            wasHandlerCalled.set(true);
        });
        datafeedJobBuilder.buildWithExtractorFactory(newDatafeedTask("datafeed1"), datafeedContext, chunkedFactory, datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
        verify(delegate).newExtractor(eq(expectedWindowStartMs), anyLong());
    }

    public void testBuildGivenEsqlDescribeFailureShouldPropagateIt() throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM logs");
        datafeed.setSourceTimeField("@timestamp");
        // Delayed-data check for ES|QL is wired separately; keep it off so this test covers routing only.
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        AtomicReference<Exception> failure = new AtomicReference<>();
        ActionListener<DatafeedJob> datafeedJobHandler = ActionListener.wrap(datafeedJob -> fail("expected start to fail"), failure::set);

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(null, null, true),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        withMissingEsqlSource(() -> datafeedJobBuilder.build(newDatafeedTask("datafeed1"), datafeedContext, datafeedJobHandler));
        assertBusy(() -> assertThat(failure.get(), notNullValue()));
        assertThat(failure.get(), instanceOf(IndexNotFoundException.class));
    }

    public void testBuildGivenScrollDatafeedAndRestartShouldNotAdvanceLatestRecordTime() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        long hour = TimeValue.timeValueHours(1).millis();
        long recordStamp = 9 * hour;
        long finalBucketStart = 8 * hour;

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(recordStamp));
            wasHandlerCalled.set(true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(finalBucketStart, recordStamp, true),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        buildDatafeedJob(newDatafeedTask("datafeed1"), datafeedContext, datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testBuildGivenRemoteIndicesButNoRemoteSearching() throws Exception {
        datafeedJobBuilder = new DatafeedJobBuilder(
            client,
            xContentRegistry(),
            auditor,
            annotationPersister,
            System::currentTimeMillis,
            jobResultsPersister,
            nonRemoteClusterClientNode(),
            clusterService,
            CloudCredentialManager.Noop::new,
            DatafeedSearchTelemetry.NOOP
        );
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());
        datafeed.setIndices(Collections.singletonList("remotecluster:index-*"));

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionListener.wrap(
            datafeedJob -> fail("datafeed builder did not fail when remote index was given and remote clusters were not enabled"),
            e -> {
                assertThat(
                    e.getMessage(),
                    equalTo(
                        Messages.getMessage(
                            Messages.DATAFEED_NEEDS_REMOTE_CLUSTER_SEARCH,
                            "datafeed1",
                            "[remotecluster:index-*]",
                            "test_node"
                        )
                    )
                );
                wasHandlerCalled.set(true);
            }
        );

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(null, null, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1", List.of("remotecluster:index-*"));

        buildDatafeedJob(datafeedTask, datafeedContext, datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testCheckRemoteIndicesAreAvailableGivenNullTaskIndicesShouldNotThrow() {
        datafeedJobBuilder = new DatafeedJobBuilder(
            client,
            xContentRegistry(),
            auditor,
            annotationPersister,
            System::currentTimeMillis,
            jobResultsPersister,
            nonRemoteClusterClientNode(),
            clusterService,
            CloudCredentialManager.Noop::new,
            DatafeedSearchTelemetry.NOOP
        );
        DatafeedConfig datafeed = new DatafeedConfig.Builder("datafeed1", "job1").setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .build();

        datafeedJobBuilder.checkRemoteIndicesAreAvailable(newDatafeedTask("datafeed1", null), datafeed);
    }

    public void testCheckRemoteIndicesAreAvailableGivenDslDatafeedShouldUseConfigIndicesNotTaskIndices() {
        datafeedJobBuilder = newBuilder(nonRemoteClusterClientNode());
        DatafeedConfig localDatafeed = new DatafeedConfig.Builder("datafeed1", "job1").setIndices(List.of("local-index")).build();
        DatafeedConfig remoteDatafeed = new DatafeedConfig.Builder("datafeed1", "job1").setIndices(List.of("east:logs-*")).build();

        // The task holds the indices the master read, which an update may have changed before the config was loaded.
        datafeedJobBuilder.checkRemoteIndicesAreAvailable(newDatafeedTask("datafeed1", List.of("east:logs-*")), localDatafeed);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> datafeedJobBuilder.checkRemoteIndicesAreAvailable(newDatafeedTask("datafeed1", List.of("local-index")), remoteDatafeed)
        );
        assertThat(e.getMessage(), containsString("east:logs-*"));
    }

    public void testBuildGivenEsqlRemoteSourceAndNoRemoteSearchingShouldFail() throws Exception {
        datafeedJobBuilder = newBuilder(nonRemoteClusterClientNode());

        AtomicReference<Exception> failure = new AtomicReference<>();
        datafeedJobBuilder.build(
            newDatafeedTask("datafeed1", List.of("east:logs-*")),
            newEsqlContext(),
            ActionListener.wrap(datafeedJob -> fail("expected the build to fail"), failure::set)
        );

        assertBusy(() -> assertThat(failure.get(), notNullValue()));
        assertThat(
            failure.get().getMessage(),
            equalTo(Messages.getMessage(Messages.DATAFEED_NEEDS_REMOTE_CLUSTER_SEARCH, "datafeed1", "[east:logs-*]", "test_node"))
        );
    }

    public void testBuildGivenEsqlLocalSourceAndNoRemoteSearchingShouldSucceed() throws Exception {
        datafeedJobBuilder = newBuilder(nonRemoteClusterClientNode());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        withEsqlDescribingDateColumn(
            "time",
            () -> datafeedJobBuilder.build(
                newDatafeedTask("datafeed1", List.of()),
                newEsqlContext(),
                ActionTestUtils.assertNoFailureListener(datafeedJob -> wasHandlerCalled.set(true))
            )
        );

        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testBuildGivenEsqlRemoteSourceAndRemoteSearchingShouldSucceed() throws Exception {
        datafeedJobBuilder = newBuilder(Settings.EMPTY);

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        withEsqlDescribingDateColumn(
            "time",
            () -> datafeedJobBuilder.build(
                newDatafeedTask("datafeed1", List.of("east:logs-*")),
                newEsqlContext(),
                ActionTestUtils.assertNoFailureListener(datafeedJob -> wasHandlerCalled.set(true))
            )
        );

        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    private DatafeedJobBuilder newBuilder(Settings settings) {
        return new DatafeedJobBuilder(
            client,
            xContentRegistry(),
            auditor,
            annotationPersister,
            System::currentTimeMillis,
            jobResultsPersister,
            settings,
            clusterService,
            CloudCredentialManager.Noop::new,
            DatafeedSearchTelemetry.NOOP
        );
    }

    private static DatafeedContext newEsqlContext() {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM east:logs-*");
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());
        return new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(null, null, true),
            new DatafeedTimingStats(jobBuilder.getId())
        );
    }

    /** Runs {@code body} with the ES|QL request builder stubbed so that every query is answered by {@code answer}. */
    @SuppressWarnings("unchecked")
    private static void withEsqlAnswer(Consumer<ActionListener<EsqlQueryResponse>> answer, Runnable body) {
        EsqlQueryRequestBuilder<EsqlQueryRequest, EsqlQueryResponse> builder = mock();
        when(builder.query(any())).thenReturn(builder);
        when(builder.allowPartialResults(anyBoolean())).thenReturn(builder);
        doAnswer(invocation -> {
            answer.accept((ActionListener<EsqlQueryResponse>) invocation.getArgument(0));
            return null;
        }).when(builder).execute(any(ActionListener.class));
        SharedSecrets.setEsqlQueryRequestBuilderAccess(client -> builder);
        try {
            body.run();
        } finally {
            SharedSecrets.setEsqlQueryRequestBuilderAccess(null);
        }
    }

    /** Runs {@code body} with every ES|QL query failing as an index that does not exist yet. */
    private static void withMissingEsqlSource(Runnable body) {
        withEsqlAnswer(listener -> listener.onFailure(new IndexNotFoundException("myIndex")), body);
    }

    /** Runs {@code body} with every ES|QL query answered as a DESCRIBE that reports {@code timeField} as a date column. */
    @SuppressWarnings("unchecked")
    private static void withEsqlDescribingDateColumn(String timeField, Runnable body) {
        ColumnInfo name = mock(ColumnInfo.class);
        when(name.name()).thenReturn("name");
        ColumnInfo type = mock(ColumnInfo.class);
        when(type.name()).thenReturn("type");
        EsqlResponse response = mock(EsqlResponse.class);
        doReturn(List.of(name, type)).when(response).columns();
        when(response.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) List.of(List.<Object>of(timeField, "date")));
        EsqlQueryResponse queryResponse = mock(EsqlQueryResponse.class);
        when(queryResponse.response()).thenReturn(response);
        withEsqlAnswer(listener -> listener.onResponse(queryResponse), body);
    }

    private static TransportStartDatafeedAction.DatafeedTask newDatafeedTask(String datafeedId) {
        return newDatafeedTask(datafeedId, List.of());
    }

    private static TransportStartDatafeedAction.DatafeedTask newDatafeedTask(String datafeedId, List<String> datafeedIndices) {
        TransportStartDatafeedAction.DatafeedTask task = mock(TransportStartDatafeedAction.DatafeedTask.class);
        when(task.getDatafeedId()).thenReturn(datafeedId);
        when(task.getDatafeedIndices()).thenReturn(datafeedIndices);
        TaskId parentTaskId = new TaskId("");
        when(task.getParentTaskId()).thenReturn(parentTaskId);
        return task;
    }
}
