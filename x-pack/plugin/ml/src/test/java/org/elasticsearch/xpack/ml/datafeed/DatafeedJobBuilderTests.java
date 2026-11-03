/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed;

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
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
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
import org.elasticsearch.xpack.ml.datafeed.extractor.DataExtractorFactory;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.RestartTimeInfo;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptySet;
import static org.elasticsearch.test.NodeRoles.nonRemoteClusterClientNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class DatafeedJobBuilderTests extends ESTestCase {

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
        datafeedJobBuilder.buildWithExtractorFactory(task, context, mock(DataExtractorFactory.class), listener);
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

    public void testBuildGivenRawEsqlDatafeedAndRestartShouldNotAdvanceLatestRecordTime() throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM logs");
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setGroupingInterval(TimeValue.timeValueHours(1));
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

    public void testBuildGivenAggregatingEsqlDatafeedAndRestartShouldAdvanceLatestRecordTimePastGroupingInterval() throws Exception {
        assumeTrue("Only relevant when the ES|QL datafeeds feature flag is on", MachineLearning.ESQL_DATAFEEDS_FEATURE_FLAG.isEnabled());
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = new DatafeedConfig.Builder("datafeed1", jobBuilder.getId());
        datafeed.setEsqlQuery("FROM logs | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 1 hour)");
        datafeed.setSourceTimeField("@timestamp");
        datafeed.setGroupingInterval(TimeValue.timeValueHours(1));
        datafeed.setDelayedDataCheckConfig(DelayedDataCheckConfig.disabledDelayedDataCheckConfig());

        long hour = TimeValue.timeValueHours(1).millis();
        long recordStamp = 9 * hour;

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(10 * hour - 1));
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

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        buildDatafeedJob(datafeedTask, datafeedContext, datafeedJobHandler);
        assertBusy(() -> assertTrue(wasHandlerCalled.get()));
    }

    public void testCheckRemoteIndicesAreAvailableGivenNullIndicesShouldNotThrow() {
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
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .build();
        DatafeedConfig datafeedWithoutIndices = spy(datafeed);
        doReturn(null).when(datafeedWithoutIndices).getIndices();

        datafeedJobBuilder.checkRemoteIndicesAreAvailable(datafeedWithoutIndices);
    }

    private static TransportStartDatafeedAction.DatafeedTask newDatafeedTask(String datafeedId) {
        TransportStartDatafeedAction.DatafeedTask task = mock(TransportStartDatafeedAction.DatafeedTask.class);
        when(task.getDatafeedId()).thenReturn(datafeedId);
        TaskId parentTaskId = new TaskId("");
        when(task.getParentTaskId()).thenReturn(parentTaskId);
        return task;
    }
}
