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
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedTimingStats;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.action.TransportStartDatafeedAction;
import org.elasticsearch.xpack.ml.annotations.AnnotationPersister;
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
import static org.mockito.Mockito.mock;
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
                    MasterService.MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING,
                    ClusterService.USER_DEFINED_METADATA,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING,
                    ClusterApplierService.CLUSTER_SERVICE_SLOW_TASK_THREAD_DUMP_TIMEOUT_SETTING
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
            clusterService
        );
    }

    public void testBuild_GivenScrollDatafeedAndNewJob() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.isRunning(), is(true));
            assertThat(datafeedJob.isIsolated(), is(false));
            assertThat(datafeedJob.lastEndTimeMs(), is(nullValue()));
            wasHandlerCalled.compareAndSet(false, true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(null, null, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        datafeedJobBuilder.build(datafeedTask, datafeedContext, datafeedJobHandler);

        assertBusy(() -> wasHandlerCalled.get());
    }

    public void testBuild_GivenScrollDatafeedAndOldJobWithLatestRecordTimestampAfterLatestBucket() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.isRunning(), is(true));
            assertThat(datafeedJob.isIsolated(), is(false));
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(7_200_000L));
            wasHandlerCalled.compareAndSet(false, true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(3_800_000L, 7_200_000L, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        datafeedJobBuilder.build(datafeedTask, datafeedContext, datafeedJobHandler);

        assertBusy(() -> wasHandlerCalled.get());
    }

    public void testBuild_GivenScrollDatafeedAndOldJobWithLatestBucketAfterLatestRecordTimestamp() throws Exception {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());

        AtomicBoolean wasHandlerCalled = new AtomicBoolean(false);
        ActionListener<DatafeedJob> datafeedJobHandler = ActionTestUtils.assertNoFailureListener(datafeedJob -> {
            assertThat(datafeedJob.isRunning(), is(true));
            assertThat(datafeedJob.isIsolated(), is(false));
            assertThat(datafeedJob.lastEndTimeMs(), equalTo(7_199_999L));
            wasHandlerCalled.compareAndSet(false, true);
        });

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(3_800_000L, 3_600_000L, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        datafeedJobBuilder.build(datafeedTask, datafeedContext, datafeedJobHandler);

        assertBusy(() -> wasHandlerCalled.get());
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
            clusterService
        );
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeField("time");
        Job.Builder jobBuilder = DatafeedRunnerTests.createDatafeedJob();
        jobBuilder.setDataDescription(dataDescription);
        jobBuilder.setCreateTime(new Date());
        DatafeedConfig.Builder datafeed = DatafeedRunnerTests.createDatafeedConfig("datafeed1", jobBuilder.getId());
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
                wasHandlerCalled.compareAndSet(false, true);
            }
        );

        DatafeedContext datafeedContext = new DatafeedContext(
            datafeed.build(),
            jobBuilder.build(),
            new RestartTimeInfo(null, null, false),
            new DatafeedTimingStats(jobBuilder.getId())
        );

        TransportStartDatafeedAction.DatafeedTask datafeedTask = newDatafeedTask("datafeed1");

        datafeedJobBuilder.build(datafeedTask, datafeedContext, datafeedJobHandler);
        assertBusy(() -> wasHandlerCalled.get());
    }

    private static TransportStartDatafeedAction.DatafeedTask newDatafeedTask(String datafeedId) {
        TransportStartDatafeedAction.DatafeedTask task = mock(TransportStartDatafeedAction.DatafeedTask.class);
        when(task.getDatafeedId()).thenReturn(datafeedId);
        TaskId parentTaskId = new TaskId("");
        when(task.getParentTaskId()).thenReturn(parentTaskId);
        return task;
    }
}
