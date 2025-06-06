/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.inference.assignment.AdaptiveAllocationsSettings;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.autoscaling.MlMemoryAutoscalingDeciderTests;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.util.Map;

import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MlMetricsTests extends ESTestCase {

    public void testFindTaskStatuses() {

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        MlMemoryAutoscalingDeciderTests.addJobTask("job1", "node1", JobState.OPENED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addJobTask("job2", "node1", JobState.OPENED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addJobTask("job3", "node2", JobState.FAILED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addJobTask("job4", null, JobState.OPENING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addJobTask("job5", "node1", JobState.CLOSING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addJobTask("job6", "node2", JobState.OPENED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addJobTask("job7", "node2", JobState.OPENING, tasksBuilder);
        addDatafeedTask("datafeed1", "node1", DatafeedState.STARTED, tasksBuilder);
        addDatafeedTask("datafeed2", "node1", DatafeedState.STARTED, tasksBuilder);
        addDatafeedTask("datafeed5", "node1", DatafeedState.STOPPING, tasksBuilder);
        addDatafeedTask("datafeed6", "node2", DatafeedState.STARTED, tasksBuilder);
        addDatafeedTask("datafeed7", "node2", DatafeedState.STARTING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa1", "node1", DataFrameAnalyticsState.STARTED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa2", "node2", DataFrameAnalyticsState.STARTED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa3", "node1", DataFrameAnalyticsState.FAILED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa4", "node2", DataFrameAnalyticsState.REINDEXING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa5", null, DataFrameAnalyticsState.STARTING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa6", "node1", DataFrameAnalyticsState.ANALYZING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa7", "node1", DataFrameAnalyticsState.STOPPING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa8", "node1", DataFrameAnalyticsState.STARTED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa9", null, DataFrameAnalyticsState.FAILED, tasksBuilder);

        MlMetrics.MlTaskStatusCounts counts = MlMetrics.findTaskStatuses(tasksBuilder.build());
        assertThat(counts.adOpeningCount(), is(2));
        assertThat(counts.adOpenedCount(), is(3));
        assertThat(counts.adClosingCount(), is(1));
        assertThat(counts.adFailedCount(), is(1));
        assertThat(counts.datafeedStartingCount(), is(1));
        assertThat(counts.datafeedStartedCount(), is(3));
        assertThat(counts.datafeedStoppingCount(), is(1));
        assertThat(counts.dfaStartingCount(), is(1));
        assertThat(counts.dfaStartedCount(), is(3));
        assertThat(counts.dfaReindexingCount(), is(1));
        assertThat(counts.dfaAnalyzingCount(), is(1));
        assertThat(counts.dfaStoppingCount(), is(1));
        assertThat(counts.dfaFailedCount(), is(2));
    }

    public void testFindDfaMemoryUsage() {

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa1", "node1", DataFrameAnalyticsState.STARTED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa2", "node2", DataFrameAnalyticsState.STARTED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa3", "node1", DataFrameAnalyticsState.FAILED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa4", "node2", DataFrameAnalyticsState.REINDEXING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa5", null, DataFrameAnalyticsState.STARTING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa6", "node1", DataFrameAnalyticsState.ANALYZING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa7", "node1", DataFrameAnalyticsState.STOPPING, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa8", "node1", DataFrameAnalyticsState.STARTED, tasksBuilder);
        MlMemoryAutoscalingDeciderTests.addAnalyticsTask("dfa9", null, DataFrameAnalyticsState.FAILED, tasksBuilder);

        DataFrameAnalyticsManager dfaManager = new DataFrameAnalyticsManager(
            Settings.EMPTY,
            mock(NodeClient.class),
            mock(ThreadPool.class),
            mock(ClusterService.class),
            mock(DataFrameAnalyticsConfigProvider.class),
            mock(AnalyticsProcessManager.class),
            mock(DataFrameAnalyticsAuditor.class),
            mock(IndexNameExpressionResolver.class),
            mock(ResultsPersisterService.class),
            mock(ModelLoadingService.class),
            new String[] {},
            Map.of(
                "dfa1",
                ByteSizeValue.ofGb(1),
                "dfa3",
                ByteSizeValue.ofGb(2),
                "dfa6",
                ByteSizeValue.ofGb(4),
                "dfa7",
                ByteSizeValue.ofGb(8),
                "dfa8",
                ByteSizeValue.ofGb(16)
            )
        );

        long bytesUsed = MlMetrics.findDfaMemoryUsage(dfaManager, tasksBuilder.build());
        assertThat(bytesUsed, is(ByteSizeValue.ofGb(29).getBytes() + 4 * DataFrameAnalyticsConfig.PROCESS_MEMORY_OVERHEAD.getBytes()));
    }

    public void testFindTrainedModelAllocationCounts() {

        TrainedModelAssignmentMetadata.Builder metadataBuilder = TrainedModelAssignmentMetadata.Builder.empty();
        metadataBuilder.addNewAssignment(
            "model1",
            TrainedModelAssignment.Builder.empty(mock(StartTrainedModelDeploymentAction.TaskParams.class), null)
                .addRoutingEntry("node1", new RoutingInfo(1, 1, RoutingState.STARTED, ""))
                .addRoutingEntry("node2", new RoutingInfo(0, 1, RoutingState.FAILED, ""))
        );
        metadataBuilder.addNewAssignment(
            "model2",
            TrainedModelAssignment.Builder.empty(mock(StartTrainedModelDeploymentAction.TaskParams.class), null)
                .addRoutingEntry("node1", new RoutingInfo(2, 2, RoutingState.STARTED, ""))
        );
        metadataBuilder.addNewAssignment(
            "model3",
            TrainedModelAssignment.Builder.empty(mock(StartTrainedModelDeploymentAction.TaskParams.class), null)
                .addRoutingEntry("node2", new RoutingInfo(0, 1, RoutingState.STARTING, ""))
        );
        metadataBuilder.addNewAssignment(
            "model4",
            TrainedModelAssignment.Builder.empty(
                mock(StartTrainedModelDeploymentAction.TaskParams.class),
                new AdaptiveAllocationsSettings(true, 0, 1)
            ).addRoutingEntry("node1", new RoutingInfo(0, 0, RoutingState.STARTING, ""))
        );
        metadataBuilder.addNewAssignment(
            "model5",
            TrainedModelAssignment.Builder.empty(
                mock(StartTrainedModelDeploymentAction.TaskParams.class),
                new AdaptiveAllocationsSettings(false, 1, 1)
            ).addRoutingEntry("node1", new RoutingInfo(1, 1, RoutingState.STARTING, ""))
        );

        MlMetrics.TrainedModelAllocationCounts counts = MlMetrics.findTrainedModelAllocationCounts(metadataBuilder.build());
        assertThat(counts.trainedModelsTargetAllocations(), is(6));
        assertThat(counts.trainedModelsCurrentAllocations(), is(4));
        assertThat(counts.trainedModelsFailedAllocations(), is(1));
        assertThat(counts.trainedModelsFixedAllocations(), is(3));
        assertThat(counts.trainedModelsDisabledAdaptiveAllocations(), is(1));
    }

    public void testFindNativeMemoryFree() {

        long bytesFree = MlMetrics.findNativeMemoryFree(
            ByteSizeValue.ofMb(4000).getBytes(),
            ByteSizeValue.ofMb(500).getBytes(),
            ByteSizeValue.ofMb(1000).getBytes(),
            ByteSizeValue.ofMb(2000).getBytes()
        );
        assertThat(bytesFree, is(ByteSizeValue.ofMb(500).getBytes() - NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()));
    }

    public static void addDatafeedTask(
        String datafeedId,
        String nodeId,
        DatafeedState datafeedState,
        PersistentTasksCustomMetadata.Builder builder
    ) {
        builder.addTask(
            MlTasks.datafeedTaskId(datafeedId),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams(datafeedId, System.currentTimeMillis()),
            nodeId == null ? AWAITING_LAZY_ASSIGNMENT : new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment")
        );
        if (datafeedState != null) {
            builder.updateTaskState(MlTasks.datafeedTaskId(datafeedId), datafeedState);
        }
    }
}
