/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class TransportStopDataFrameAnalyticsActionTests extends ESTestCase {

    public void testFindAnalyticsToStop_GivenOneFailedTaskAndNotForce() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addAnalyticsTask(tasksBuilder, "started", "foo-node", DataFrameAnalyticsState.STARTED);
        addAnalyticsTask(tasksBuilder, "reindexing", "foo-node", DataFrameAnalyticsState.REINDEXING);
        addAnalyticsTask(tasksBuilder, "analyzing", "foo-node", DataFrameAnalyticsState.ANALYZING);
        addAnalyticsTask(tasksBuilder, "stopping", "foo-node", DataFrameAnalyticsState.STOPPING);
        addAnalyticsTask(tasksBuilder, "stopped", "foo-node", DataFrameAnalyticsState.STOPPED);
        addAnalyticsTask(tasksBuilder, "failed", "foo-node", DataFrameAnalyticsState.FAILED);

        Set<String> ids = new HashSet<>(Arrays.asList("started", "reindexing", "analyzing", "stopping", "stopped", "failed"));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> TransportStopDataFrameAnalyticsAction.findAnalyticsToStop(tasksBuilder.build(), ids, false));

        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        assertThat(e.getMessage(), equalTo("cannot close data frame analytics [failed] because it failed, use force stop instead"));
    }

    public void testFindAnalyticsToStop_GivenTwoFailedTasksAndNotForce() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addAnalyticsTask(tasksBuilder, "failed", "foo-node", DataFrameAnalyticsState.FAILED);
        addAnalyticsTask(tasksBuilder, "another_failed", "foo-node", DataFrameAnalyticsState.FAILED);

        Set<String> ids = new HashSet<>(Arrays.asList("failed", "another_failed"));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
            () -> TransportStopDataFrameAnalyticsAction.findAnalyticsToStop(tasksBuilder.build(), ids, false));

        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        assertThat(e.getMessage(), equalTo("one or more data frame analytics are in failed state, use force stop instead"));
    }

    public void testFindAnalyticsToStop_GivenFailedTaskAndForce() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addAnalyticsTask(tasksBuilder, "started", "foo-node", DataFrameAnalyticsState.STARTED);
        addAnalyticsTask(tasksBuilder, "reindexing", "foo-node", DataFrameAnalyticsState.REINDEXING);
        addAnalyticsTask(tasksBuilder, "analyzing", "foo-node", DataFrameAnalyticsState.ANALYZING);
        addAnalyticsTask(tasksBuilder, "stopping", "foo-node", DataFrameAnalyticsState.STOPPING);
        addAnalyticsTask(tasksBuilder, "stopped", "foo-node", DataFrameAnalyticsState.STOPPED);
        addAnalyticsTask(tasksBuilder, "failed", "foo-node", DataFrameAnalyticsState.FAILED);

        Set<String> ids = new HashSet<>(Arrays.asList("started", "reindexing", "analyzing", "stopping", "stopped", "failed"));

        Set<String> analyticsToStop = TransportStopDataFrameAnalyticsAction.findAnalyticsToStop(tasksBuilder.build(), ids, true);

        assertThat(analyticsToStop, containsInAnyOrder("started", "reindexing", "analyzing", "failed"));
    }

    private static void addAnalyticsTask(PersistentTasksCustomMetaData.Builder builder, String analyticsId, String nodeId,
                                         DataFrameAnalyticsState state) {
        builder.addTask(MlTasks.dataFrameAnalyticsTaskId(analyticsId), MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams(analyticsId, Version.CURRENT),
            new PersistentTasksCustomMetaData.Assignment(nodeId, "test assignment"));

        builder.updateTaskState(MlTasks.dataFrameAnalyticsTaskId(analyticsId),
            new DataFrameAnalyticsTaskState(state, builder.getLastAllocationId(), null));

    }
}
