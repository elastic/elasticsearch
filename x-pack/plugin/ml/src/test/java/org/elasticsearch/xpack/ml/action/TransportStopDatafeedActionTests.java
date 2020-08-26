/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TransportStopDatafeedActionTests extends ESTestCase {
    public void testSortDatafeedIdsByTaskState_GivenDatafeedId() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();

        addTask("datafeed_1", 0L, "node-1", DatafeedState.STARTED, tasksBuilder);
        addTask("datafeed_2", 0L, "node-1", DatafeedState.STOPPED, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        List<String> startedDatafeeds = new ArrayList<>();
        List<String> stoppingDatafeeds = new ArrayList<>();
        List<String> notStoppedDatafeeds = new ArrayList<>();
        TransportStopDatafeedAction.sortDatafeedIdsByTaskState(
                Collections.singleton("datafeed_1"), tasks, startedDatafeeds, stoppingDatafeeds, notStoppedDatafeeds);
        assertEquals(Collections.singletonList("datafeed_1"), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);
        assertEquals(Collections.singletonList("datafeed_1"), notStoppedDatafeeds);

        startedDatafeeds.clear();
        stoppingDatafeeds.clear();
        notStoppedDatafeeds.clear();
        TransportStopDatafeedAction.sortDatafeedIdsByTaskState(
                Collections.singleton("datafeed_2"), tasks, startedDatafeeds, stoppingDatafeeds, notStoppedDatafeeds);
        assertEquals(Collections.emptyList(), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);
        assertEquals(Collections.emptyList(), notStoppedDatafeeds);
    }

    public void testSortDatafeedIdsByTaskState_GivenAll() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();

        addTask("datafeed_1", 0L, "node-1", DatafeedState.STARTED, tasksBuilder);
        addTask("datafeed_2", 0L, "node-1", DatafeedState.STOPPED, tasksBuilder);
        addTask("datafeed_3", 0L, "node-1", DatafeedState.STOPPING, tasksBuilder);
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        List<String> startedDatafeeds = new ArrayList<>();
        List<String> stoppingDatafeeds = new ArrayList<>();
        List<String> notStoppedDatafeeds = new ArrayList<>();
        TransportStopDatafeedAction.sortDatafeedIdsByTaskState(
                Arrays.asList("datafeed_1", "datafeed_2", "datafeed_3"), tasks, startedDatafeeds, stoppingDatafeeds, notStoppedDatafeeds);
        assertEquals(Collections.singletonList("datafeed_1"), startedDatafeeds);
        assertEquals(Collections.singletonList("datafeed_3"), stoppingDatafeeds);
        assertEquals(Arrays.asList("datafeed_1", "datafeed_3"), notStoppedDatafeeds);

        startedDatafeeds.clear();
        stoppingDatafeeds.clear();
        TransportStopDatafeedAction.sortDatafeedIdsByTaskState(Collections.singleton("datafeed_2"), tasks, startedDatafeeds,
                stoppingDatafeeds, notStoppedDatafeeds);
        assertEquals(Collections.emptyList(), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);
    }

    public static void addTask(String datafeedId, long startTime, String nodeId, DatafeedState state,
                               PersistentTasksCustomMetadata.Builder taskBuilder) {
        taskBuilder.addTask(MlTasks.datafeedTaskId(datafeedId), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(datafeedId, startTime),
                new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment"));
        taskBuilder.updateTaskState(MlTasks.datafeedTaskId(datafeedId), state);
    }
}
