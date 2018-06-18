/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MLMetadataField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedConfig;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedJob;
import static org.hamcrest.Matchers.equalTo;

public class TransportStopDatafeedActionTests extends ESTestCase {
    public void testValidate() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MLMetadataField.datafeedTaskId("foo"), StartDatafeedAction.TASK_NAME,
                new StartDatafeedAction.DatafeedParams("foo", 0L), new PersistentTasksCustomMetaData.Assignment("node_id", ""));
        tasksBuilder.updateTaskState(MLMetadataField.datafeedTaskId("foo"), DatafeedState.STARTED);
        tasksBuilder.build();

        Job job = createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder().putJob(job, false).build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> TransportStopDatafeedAction.validateDatafeedTask("foo", mlMetadata1));
        assertThat(e.getMessage(), equalTo("No datafeed with id [foo] exists"));

        DatafeedConfig datafeedConfig = createDatafeedConfig("foo", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder().putJob(job, false)
                .putDatafeed(datafeedConfig, Collections.emptyMap())
                .build();
        TransportStopDatafeedAction.validateDatafeedTask("foo", mlMetadata2);
    }

    public void testResolveDataFeedIds_GivenDatafeedId() {
        MlMetadata.Builder mlMetadataBuilder = new MlMetadata.Builder();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();

        addTask("datafeed_1", 0L, "node-1", DatafeedState.STARTED, tasksBuilder);
        Job job = BaseMlIntegTestCase.createScheduledJob("job_id_1").build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed_1", "job_id_1").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig, Collections.emptyMap());

        addTask("datafeed_2", 0L, "node-1", DatafeedState.STOPPED, tasksBuilder);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_2").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_2", "job_id_2").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig, Collections.emptyMap());

        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        MlMetadata mlMetadata = mlMetadataBuilder.build();

        List<String> startedDatafeeds = new ArrayList<>();
        List<String> stoppingDatafeeds = new ArrayList<>();
        TransportStopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("datafeed_1"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Collections.singletonList("datafeed_1"), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);

        startedDatafeeds.clear();
        stoppingDatafeeds.clear();
        TransportStopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("datafeed_2"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Collections.emptyList(), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);
    }

    public void testResolveDataFeedIds_GivenAll() {
        MlMetadata.Builder mlMetadataBuilder = new MlMetadata.Builder();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();

        addTask("datafeed_1", 0L, "node-1", DatafeedState.STARTED, tasksBuilder);
        Job job = BaseMlIntegTestCase.createScheduledJob("job_id_1").build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed_1", "job_id_1").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig, Collections.emptyMap());

        addTask("datafeed_2", 0L, "node-1", DatafeedState.STOPPED, tasksBuilder);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_2").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_2", "job_id_2").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig, Collections.emptyMap());

        addTask("datafeed_3", 0L, "node-1", DatafeedState.STOPPING, tasksBuilder);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_3").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_3", "job_id_3").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig, Collections.emptyMap());

        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        MlMetadata mlMetadata = mlMetadataBuilder.build();

        List<String> startedDatafeeds = new ArrayList<>();
        List<String> stoppingDatafeeds = new ArrayList<>();
        TransportStopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("_all"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Collections.singletonList("datafeed_1"), startedDatafeeds);
        assertEquals(Collections.singletonList("datafeed_3"), stoppingDatafeeds);

        startedDatafeeds.clear();
        stoppingDatafeeds.clear();
        TransportStopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("datafeed_2"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Collections.emptyList(), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);
    }

    public static void addTask(String datafeedId, long startTime, String nodeId, DatafeedState state,
                               PersistentTasksCustomMetaData.Builder taskBuilder) {
        taskBuilder.addTask(MLMetadataField.datafeedTaskId(datafeedId), StartDatafeedAction.TASK_NAME,
                new StartDatafeedAction.DatafeedParams(datafeedId, startTime),
                new PersistentTasksCustomMetaData.Assignment(nodeId, "test assignment"));
        taskBuilder.updateTaskState(MLMetadataField.datafeedTaskId(datafeedId), state);
    }
}
