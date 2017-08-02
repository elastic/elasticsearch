/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.MlMetadata.Builder;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction.Request;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedConfig;
import static org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests.createDatafeedJob;
import static org.hamcrest.Matchers.equalTo;

public class StopDatafeedActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setStopTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setForce(randomBoolean());
        }
        if (randomBoolean()) {
            request.setAllowNoDatafeeds(randomBoolean());
        }
        return request;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

    public void testValidate() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlMetadata.datafeedTaskId("foo"), StartDatafeedAction.TASK_NAME,
                new StartDatafeedAction.DatafeedParams("foo", 0L), new Assignment("node_id", ""));
        tasksBuilder.updateTaskStatus(MlMetadata.datafeedTaskId("foo"), DatafeedState.STARTED);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        Job job = createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder().putJob(job, false).build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> StopDatafeedAction.validateDatafeedTask("foo", mlMetadata1));
        assertThat(e.getMessage(), equalTo("No datafeed with id [foo] exists"));

        DatafeedConfig datafeedConfig = createDatafeedConfig("foo", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder().putJob(job, false)
                .putDatafeed(datafeedConfig)
                .build();
        StopDatafeedAction.validateDatafeedTask("foo", mlMetadata2);
    }

    public void testResolveDataFeedIds_GivenDatafeedId() {
        Builder mlMetadataBuilder = new MlMetadata.Builder();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();

        addTask("datafeed_1", 0L, "node-1", DatafeedState.STARTED, tasksBuilder);
        Job job = BaseMlIntegTestCase.createScheduledJob("job_id_1").build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed_1", "job_id_1").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        addTask("datafeed_2", 0L, "node-1", DatafeedState.STOPPED, tasksBuilder);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_2").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_2", "job_id_2").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        MlMetadata mlMetadata = mlMetadataBuilder.build();

        List<String> startedDatafeeds = new ArrayList<>();
        List<String> stoppingDatafeeds = new ArrayList<>();
        StopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("datafeed_1"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Arrays.asList("datafeed_1"), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);

        startedDatafeeds.clear();
        stoppingDatafeeds.clear();
        StopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("datafeed_2"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Collections.emptyList(), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);
    }

    public void testResolveDataFeedIds_GivenAll() {
        Builder mlMetadataBuilder = new MlMetadata.Builder();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();

        addTask("datafeed_1", 0L, "node-1", DatafeedState.STARTED, tasksBuilder);
        Job job = BaseMlIntegTestCase.createScheduledJob("job_id_1").build(new Date());
        DatafeedConfig datafeedConfig = createDatafeedConfig("datafeed_1", "job_id_1").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        addTask("datafeed_2", 0L, "node-1", DatafeedState.STOPPED, tasksBuilder);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_2").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_2", "job_id_2").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        addTask("datafeed_3", 0L, "node-1", DatafeedState.STOPPING, tasksBuilder);
        job = BaseMlIntegTestCase.createScheduledJob("job_id_3").build(new Date());
        datafeedConfig = createDatafeedConfig("datafeed_3", "job_id_3").build();
        mlMetadataBuilder.putJob(job, false).putDatafeed(datafeedConfig);

        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        MlMetadata mlMetadata = mlMetadataBuilder.build();

        List<String> startedDatafeeds = new ArrayList<>();
        List<String> stoppingDatafeeds = new ArrayList<>();
        StopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("_all"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Arrays.asList("datafeed_1"), startedDatafeeds);
        assertEquals(Arrays.asList("datafeed_3"), stoppingDatafeeds);

        startedDatafeeds.clear();
        stoppingDatafeeds.clear();
        StopDatafeedAction.resolveDataFeedIds(new StopDatafeedAction.Request("datafeed_2"), mlMetadata, tasks, startedDatafeeds,
                stoppingDatafeeds);
        assertEquals(Collections.emptyList(), startedDatafeeds);
        assertEquals(Collections.emptyList(), stoppingDatafeeds);
    }

    public static void addTask(String datafeedId, long startTime, String nodeId, DatafeedState state,
                               PersistentTasksCustomMetaData.Builder taskBuilder) {
        taskBuilder.addTask(MlMetadata.datafeedTaskId(datafeedId), StartDatafeedAction.TASK_NAME,
                new StartDatafeedAction.DatafeedParams(datafeedId, startTime), new Assignment(nodeId, "test assignment"));
        taskBuilder.updateTaskStatus(MlMetadata.datafeedTaskId(datafeedId), state);
    }

}
