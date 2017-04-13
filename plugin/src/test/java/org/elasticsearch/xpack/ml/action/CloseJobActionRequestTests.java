/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.CloseJobAction.Request;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.addJobTask;

public class CloseJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setCloseTimeout(TimeValue.timeValueMillis(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            request.setForce(randomBoolean());
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(null, parser);
    }

    public void testValidate() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createScheduledJob("job_id").build(new Date()), false);
        mlBuilder.putDatafeed(BaseMlIntegTestCase.createDatafeed("datafeed_id", "job_id",
                Collections.singletonList("*")));
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, JobState.OPENED, tasksBuilder);
        addTask("datafeed_id", 0L, null, DatafeedState.STARTED, tasksBuilder);
        ClusterState cs1 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())).build();

        ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class,
                        () -> CloseJobAction.validateAndReturnJobTask("job_id", cs1));
        assertEquals(RestStatus.CONFLICT, e.status());
        assertEquals("cannot close job [job_id], datafeed hasn't been stopped", e.getMessage());

        tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", null, JobState.OPENED, tasksBuilder);
        if (randomBoolean()) {
            addTask("datafeed_id", 0L, null, DatafeedState.STOPPED, tasksBuilder);
        }
        ClusterState cs2 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())).build();
        CloseJobAction.validateAndReturnJobTask("job_id", cs2);
    }

    public void testResolve() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createScheduledJob("job_id_1").build(new Date()),
                false);
        mlBuilder.putDatafeed(BaseMlIntegTestCase.createDatafeed("datafeed_id_1", "job_id_1",
                Collections.singletonList("*")));

        mlBuilder.putJob(BaseMlIntegTestCase.createScheduledJob("job_id_2").build(new Date()),
                false);
        mlBuilder.putDatafeed(BaseMlIntegTestCase.createDatafeed("datafeed_id_2", "job_id_2",
                Collections.singletonList("*")));

        mlBuilder.putJob(BaseMlIntegTestCase.createScheduledJob("job_id_3").build(new Date()),
                false);
        mlBuilder.putDatafeed(BaseMlIntegTestCase.createDatafeed("datafeed_id_3", "job_id_3",
                Collections.singletonList("*")));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job_id_1", null, JobState.OPENED, tasksBuilder);
        addJobTask("job_id_2", null, JobState.CLOSED, tasksBuilder);
        addJobTask("job_id_3", null, JobState.FAILED, tasksBuilder);

        ClusterState cs1 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE,  tasksBuilder.build()))
                .build();

        assertEquals(Arrays.asList("job_id_1", "job_id_3"),
                CloseJobAction.resolveAndValidateJobId("_all", cs1));
    }

    public static void addTask(String datafeedId, long startTime, String nodeId, DatafeedState state,
                               PersistentTasksCustomMetaData.Builder tasks) {
        tasks.addTask(MlMetadata.datafeedTaskId(datafeedId), StartDatafeedAction.TASK_NAME,
                new StartDatafeedAction.DatafeedParams(datafeedId, startTime), new Assignment(nodeId, "test assignment"));
        tasks.updateTaskStatus(MlMetadata.datafeedTaskId(datafeedId), state);
    }

}