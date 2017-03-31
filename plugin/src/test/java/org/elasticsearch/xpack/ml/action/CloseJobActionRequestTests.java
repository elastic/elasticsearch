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
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.createJobTask;

public class CloseJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAsciiOfLengthBetween(1, 20));
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
        mlBuilder.putJob(BaseMlIntegTestCase.createScheduledJob("job_id").build(), false);
        mlBuilder.putDatafeed(BaseMlIntegTestCase.createDatafeed("datafeed_id", "job_id",
                Collections.singletonList("*")));
        Map<Long, PersistentTask<?>> tasks = new HashMap<>();
        PersistentTask<?> jobTask = createJobTask(1L, "job_id", null, JobState.OPENED);
        tasks.put(1L, jobTask);
        tasks.put(2L, createTask(2L, "datafeed_id", 0L, null, DatafeedState.STARTED));
        ClusterState cs1 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE,
                                new PersistentTasksCustomMetaData(1L, tasks))).build();

        ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class,
                        () -> CloseJobAction.validate("job_id", cs1));
        assertEquals(RestStatus.CONFLICT, e.status());
        assertEquals("cannot close job [job_id], datafeed hasn't been stopped", e.getMessage());

        tasks = new HashMap<>();
        tasks.put(1L, jobTask);
        if (randomBoolean()) {
            tasks.put(2L, createTask(2L, "datafeed_id", 0L, null, DatafeedState.STOPPED));
        }
        ClusterState cs2 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE,
                                new PersistentTasksCustomMetaData(1L, tasks))).build();
        CloseJobAction.validate("job_id", cs2);
    }

    public static PersistentTask<StartDatafeedAction.Request> createTask(long id,
                                                                         String datafeedId,
                                                                         long startTime,
                                                                         String nodeId,
                                                                         DatafeedState state) {
        PersistentTask<StartDatafeedAction.Request> task =
                new PersistentTask<>(id, StartDatafeedAction.NAME,
                        new StartDatafeedAction.Request(datafeedId, startTime),
                        new PersistentTasksCustomMetaData.Assignment(nodeId, "test assignment"));
        task = new PersistentTask<>(task, state);
        return task;
    }

}