/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.createJobTask;
import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;

public class CloseJobActionTests extends ESTestCase {

    public void testMoveJobToClosingState() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id").build(), false);
        PersistentTask<OpenJobAction.Request> task =
                createJobTask(1L, "job_id", null, randomFrom(JobState.OPENED, JobState.FAILED));
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE,
                                new PersistentTasksCustomMetaData(1L, Collections.singletonMap(1L, task))));
        ClusterState result = CloseJobAction.moveJobToClosingState("job_id", csBuilder.build());

        PersistentTasksCustomMetaData actualTasks = result.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        assertEquals(JobState.CLOSING, actualTasks.getTask(1L).getStatus());

        MlMetadata actualMetadata = result.metaData().custom(MlMetadata.TYPE);
        assertNotNull(actualMetadata.getJobs().get("job_id").getFinishedTime());
    }

    public void testMoveJobToClosingState_jobMissing() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, new PersistentTasksCustomMetaData(1L, Collections.emptyMap())));
        expectThrows(ResourceNotFoundException.class, () -> CloseJobAction.moveJobToClosingState("job_id", csBuilder.build()));
    }

    public void testMoveJobToClosingState_unexpectedJobState() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id").build(), false);
        PersistentTask<OpenJobAction.Request> task = createJobTask(1L, "job_id", null, JobState.OPENING);
        ClusterState.Builder csBuilder1 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE,
                                new PersistentTasksCustomMetaData(1L, Collections.singletonMap(1L, task))));
        ElasticsearchStatusException result =
                expectThrows(ElasticsearchStatusException.class, () -> CloseJobAction.moveJobToClosingState("job_id", csBuilder1.build()));
        assertEquals("cannot close job [job_id], expected job state [opened], but got [opening]", result.getMessage());

        ClusterState.Builder csBuilder2 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, new PersistentTasksCustomMetaData(1L, Collections.emptyMap())));
        result = expectThrows(ElasticsearchStatusException.class, () -> CloseJobAction.moveJobToClosingState("job_id", csBuilder2.build()));
        assertEquals("cannot close job [job_id], expected job state [opened], but got [closed]", result.getMessage());
    }

    public void testCloseJob_datafeedNotStopped() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createScheduledJob("job_id").build(), false);
        mlBuilder.putDatafeed(BaseMlIntegTestCase.createDatafeed("datafeed_id", "job_id", Collections.singletonList("*")));
        Map<Long, PersistentTask<?>> tasks = new HashMap<>();
        PersistentTask<?> jobTask = createJobTask(1L, "job_id", null, JobState.OPENED);
        tasks.put(1L, jobTask);
        tasks.put(2L, createDatafeedTask(2L, "datafeed_id", 0L, null, DatafeedState.STARTED));
        ClusterState cs1 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, new PersistentTasksCustomMetaData(1L, tasks))).build();

        ElasticsearchStatusException e =
                expectThrows(ElasticsearchStatusException.class, () -> CloseJobAction.validateAndFindTask("job_id", cs1));
        assertEquals(RestStatus.CONFLICT, e.status());
        assertEquals("cannot close job [job_id], datafeed hasn't been stopped", e.getMessage());

        tasks = new HashMap<>();
        tasks.put(1L, jobTask);
        if (randomBoolean()) {
            tasks.put(2L, createDatafeedTask(2L, "datafeed_id", 0L, null, DatafeedState.STOPPED));
        }
        ClusterState cs2 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, new PersistentTasksCustomMetaData(1L, tasks))).build();
        assertEquals(jobTask, CloseJobAction.validateAndFindTask("job_id", cs2));
    }

    public static PersistentTask<StartDatafeedAction.Request> createDatafeedTask(long id, String datafeedId, long startTime,
                                                                                 String nodeId, DatafeedState datafeedState) {
        PersistentTask<StartDatafeedAction.Request> task =
                new PersistentTask<>(id, StartDatafeedAction.NAME, new StartDatafeedAction.Request(datafeedId, startTime),
                        new PersistentTasksCustomMetaData.Assignment(nodeId, "test assignment"));
        task = new PersistentTask<>(task, datafeedState);
        return task;
    }

}
