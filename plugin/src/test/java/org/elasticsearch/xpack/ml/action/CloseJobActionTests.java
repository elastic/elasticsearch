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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.util.Collections;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.createJobTask;
import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;

public class CloseJobActionTests extends ESTestCase {

    public void testMoveJobToClosingState() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id").build(), false);
        PersistentTaskInProgress<OpenJobAction.Request> task =
                createJobTask(1L, "job_id", null, randomFrom(JobState.OPENED, JobState.FAILED));
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksInProgress.TYPE, new PersistentTasksInProgress(1L, Collections.singletonMap(1L, task))));
        ClusterState result = CloseJobAction.moveJobToClosingState("job_id", csBuilder.build());

        PersistentTasksInProgress actualTasks = result.getMetaData().custom(PersistentTasksInProgress.TYPE);
        assertEquals(JobState.CLOSING, actualTasks.getTask(1L).getStatus());

        MlMetadata actualMetadata = result.metaData().custom(MlMetadata.TYPE);
        assertNotNull(actualMetadata.getJobs().get("job_id").getFinishedTime());
    }

    public void testMoveJobToClosingState_jobMissing() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksInProgress.TYPE, new PersistentTasksInProgress(1L, Collections.emptyMap())));
        expectThrows(ResourceNotFoundException.class, () -> CloseJobAction.moveJobToClosingState("job_id", csBuilder.build()));
    }

    public void testMoveJobToClosingState_unexpectedJobState() {
        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(buildJobBuilder("job_id").build(), false);
        PersistentTaskInProgress<OpenJobAction.Request> task = createJobTask(1L, "job_id", null, JobState.OPENING);
        ClusterState.Builder csBuilder1 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksInProgress.TYPE, new PersistentTasksInProgress(1L, Collections.singletonMap(1L, task))));
        ElasticsearchStatusException result =
                expectThrows(ElasticsearchStatusException.class, () -> CloseJobAction.moveJobToClosingState("job_id", csBuilder1.build()));
        assertEquals("cannot close job, expected job state [opened], but got [opening]", result.getMessage());

        ClusterState.Builder csBuilder2 = ClusterState.builder(new ClusterName("_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlBuilder.build())
                        .putCustom(PersistentTasksInProgress.TYPE, new PersistentTasksInProgress(1L, Collections.emptyMap())));
        result = expectThrows(ElasticsearchStatusException.class, () -> CloseJobAction.moveJobToClosingState("job_id", csBuilder2.build()));
        assertEquals("cannot close job, expected job state [opened], but got [closed]", result.getMessage());
    }

}
