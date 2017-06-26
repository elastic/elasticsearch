/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;

import java.util.Date;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.addJobTask;
import static org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT;
import static org.hamcrest.Matchers.equalTo;

public class StartDatafeedActionTests extends ESTestCase {

    public void testValidate_GivenDatafeedIsMissing() {
        Job job = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata = new MlMetadata.Builder()
                .putJob(job, false)
                .build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> StartDatafeedAction.validate("some-datafeed", mlMetadata, null));
        assertThat(e.getMessage(), equalTo("No datafeed with id [some-datafeed] exists"));
    }

    public void testValidate_jobClosed() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTasksCustomMetaData tasks = PersistentTasksCustomMetaData.builder().build();
        DatafeedConfig datafeedConfig1 = DatafeedManagerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1)
                .build();
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> StartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks));
        assertThat(e.getMessage(), equalTo("cannot start datafeed [foo-datafeed] because job [job_id] is closed"));
    }

    public void testValidate_jobOpening() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), null, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedManagerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1)
                .build();

        StartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks);
    }

    public void testValidate_jobOpened() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask("job_id", INITIAL_ASSIGNMENT.getExecutorNode(), JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();
        DatafeedConfig datafeedConfig1 = DatafeedManagerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1)
                .build();

        StartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks);
    }

    public static StartDatafeedAction.DatafeedTask createDatafeedTask(long id, String type, String action,
                                                                      TaskId parentTaskId,
                                                                      StartDatafeedAction.DatafeedParams params,
                                                                      DatafeedManager datafeedManager) {
        StartDatafeedAction.DatafeedTask task = new StartDatafeedAction.DatafeedTask(id, type, action, parentTaskId, params);
        task.datafeedManager = datafeedManager;
        return task;
    }
}
