/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

public class MlTasksTests extends ESTestCase {
    public void testGetJobState() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        // A missing task is a closed job
        assertEquals(JobState.CLOSED, MlTasks.getJobState("foo", tasksBuilder.build()));
        // A task with no status is opening
        tasksBuilder.addTask(MlTasks.jobTaskId("foo"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo"),
                new PersistentTasksCustomMetaData.Assignment("bar", "test assignment"));
        assertEquals(JobState.OPENING, MlTasks.getJobState("foo", tasksBuilder.build()));

        tasksBuilder.updateTaskState(MlTasks.jobTaskId("foo"), new JobTaskState(JobState.OPENED, tasksBuilder.getLastAllocationId()));
        assertEquals(JobState.OPENED, MlTasks.getJobState("foo", tasksBuilder.build()));
    }

    public void testGetJobState_GivenNull() {
        assertEquals(JobState.CLOSED, MlTasks.getJobState("foo", null));
    }

    public void testGetDatefeedState() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        // A missing task is a stopped datafeed
        assertEquals(DatafeedState.STOPPED, MlTasks.getDatafeedState("foo", tasksBuilder.build()));

        tasksBuilder.addTask(MlTasks.datafeedTaskId("foo"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("foo", 0L),
                new PersistentTasksCustomMetaData.Assignment("bar", "test assignment"));
        assertEquals(DatafeedState.STOPPED, MlTasks.getDatafeedState("foo", tasksBuilder.build()));

        tasksBuilder.updateTaskState(MlTasks.datafeedTaskId("foo"), DatafeedState.STARTED);
        assertEquals(DatafeedState.STARTED, MlTasks.getDatafeedState("foo", tasksBuilder.build()));
    }

    public void testGetJobTask() {
        assertNull(MlTasks.getJobTask("foo", null));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId("foo"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo"),
                new PersistentTasksCustomMetaData.Assignment("bar", "test assignment"));

        assertNotNull(MlTasks.getJobTask("foo", tasksBuilder.build()));
        assertNull(MlTasks.getJobTask("other", tasksBuilder.build()));
    }

    public void testGetDatafeedTask() {
        assertNull(MlTasks.getDatafeedTask("foo", null));

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId("foo"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("foo", 0L),
                new PersistentTasksCustomMetaData.Assignment("bar", "test assignment"));

        assertNotNull(MlTasks.getDatafeedTask("foo", tasksBuilder.build()));
        assertNull(MlTasks.getDatafeedTask("other", tasksBuilder.build()));
    }

    public void testOpenJobIds() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        assertThat(MlTasks.openJobIds(tasksBuilder.build()), empty());

        tasksBuilder.addTask(MlTasks.jobTaskId("foo-1"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo-1"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.jobTaskId("bar"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("bar"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId("df"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("df", 0L),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        assertThat(MlTasks.openJobIds(tasksBuilder.build()), containsInAnyOrder("foo-1", "bar"));
    }

    public void testOpenJobIds_GivenNull() {
        assertThat(MlTasks.openJobIds(null), empty());
    }

    public void testStartedDatafeedIds() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        assertThat(MlTasks.openJobIds(tasksBuilder.build()), empty());

        tasksBuilder.addTask(MlTasks.jobTaskId("job-1"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo-1"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId("df1"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("df1", 0L),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId("df2"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("df2", 0L),
                new PersistentTasksCustomMetaData.Assignment("node-2", "test assignment"));

        assertThat(MlTasks.startedDatafeedIds(tasksBuilder.build()), containsInAnyOrder("df1", "df2"));
    }

    public void testStartedDatafeedIds_GivenNull() {
        assertThat(MlTasks.startedDatafeedIds(null), empty());
    }

    public void testTaskExistsForJob() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        assertFalse(MlTasks.taskExistsForJob("job-1", tasksBuilder.build()));

        tasksBuilder.addTask(MlTasks.jobTaskId("foo"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.jobTaskId("bar"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("bar"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        assertFalse(MlTasks.taskExistsForJob("job-1", tasksBuilder.build()));
        assertTrue(MlTasks.taskExistsForJob("foo", tasksBuilder.build()));
    }
}
