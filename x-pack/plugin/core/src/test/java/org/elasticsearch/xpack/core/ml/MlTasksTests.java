/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;

import java.net.InetAddress;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class MlTasksTests extends ESTestCase {
    public void testGetJobState() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        // A missing task is a closed job
        assertEquals(JobState.CLOSED, MlTasks.getJobState("foo", tasksBuilder.build()));
        // A task with no status is opening
        tasksBuilder.addTask(MlTasks.jobTaskId("foo"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo"),
                new PersistentTasksCustomMetaData.Assignment("bar", "test assignment"));
        assertEquals(JobState.OPENING, MlTasks.getJobState("foo", tasksBuilder.build()));

        tasksBuilder.updateTaskState(MlTasks.jobTaskId("foo"), new JobTaskState(JobState.OPENED, tasksBuilder.getLastAllocationId(), null));
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

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
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
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
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
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
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

    public void testUnallocatedJobIds() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId("job_with_assignment"), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams("job_with_assignment"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.jobTaskId("job_without_assignment"), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams("job_without_assignment"),
                new PersistentTasksCustomMetaData.Assignment(null, "test assignment"));
        tasksBuilder.addTask(MlTasks.jobTaskId("job_without_node"), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams("job_without_node"),
                new PersistentTasksCustomMetaData.Assignment("dead-node", "expired node"));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node-1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                .localNodeId("node-1")
                .masterNodeId("node-1")
                .build();

        assertThat(MlTasks.unallocatedJobIds(tasksBuilder.build(), nodes),
                containsInAnyOrder("job_without_assignment", "job_without_node"));
    }

    public void testUnallocatedDatafeedIds() {
        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId("datafeed_with_assignment"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("datafeed_with_assignment", 0L),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId("datafeed_without_assignment"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("datafeed_without_assignment", 0L),
                new PersistentTasksCustomMetaData.Assignment(null, "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId("datafeed_without_node"), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams("datafeed_without_node", 0L),
                new PersistentTasksCustomMetaData.Assignment("dead_node", "expired node"));


        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node-1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                .localNodeId("node-1")
                .masterNodeId("node-1")
                .build();

        assertThat(MlTasks.unallocatedDatafeedIds(tasksBuilder.build(), nodes),
                containsInAnyOrder("datafeed_without_assignment", "datafeed_without_node"));
    }

    public void testDataFrameAnalyticsTaskIds() {
        String taskId = MlTasks.dataFrameAnalyticsTaskId("foo");
        assertThat(taskId, equalTo("data_frame_analytics-foo"));
        assertThat(MlTasks.dataFrameAnalyticsIdFromTaskId(taskId), equalTo("foo"));
    }
}
