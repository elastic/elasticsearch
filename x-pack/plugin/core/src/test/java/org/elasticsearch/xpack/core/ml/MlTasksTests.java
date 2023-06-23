/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsTaskState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskState;

import java.net.InetAddress;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasSize;

public class MlTasksTests extends ESTestCase {

    public void testGetJobState() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        // A missing task is a closed job
        assertEquals(JobState.CLOSED, MlTasks.getJobState("foo", tasksBuilder.build()));
        // A task with no status is opening
        tasksBuilder.addTask(
            MlTasks.jobTaskId("foo"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo"),
            new PersistentTasksCustomMetadata.Assignment("bar", "test assignment")
        );
        assertEquals(JobState.OPENING, MlTasks.getJobState("foo", tasksBuilder.build()));

        tasksBuilder.updateTaskState(MlTasks.jobTaskId("foo"), new JobTaskState(JobState.OPENED, tasksBuilder.getLastAllocationId(), null));
        assertEquals(JobState.OPENED, MlTasks.getJobState("foo", tasksBuilder.build()));
    }

    public void testGetJobState_GivenNull() {
        assertEquals(JobState.CLOSED, MlTasks.getJobState("foo", null));
    }

    public void testGetDatefeedState() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        // A missing task is a stopped datafeed
        assertEquals(DatafeedState.STOPPED, MlTasks.getDatafeedState("foo", tasksBuilder.build()));

        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("foo"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("foo", 0L),
            new PersistentTasksCustomMetadata.Assignment("bar", "test assignment")
        );
        // A task with no state means the datafeed is starting
        assertEquals(DatafeedState.STARTING, MlTasks.getDatafeedState("foo", tasksBuilder.build()));

        tasksBuilder.updateTaskState(MlTasks.datafeedTaskId("foo"), DatafeedState.STARTED);
        assertEquals(DatafeedState.STARTED, MlTasks.getDatafeedState("foo", tasksBuilder.build()));
    }

    public void testGetSnapshotUpgradeState() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        // A missing task is a stopped snapshot upgrade
        assertEquals(SnapshotUpgradeState.STOPPED, MlTasks.getSnapshotUpgradeState("foo", "1", tasksBuilder.build()));

        tasksBuilder.addTask(
            MlTasks.snapshotUpgradeTaskId("foo", "1"),
            MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("foo", "1"),
            new PersistentTasksCustomMetadata.Assignment("bar", "test assignment")
        );
        // A task with no state means the datafeed is starting
        assertEquals(SnapshotUpgradeState.LOADING_OLD_STATE, MlTasks.getSnapshotUpgradeState("foo", "1", tasksBuilder.build()));

        tasksBuilder.updateTaskState(
            MlTasks.snapshotUpgradeTaskId("foo", "1"),
            new SnapshotUpgradeTaskState(SnapshotUpgradeState.SAVING_NEW_STATE, tasksBuilder.getLastAllocationId(), null)
        );
        assertEquals(SnapshotUpgradeState.SAVING_NEW_STATE, MlTasks.getSnapshotUpgradeState("foo", "1", tasksBuilder.build()));
    }

    public void testGetJobTask() {
        assertNull(MlTasks.getJobTask("foo", null));

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            MlTasks.jobTaskId("foo"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo"),
            new PersistentTasksCustomMetadata.Assignment("bar", "test assignment")
        );

        assertNotNull(MlTasks.getJobTask("foo", tasksBuilder.build()));
        assertNull(MlTasks.getJobTask("other", tasksBuilder.build()));
    }

    public void testGetDatafeedTask() {
        assertNull(MlTasks.getDatafeedTask("foo", null));

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("foo"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("foo", 0L),
            new PersistentTasksCustomMetadata.Assignment("bar", "test assignment")
        );

        assertNotNull(MlTasks.getDatafeedTask("foo", tasksBuilder.build()));
        assertNull(MlTasks.getDatafeedTask("other", tasksBuilder.build()));
    }

    public void testOpenJobIds() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        assertThat(MlTasks.openJobIds(tasksBuilder.build()), empty());

        tasksBuilder.addTask(
            MlTasks.jobTaskId("foo-1"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.jobTaskId("bar"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("bar"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );

        assertThat(MlTasks.openJobIds(tasksBuilder.build()), containsInAnyOrder("foo-1", "bar"));
    }

    public void testOpenJobIds_GivenNull() {
        assertThat(MlTasks.openJobIds(null), empty());
    }

    public void testStartedDatafeedIds() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        assertThat(MlTasks.openJobIds(tasksBuilder.build()), empty());

        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-1"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df1"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df1", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df2"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df2", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );

        assertThat(MlTasks.startedDatafeedIds(tasksBuilder.build()), containsInAnyOrder("df1", "df2"));
    }

    public void testStartedDatafeedIds_GivenNull() {
        assertThat(MlTasks.startedDatafeedIds(null), empty());
    }

    public void testUnallocatedJobIds() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job_with_assignment"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("job_with_assignment"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job_without_assignment"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("job_without_assignment"),
            new PersistentTasksCustomMetadata.Assignment(null, "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job_without_node"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("job_without_node"),
            new PersistentTasksCustomMetadata.Assignment("dead-node", "expired node")
        );

        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node-1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300)))
            .localNodeId("node-1")
            .masterNodeId("node-1")
            .build();

        assertThat(MlTasks.unassignedJobIds(tasksBuilder.build(), nodes), containsInAnyOrder("job_without_assignment", "job_without_node"));
    }

    public void testUnallocatedDatafeedIds() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("datafeed_with_assignment"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("datafeed_with_assignment", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("datafeed_without_assignment"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("datafeed_without_assignment", 0L),
            new PersistentTasksCustomMetadata.Assignment(null, "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("datafeed_without_node"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("datafeed_without_node", 0L),
            new PersistentTasksCustomMetadata.Assignment("dead_node", "expired node")
        );

        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node-1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300)))
            .localNodeId("node-1")
            .masterNodeId("node-1")
            .build();

        assertThat(
            MlTasks.unassignedDatafeedIds(tasksBuilder.build(), nodes),
            containsInAnyOrder("datafeed_without_assignment", "datafeed_without_node")
        );
    }

    public void testDatafeedTasksOnNode() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        assertThat(MlTasks.openJobIds(tasksBuilder.build()), empty());

        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df1"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df1", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-2"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-2"),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df2"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df2", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );

        assertThat(MlTasks.datafeedTasksOnNode(tasksBuilder.build(), "node-2"), contains(hasProperty("id", equalTo("datafeed-df2"))));
    }

    public void testJobTasksOnNode() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        assertThat(MlTasks.openJobIds(tasksBuilder.build()), empty());

        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-1"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df1"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df1", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-2"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-2"),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("df2"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("df2", 0L),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-3"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-3"),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );

        assertThat(
            MlTasks.jobTasksOnNode(tasksBuilder.build(), "node-2"),
            containsInAnyOrder(hasProperty("id", equalTo("job-job-2")), hasProperty("id", equalTo("job-job-3")))
        );
    }

    public void testNonFailedJobTasksOnNode() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        assertThat(MlTasks.openJobIds(tasksBuilder.build()), empty());

        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-1"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-1"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        tasksBuilder.updateTaskState(MlTasks.jobTaskId("job-1"), new JobTaskState(JobState.FAILED, 1, "testing"));
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-2"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-2"),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment")
        );
        if (randomBoolean()) {
            tasksBuilder.updateTaskState(MlTasks.jobTaskId("job-2"), new JobTaskState(JobState.OPENED, 2, "testing"));
        }
        tasksBuilder.addTask(
            MlTasks.jobTaskId("job-3"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("foo-3"),
            new PersistentTasksCustomMetadata.Assignment("node-2", "test assignment")
        );
        if (randomBoolean()) {
            tasksBuilder.updateTaskState(MlTasks.jobTaskId("job-3"), new JobTaskState(JobState.FAILED, 3, "testing"));
        }

        assertThat(MlTasks.nonFailedJobTasksOnNode(tasksBuilder.build(), "node-1"), contains(hasProperty("id", equalTo("job-job-2"))));
    }

    public void testGetDataFrameAnalyticsState_GivenNullTask() {
        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(null);
        assertThat(state, equalTo(DataFrameAnalyticsState.STOPPED));
    }

    public void testGetDataFrameAnalyticsState_GivenTaskWithNullState() {
        String jobId = "foo";
        PersistentTasksCustomMetadata.PersistentTask<?> task = createDataFrameAnalyticsTask(jobId, "test_node", null, false);

        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(task);

        assertThat(state, equalTo(DataFrameAnalyticsState.STARTING));
    }

    public void testGetDataFrameAnalyticsState_GivenTaskWithStartedState() {
        String jobId = "foo";
        PersistentTasksCustomMetadata.PersistentTask<?> task = createDataFrameAnalyticsTask(
            jobId,
            "test_node",
            DataFrameAnalyticsState.STARTED,
            false
        );

        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(task);

        assertThat(state, equalTo(DataFrameAnalyticsState.STARTED));
    }

    public void testGetDataFrameAnalyticsState_GivenStaleTaskWithStartedState() {
        String jobId = "foo";
        PersistentTasksCustomMetadata.PersistentTask<?> task = createDataFrameAnalyticsTask(
            jobId,
            "test_node",
            DataFrameAnalyticsState.STARTED,
            true
        );

        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(task);

        assertThat(state, equalTo(DataFrameAnalyticsState.STARTING));
    }

    public void testGetDataFrameAnalyticsState_GivenTaskWithStoppingState() {
        String jobId = "foo";
        PersistentTasksCustomMetadata.PersistentTask<?> task = createDataFrameAnalyticsTask(
            jobId,
            "test_node",
            DataFrameAnalyticsState.STOPPING,
            false
        );

        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(task);

        assertThat(state, equalTo(DataFrameAnalyticsState.STOPPING));
    }

    public void testGetDataFrameAnalyticsState_GivenStaleTaskWithStoppingState() {
        String jobId = "foo";
        PersistentTasksCustomMetadata.PersistentTask<?> task = createDataFrameAnalyticsTask(
            jobId,
            "test_node",
            DataFrameAnalyticsState.STOPPING,
            true
        );

        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(task);

        assertThat(state, equalTo(DataFrameAnalyticsState.STOPPED));
    }

    public void testGetDataFrameAnalyticsState_GivenTaskWithFailedState() {
        String jobId = "foo";
        PersistentTasksCustomMetadata.PersistentTask<?> task = createDataFrameAnalyticsTask(
            jobId,
            "test_node",
            DataFrameAnalyticsState.FAILED,
            false
        );

        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(task);

        assertThat(state, equalTo(DataFrameAnalyticsState.FAILED));
    }

    public void testGetDataFrameAnalyticsState_GivenStaleTaskWithFailedState() {
        String jobId = "foo";
        PersistentTasksCustomMetadata.PersistentTask<?> task = createDataFrameAnalyticsTask(
            jobId,
            "test_node",
            DataFrameAnalyticsState.FAILED,
            true
        );

        DataFrameAnalyticsState state = MlTasks.getDataFrameAnalyticsState(task);

        assertThat(state, equalTo(DataFrameAnalyticsState.FAILED));
    }

    public void testPrettyPrintTaskName() {
        assertThat(MlTasks.prettyPrintTaskName(MlTasks.DATAFEED_TASK_NAME), equalTo("datafeed"));
        assertThat(MlTasks.prettyPrintTaskName(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME), equalTo("data frame analytics"));
        assertThat(MlTasks.prettyPrintTaskName(MlTasks.JOB_TASK_NAME), equalTo("anomaly detection"));
        assertThat(MlTasks.prettyPrintTaskName(MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME), equalTo("snapshot upgrade (anomaly detection)"));
    }

    public void testPrettyPrintTaskName_GivenUnknownTaskName() {
        expectThrows(IllegalArgumentException.class, () -> MlTasks.prettyPrintTaskName("unknown"));
    }

    public void testFindMlProcessTasks() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(
            MlTasks.jobTaskId("ad-1"),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams("ad-1"),
            new PersistentTasksCustomMetadata.Assignment(randomAlphaOfLength(5), "test")
        );
        tasksBuilder.addTask(
            MlTasks.dataFrameAnalyticsTaskId("dfa-1"),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams("dfa-1", Version.CURRENT, true),
            new PersistentTasksCustomMetadata.Assignment(randomAlphaOfLength(5), "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.snapshotUpgradeTaskId("snapshot-upgrade-1", "some-snapshot-id"),
            MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME,
            new SnapshotUpgradeTaskParams("snapshot-upgrade-1", "some-snapshot-id"),
            new PersistentTasksCustomMetadata.Assignment(randomAlphaOfLength(5), "test assignment")
        );
        tasksBuilder.addTask(
            MlTasks.datafeedTaskId("datafeed-1"),
            MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams("datafeed-1", "now"),
            new PersistentTasksCustomMetadata.Assignment(randomAlphaOfLength(5), "test assignment")
        );
        PersistentTasksCustomMetadata tasks = tasksBuilder.build();

        Set<PersistentTasksCustomMetadata.PersistentTask<?>> mlProcessTasks = MlTasks.findMlProcessTasks(tasks);
        assertThat(mlProcessTasks, hasSize(3));
        Set<String> taskNames = mlProcessTasks.stream()
            .map(PersistentTasksCustomMetadata.PersistentTask::getTaskName)
            .collect(Collectors.toSet());
        assertThat(
            taskNames,
            contains(MlTasks.JOB_TASK_NAME, MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME, MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME)
        );
    }

    private static PersistentTasksCustomMetadata.PersistentTask<?> createDataFrameAnalyticsTask(
        String jobId,
        String nodeId,
        DataFrameAnalyticsState state,
        boolean isStale
    ) {
        PersistentTasksCustomMetadata.Builder builder = PersistentTasksCustomMetadata.builder();
        builder.addTask(
            MlTasks.dataFrameAnalyticsTaskId(jobId),
            MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            new StartDataFrameAnalyticsAction.TaskParams(jobId, Version.CURRENT, false),
            new PersistentTasksCustomMetadata.Assignment(nodeId, "test assignment")
        );
        if (state != null) {
            builder.updateTaskState(
                MlTasks.dataFrameAnalyticsTaskId(jobId),
                new DataFrameAnalyticsTaskState(state, builder.getLastAllocationId() - (isStale ? 1 : 0), null)
            );
        }
        PersistentTasksCustomMetadata tasks = builder.build();
        return tasks.getTask(MlTasks.dataFrameAnalyticsTaskId(jobId));
    }

}
