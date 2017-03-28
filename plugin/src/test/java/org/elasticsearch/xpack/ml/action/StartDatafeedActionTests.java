/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunner;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunnerTests;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.PersistentTask;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.createJobTask;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT;
import static org.hamcrest.Matchers.equalTo;

public class StartDatafeedActionTests extends ESTestCase {

    public void testSelectNode() throws Exception {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build();
        mlMetadata.putJob(job, false);
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("*")));

        JobState jobState = randomFrom(JobState.FAILED, JobState.CLOSED);
        PersistentTask<OpenJobAction.Request> task = createJobTask(0L, job.getId(), "node_id", jobState);
        PersistentTasksCustomMetaData tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap(0L, task));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks))
                .nodes(nodes);

        Assignment result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build());
        assertNull(result.getExecutorNode());
        assertEquals("cannot start datafeed [datafeed_id], because job's [job_id] state is [" + jobState +
                "] while state [opened] is required", result.getExplanation());

        task = createJobTask(0L, job.getId(), "node_id", JobState.OPENED);
        tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap(0L, task));
        cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks))
                .nodes(nodes);
        result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build());
        assertEquals("node_id", result.getExecutorNode());
    }

    public void testSelectNode_jobTaskStale() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build();
        mlMetadata.putJob(job, false);
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("*")));

        String nodeId = randomBoolean() ? "node_id2" : null;
        PersistentTask<OpenJobAction.Request> task = createJobTask(0L, job.getId(), nodeId, JobState.OPENED);
        PersistentTasksCustomMetaData tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap(0L, task));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks))
                .nodes(nodes);

        Assignment result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build());
        assertNull(result.getExecutorNode());
        assertEquals("cannot start datafeed [datafeed_id], job [job_id] is unassigned or unassigned to a non existing node",
                result.getExplanation());

        task = createJobTask(0L, job.getId(), "node_id1", JobState.OPENED);
        tasks = new PersistentTasksCustomMetaData(1L, Collections.singletonMap(0L, task));
        cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks))
                .nodes(nodes);
        result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build());
        assertEquals("node_id1", result.getExecutorNode());
    }

    public void testValidate() {
        Job job1 = DatafeedJobRunnerTests.createDatafeedJob().build();
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> StartDatafeedAction.validate("some-datafeed", mlMetadata1, null));
        assertThat(e.getMessage(), equalTo("No datafeed with id [some-datafeed] exists"));
    }

    public void testValidate_jobClosed() {
        Job job1 = DatafeedJobRunnerTests.createDatafeedJob().build();
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTask<OpenJobAction.Request> task =
                new PersistentTask<>(0L, OpenJobAction.NAME, new OpenJobAction.Request("job_id"), INITIAL_ASSIGNMENT);
        PersistentTasksCustomMetaData tasks = new PersistentTasksCustomMetaData(0L, Collections.singletonMap(0L, task));
        DatafeedConfig datafeedConfig1 = DatafeedJobRunnerTests.createDatafeedConfig("foo-datafeed", "job_id").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1)
                .build();
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> StartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks));
        assertThat(e.getMessage(), equalTo("cannot start datafeed [foo-datafeed] because job [job_id] hasn't been opened"));
    }

    public void testValidate_dataFeedAlreadyStarted() {
        Job job1 = createScheduledJob("job_id").build();
        DatafeedConfig datafeedConfig = createDatafeed("datafeed_id", "job_id", Collections.singletonList("*"));
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .putDatafeed(datafeedConfig)
                .build();

        PersistentTask<OpenJobAction.Request> jobTask = createJobTask(0L, "job_id", "node_id", JobState.OPENED);
        PersistentTask<StartDatafeedAction.Request> datafeedTask =
                new PersistentTask<>(0L, StartDatafeedAction.NAME, new StartDatafeedAction.Request("datafeed_id", 0L),
                        new Assignment("node_id", "test assignment"));
        datafeedTask = new PersistentTask<>(datafeedTask, DatafeedState.STARTED);
        Map<Long, PersistentTask<?>> taskMap = new HashMap<>();
        taskMap.put(0L, jobTask);
        taskMap.put(1L, datafeedTask);
        PersistentTasksCustomMetaData tasks = new PersistentTasksCustomMetaData(2L, taskMap);

        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> StartDatafeedAction.validate("datafeed_id", mlMetadata1, tasks));
        assertThat(e.getMessage(), equalTo("cannot start datafeed [datafeed_id] because it has already been started"));
    }

    public static StartDatafeedAction.DatafeedTask createDatafeedTask(long id, String type, String action,
                                                                      TaskId parentTaskId,
                                                                      StartDatafeedAction.Request request,
                                                                      DatafeedJobRunner datafeedJobRunner) {
        StartDatafeedAction.DatafeedTask task = new StartDatafeedAction.DatafeedTask(id, type, action, parentTaskId, request);
        task.datafeedJobRunner = datafeedJobRunner;
        return task;
    }

}
