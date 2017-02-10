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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedJobRunnerTests;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress;
import org.elasticsearch.xpack.persistent.PersistentTasksInProgress.PersistentTaskInProgress;

import java.net.InetAddress;
import java.util.Collections;

import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.hamcrest.Matchers.equalTo;

public class StartDatafeedActionTests extends ESTestCase {

    public void testSelectNode() throws Exception {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build();
        mlMetadata.putJob(job, false);
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("*")));

        PersistentTaskInProgress<OpenJobAction.Request> task =
                new PersistentTaskInProgress<>(0L, OpenJobAction.NAME, new OpenJobAction.Request(job.getId()), "node_id");
        task = new PersistentTaskInProgress<OpenJobAction.Request>(task, randomFrom(JobState.FAILED, JobState.CLOSED,
                JobState.CLOSING, JobState.OPENING));
        PersistentTasksInProgress tasks = new PersistentTasksInProgress(1L, Collections.singletonMap(0L, task));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .putCustom(PersistentTasksInProgress.TYPE, tasks)
                .nodes(nodes);

        StartDatafeedAction.Request request = new StartDatafeedAction.Request("datafeed_id", 0L);
        DiscoveryNode node = StartDatafeedAction.selectNode(logger, request, cs.build());
        assertNull(node);

        task = new PersistentTaskInProgress<OpenJobAction.Request>(task, JobState.OPENED);
        tasks = new PersistentTasksInProgress(1L, Collections.singletonMap(0L, task));
        cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder().putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .putCustom(PersistentTasksInProgress.TYPE, tasks)
                .nodes(nodes);
        node = StartDatafeedAction.selectNode(logger, request, cs.build());
        assertEquals("node_id", node.getId());
    }

    public void testValidate() {
        Job job1 = DatafeedJobRunnerTests.createDatafeedJob().build();
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> StartDatafeedAction.validate("some-datafeed", mlMetadata1, null, false));
        assertThat(e.getMessage(), equalTo("No datafeed with id [some-datafeed] exists"));
    }

    public void testValidate_jobClosed() {
        Job job1 = DatafeedJobRunnerTests.createDatafeedJob().build();
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        PersistentTaskInProgress<OpenJobAction.Request> task =
                new PersistentTaskInProgress<>(0L, OpenJobAction.NAME, new OpenJobAction.Request("foo"), null);
        PersistentTasksInProgress tasks = new PersistentTasksInProgress(0L, Collections.singletonMap(0L, task));
        DatafeedConfig datafeedConfig1 = DatafeedJobRunnerTests.createDatafeedConfig("foo-datafeed", "foo").build();
        MlMetadata mlMetadata2 = new MlMetadata.Builder(mlMetadata1)
                .putDatafeed(datafeedConfig1)
                .build();
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> StartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks, true));
        assertThat(e.getMessage(), equalTo("cannot start datafeed, expected job state [opened], but got [closed]"));
    }



    public void testValidate_dataFeedAlreadyStarted() {
        Job job1 = createScheduledJob("job_id").build();
        DatafeedConfig datafeedConfig = createDatafeed("datafeed_id", "job_id", Collections.singletonList("*"));
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .putDatafeed(datafeedConfig)
                .build();

        PersistentTaskInProgress<StartDatafeedAction.Request> task =
                new PersistentTaskInProgress<>(0L, StartDatafeedAction.NAME, new StartDatafeedAction.Request("datafeed_id", 0L),
                        "node_id");
        task = new PersistentTaskInProgress<>(task, DatafeedState.STARTED);
        PersistentTasksInProgress tasks = new PersistentTasksInProgress(1L, Collections.singletonMap(0L, task));

        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> StartDatafeedAction.validate("datafeed_id", mlMetadata1, tasks, false));
        assertThat(e.getMessage(), equalTo("datafeed already started, expected datafeed state [stopped], but got [started]"));
    }

}
