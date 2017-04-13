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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManager;
import org.elasticsearch.xpack.ml.datafeed.DatafeedManagerTests;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.Assignment;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.xpack.ml.action.OpenJobActionTests.addJobTask;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData.INITIAL_ASSIGNMENT;
import static org.hamcrest.Matchers.equalTo;

public class StartDatafeedActionTests extends ESTestCase {

    public void testSelectNode() throws Exception {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        IndexMetaData indexMetaData = IndexMetaData.builder("foo")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build(new Date());
        mlMetadata.putJob(job, false);
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo")));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        JobState jobState = randomFrom(JobState.FAILED, JobState.CLOSED);
        addJobTask(job.getId(), "node_id", jobState, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData));

        Assignment result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build(), resolver);
        assertNull(result.getExecutorNode());
        assertEquals("cannot start datafeed [datafeed_id], because job's [job_id] state is [" + jobState +
                "] while state [opened] is required", result.getExplanation());

        tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();
        cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData));
        result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build(), resolver);
        assertEquals("node_id", result.getExecutorNode());
    }

    public void testShardUnassigned() throws Exception {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        IndexMetaData indexMetaData = IndexMetaData.builder("foo")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build(new Date());
        mlMetadata.putJob(job, false);

        // Using wildcard index name to test for index resolving as well
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("fo*")));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        List<Tuple<Integer, ShardRoutingState>> states = new ArrayList<>(2);
        states.add(new Tuple<>(0, ShardRoutingState.UNASSIGNED));

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData, states));

        Assignment result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build(), resolver);
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), equalTo("cannot start datafeed [datafeed_id] because index [foo] " +
                "does not have all primary shards active yet."));
    }

    public void testShardNotAllActive() throws Exception {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        IndexMetaData indexMetaData = IndexMetaData.builder("foo")
                .settings(settings(Version.CURRENT))
                .numberOfShards(2)
                .numberOfReplicas(0)
                .build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build(new Date());
        mlMetadata.putJob(job, false);

        // Using wildcard index name to test for index resolving as well
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("fo*")));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        List<Tuple<Integer, ShardRoutingState>> states = new ArrayList<>(2);
        states.add(new Tuple<>(0, ShardRoutingState.STARTED));
        states.add(new Tuple<>(1, ShardRoutingState.INITIALIZING));

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData, states));

        Assignment result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build(), resolver);
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), equalTo("cannot start datafeed [datafeed_id] because index [foo] " +
                "does not have all primary shards active yet."));
    }

    public void testIndexDoesntExist() throws Exception {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        IndexMetaData indexMetaData = IndexMetaData.builder("foo")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build(new Date());
        mlMetadata.putJob(job, false);
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("not_foo")));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData));

        Assignment result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build(), resolver);
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), equalTo("cannot start datafeed [datafeed_id] because index [not_foo] " +
                "does not exist, is closed, or is still initializing."));
    }

    public void testSelectNode_jobTaskStale() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        IndexMetaData indexMetaData = IndexMetaData.builder("foo")
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Job job = createScheduledJob("job_id").build(new Date());
        mlMetadata.putJob(job, false);
        mlMetadata.putDatafeed(createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo")));

        String nodeId = randomBoolean() ? "node_id2" : null;
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), nodeId, JobState.OPENED, tasksBuilder);
        PersistentTasksCustomMetaData tasks = tasksBuilder.build();

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();

        ClusterState.Builder cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData));

        Assignment result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build(), resolver);
        assertNull(result.getExecutorNode());
        assertEquals("cannot start datafeed [datafeed_id], job [job_id] is unassigned or unassigned to a non existing node",
                result.getExplanation());

        tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id1", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();
        cs = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData));
        result = StartDatafeedAction.selectNode(logger, "datafeed_id", cs.build(), resolver);
        assertEquals("node_id1", result.getExecutorNode());
    }

    public void testValidate() {
        Job job1 = DatafeedManagerTests.createDatafeedJob().build(new Date());
        MlMetadata mlMetadata1 = new MlMetadata.Builder()
                .putJob(job1, false)
                .build();
        Exception e = expectThrows(ResourceNotFoundException.class,
                () -> StartDatafeedAction.validate("some-datafeed", mlMetadata1, null));
        assertThat(e.getMessage(), equalTo("No datafeed with id [some-datafeed] exists"));
    }

    public void testValidate_jobClosed() {
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
        Exception e = expectThrows(ElasticsearchStatusException.class,
                () -> StartDatafeedAction.validate("foo-datafeed", mlMetadata2, tasks));
        assertThat(e.getMessage(), equalTo("cannot start datafeed [foo-datafeed] because job [job_id] is not open"));
    }

    public static StartDatafeedAction.DatafeedTask createDatafeedTask(long id, String type, String action,
                                                                      TaskId parentTaskId,
                                                                      StartDatafeedAction.DatafeedParams params,
                                                                      DatafeedManager datafeedManager) {
        StartDatafeedAction.DatafeedTask task = new StartDatafeedAction.DatafeedTask(id, type, action, parentTaskId, params);
        task.datafeedManager = datafeedManager;
        return task;
    }

    private RoutingTable generateRoutingTable(IndexMetaData indexMetaData) {
        List<Tuple<Integer, ShardRoutingState>> states = new ArrayList<>(1);
        states.add(new Tuple<>(0, ShardRoutingState.STARTED));
        return generateRoutingTable(indexMetaData, states);
    }

    private RoutingTable generateRoutingTable(IndexMetaData indexMetaData, List<Tuple<Integer, ShardRoutingState>> states) {
        IndexRoutingTable.Builder rtBuilder = IndexRoutingTable.builder(indexMetaData.getIndex());

        final String index = indexMetaData.getIndex().getName();
        int counter = 0;
        for (Tuple<Integer, ShardRoutingState> state : states) {
            ShardId shardId = new ShardId(index, "_na_", counter);
            IndexShardRoutingTable.Builder shardRTBuilder = new IndexShardRoutingTable.Builder(shardId);
            ShardRouting shardRouting;

            if (state.v2().equals(ShardRoutingState.STARTED)) {
                shardRouting = TestShardRouting.newShardRouting(index, shardId.getId(),
                        "node_" + Integer.toString(state.v1()), null, true, ShardRoutingState.STARTED);
            } else if (state.v2().equals(ShardRoutingState.INITIALIZING)) {
                shardRouting = TestShardRouting.newShardRouting(index, shardId.getId(),
                        "node_" + Integer.toString(state.v1()), null, true, ShardRoutingState.INITIALIZING);
            } else if (state.v2().equals(ShardRoutingState.RELOCATING)) {
                shardRouting = TestShardRouting.newShardRouting(index, shardId.getId(),
                        "node_" + Integer.toString(state.v1()), "node_" + Integer.toString((state.v1() + 1) % 3),
                        true, ShardRoutingState.RELOCATING);
            } else {
                shardRouting = ShardRouting.newUnassigned(shardId, true,
                        RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            }

            shardRTBuilder.addShard(shardRouting);
            rtBuilder.addIndexShard(shardRTBuilder.build());
            counter += 1;
        }

        return new RoutingTable.Builder().add(rtBuilder).build();
    }

}
