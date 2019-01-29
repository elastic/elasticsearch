/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.junit.Before;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createDatafeed;
import static org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase.createScheduledJob;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DatafeedNodeSelectorTests extends ESTestCase {

    private IndexNameExpressionResolver resolver;
    private DiscoveryNodes nodes;
    private ClusterState clusterState;
    private PersistentTasksCustomMetaData tasks;
    private MlMetadata mlMetadata;

    @Before
    public void init() {
        resolver = new IndexNameExpressionResolver();
        nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_name", "node_id", new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
                        Collections.emptyMap(), Collections.emptySet(), Version.CURRENT))
                .build();
        mlMetadata = new MlMetadata.Builder().build();
    }

    public void testSelectNode_GivenJobIsOpened() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertEquals("node_id", result.getExecutorNode());
        new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).checkDatafeedTaskCanBeCreated();
    }

    public void testSelectNode_GivenJobIsOpening() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", null, tasksBuilder);
        tasks = tasksBuilder.build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertEquals("node_id", result.getExecutorNode());
        new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).checkDatafeedTaskCanBeCreated();
    }

    public void testNoJobTask() {
        Job job = createScheduledJob("job_id").build(new Date());

        // Using wildcard index name to test for index resolving as well
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("fo*"));

        tasks = PersistentTasksCustomMetaData.builder().build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), equalTo("cannot start datafeed [datafeed_id], because the job's [job_id] state is " +
                "[closed] while state [opened] is required"));

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices())
                        .checkDatafeedTaskCanBeCreated());
        assertThat(e.getMessage(), containsString("No node found to start datafeed [datafeed_id], allocation explanation "
                + "[cannot start datafeed [datafeed_id], because the job's [job_id] state is [closed] while state [opened] is required]"));
    }

    public void testSelectNode_GivenJobFailedOrClosed() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        JobState jobState = randomFrom(JobState.FAILED, JobState.CLOSED);
        addJobTask(job.getId(), "node_id", jobState, tasksBuilder);
        tasks = tasksBuilder.build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertNull(result.getExecutorNode());
        assertEquals("cannot start datafeed [datafeed_id], because the job's [job_id] state is [" + jobState +
                "] while state [opened] is required", result.getExplanation());

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices())
                        .checkDatafeedTaskCanBeCreated());
        assertThat(e.getMessage(), containsString("No node found to start datafeed [datafeed_id], allocation explanation "
                + "[cannot start datafeed [datafeed_id], because the job's [job_id] state is [" + jobState
                + "] while state [opened] is required]"));
    }

    public void testShardUnassigned() {
        Job job = createScheduledJob("job_id").build(new Date());

        // Using wildcard index name to test for index resolving as well
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("fo*"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();

        List<Tuple<Integer, ShardRoutingState>> states = new ArrayList<>(2);
        states.add(new Tuple<>(0, ShardRoutingState.UNASSIGNED));

        givenClusterState("foo", 1, 0, states);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), equalTo("cannot start datafeed [datafeed_id] because index [foo] " +
                "does not have all primary shards active yet."));

        new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).checkDatafeedTaskCanBeCreated();
    }

    public void testShardNotAllActive() {
        Job job = createScheduledJob("job_id").build(new Date());

        // Using wildcard index name to test for index resolving as well
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("fo*"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();

        List<Tuple<Integer, ShardRoutingState>> states = new ArrayList<>(2);
        states.add(new Tuple<>(0, ShardRoutingState.STARTED));
        states.add(new Tuple<>(1, ShardRoutingState.INITIALIZING));

        givenClusterState("foo", 2, 0, states);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), equalTo("cannot start datafeed [datafeed_id] because index [foo] " +
                "does not have all primary shards active yet."));

        new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).checkDatafeedTaskCanBeCreated();
    }

    public void testIndexDoesntExist() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("not_foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertNull(result.getExecutorNode());
        assertThat(result.getExplanation(), equalTo("cannot start datafeed [datafeed_id] because index [not_foo] " +
                "does not exist, is closed, or is still initializing."));

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices())
                        .checkDatafeedTaskCanBeCreated());
        assertThat(e.getMessage(), containsString("No node found to start datafeed [datafeed_id], allocation explanation "
                + "[cannot start datafeed [datafeed_id] because index [not_foo] does not exist, is closed, or is still initializing.]"));
    }

    public void testRemoteIndex() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("remote:foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertNotNull(result.getExecutorNode());
    }

    public void testSelectNode_jobTaskStale() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo"));

        String nodeId = randomBoolean() ? "node_id2" : null;
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), nodeId, JobState.OPENED, tasksBuilder);
        // Set to lower allocationId, so job task is stale:
        tasksBuilder.updateTaskState(MlTasks.jobTaskId(job.getId()), new JobTaskState(JobState.OPENED, 0, null));
        tasks = tasksBuilder.build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
                new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertNull(result.getExecutorNode());
        assertEquals("cannot start datafeed [datafeed_id], because the job's [job_id] state is stale",
                result.getExplanation());

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices())
                        .checkDatafeedTaskCanBeCreated());
        assertThat(e.getMessage(), containsString("No node found to start datafeed [datafeed_id], allocation explanation "
                + "[cannot start datafeed [datafeed_id], because the job's [job_id] state is stale]"));

        tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id1", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();
        givenClusterState("foo", 1, 0);
        result = new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertEquals("node_id1", result.getExecutorNode());
        new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).checkDatafeedTaskCanBeCreated();
    }

    public void testSelectNode_GivenJobOpeningAndIndexDoesNotExist() {
        // Here we test that when there are 2 problems, the most critical gets reported first.
        // In this case job is Opening (non-critical) and the index does not exist (critical)

        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("not_foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENING, tasksBuilder);
        tasks = tasksBuilder.build();

        givenClusterState("foo", 1, 0);

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices())
                        .checkDatafeedTaskCanBeCreated());
        assertThat(e.getMessage(), containsString("No node found to start datafeed [datafeed_id], allocation explanation "
                + "[cannot start datafeed [datafeed_id] because index [not_foo] does not exist, is closed, or is still initializing.]"));
    }

    public void testSelectNode_GivenMlUpgradeMode() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();
        mlMetadata = new MlMetadata.Builder().isUpgradeMode(true).build();

        givenClusterState("foo", 1, 0);

        PersistentTasksCustomMetaData.Assignment result =
            new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices()).selectNode();
        assertThat(result, equalTo(MlTasks.AWAITING_UPGRADE));
    }

    public void testCheckDatafeedTaskCanBeCreated_GivenMlUpgradeMode() {
        Job job = createScheduledJob("job_id").build(new Date());
        DatafeedConfig df = createDatafeed("datafeed_id", job.getId(), Collections.singletonList("foo"));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job.getId(), "node_id", JobState.OPENED, tasksBuilder);
        tasks = tasksBuilder.build();
        mlMetadata = new MlMetadata.Builder().isUpgradeMode(true).build();

        givenClusterState("foo", 1, 0);

        ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> new DatafeedNodeSelector(clusterState, resolver, df.getId(), df.getJobId(), df.getIndices())
                .checkDatafeedTaskCanBeCreated());
        assertThat(e.getMessage(), equalTo("Could not start datafeed [datafeed_id] as indices are being upgraded"));
    }

    private void givenClusterState(String index, int numberOfShards, int numberOfReplicas) {
        List<Tuple<Integer, ShardRoutingState>> states = new ArrayList<>(1);
        states.add(new Tuple<>(0, ShardRoutingState.STARTED));
        givenClusterState(index, numberOfShards, numberOfReplicas, states);
    }

    private void givenClusterState(String index, int numberOfShards, int numberOfReplicas, List<Tuple<Integer, ShardRoutingState>> states) {
        IndexMetaData indexMetaData = IndexMetaData.builder(index)
                .settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards)
                .numberOfReplicas(numberOfReplicas)
                .build();

        clusterState = ClusterState.builder(new ClusterName("cluster_name"))
                .metaData(new MetaData.Builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasks)
                        .putCustom(MlMetadata.TYPE, mlMetadata)
                        .put(indexMetaData, false))
                .nodes(nodes)
                .routingTable(generateRoutingTable(indexMetaData, states))
                .build();
    }

    private static RoutingTable generateRoutingTable(IndexMetaData indexMetaData, List<Tuple<Integer, ShardRoutingState>> states) {
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
                        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
            }

            shardRTBuilder.addShard(shardRouting);
            rtBuilder.addIndexShard(shardRTBuilder.build());
            counter += 1;
        }

        return new RoutingTable.Builder().add(rtBuilder).build();
    }
}
