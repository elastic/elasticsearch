/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.junit.Before;

import java.net.InetAddress;
import java.util.Collections;
import java.util.HashSet;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlConfigMigrationEligibilityCheckTests extends ESTestCase {

    private ClusterService clusterService;

    @Before
    public void setUpTests() {
        clusterService = mock(ClusterService.class);
    }

    public void testCanStartMigration_givenMigrationIsDisabled() {
        Settings settings = newSettings(false);
        givenClusterSettings(settings);
        ClusterState clusterState = mock(ClusterState.class);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.canStartMigration(clusterState));
    }

    public void testCanStartMigration_givenNodesNotUpToVersion() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metaData, routingTable);

        // mixed 6.5 and 6.6 nodes
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.V_6_5_0))
                        .add(new DiscoveryNode("node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301), Version.V_6_6_0)))
                .routingTable(routingTable.build())
                .metaData(metaData)
                .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.canStartMigration(clusterState));
    }

    public void testCanStartMigration_givenNodesNotUpToVersionAndMigrationIsEnabled() {
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metaData, routingTable);

        // mixed 6.5 and 6.6 nodes
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .nodes(DiscoveryNodes.builder()
                        .add(new DiscoveryNode("node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.V_6_6_0))
                        .add(new DiscoveryNode("node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301), Version.V_6_6_0)))
                .routingTable(routingTable.build())
                .metaData(metaData)
                .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertTrue(check.canStartMigration(clusterState));
    }

    public void testCanStartMigration_givenMissingIndex() {
        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .build();

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        assertFalse(check.canStartMigration(clusterState));
    }

    public void testCanStartMigration_givenInactiveShards() {
        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        // index is present but no routing
        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metaData, routingTable);
        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(metaData)
                .build();

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        assertFalse(check.canStartMigration(clusterState));
    }

    private void addMlConfigIndex(MetaData.Builder metaData, RoutingTable.Builder routingTable) {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(AnomalyDetectorsIndex.configIndexName());
        indexMetaData.settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        metaData.put(indexMetaData);
        Index index = new Index(AnomalyDetectorsIndex.configIndexName(), "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
    }


    public void testJobIsEligibleForMigration_givenNodesNotUpToVersion() {
        // mixed 6.5 and 6.6 nodes
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .nodes(DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.V_6_5_0))
                .add(new DiscoveryNode("node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301), Version.V_6_6_0)))
            .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.jobIsEligibleForMigration("pre-min-version", clusterState));
    }

    public void testJobIsEligibleForMigration_givenJobNotInClusterState() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests")).build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.jobIsEligibleForMigration("not-in-state", clusterState));
    }

    public void testJobIsEligibleForMigration_givenDeletingJob() {
        Job deletingJob = JobTests.buildJobBuilder("deleting-job").setDeleting(true).build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(deletingJob, false);

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(deletingJob.getId()),
            MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(deletingJob.getId()),
            new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metaData(MetaData.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
            )
            .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.jobIsEligibleForMigration(deletingJob.getId(), clusterState));
    }

    public void testJobIsEligibleForMigration_givenOpenJob() {
        Job openJob = JobTests.buildJobBuilder("open-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(openJob, false);

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(openJob.getId()), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(openJob.getId()),
            new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metaData(MetaData.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
            )
            .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.jobIsEligibleForMigration(openJob.getId(), clusterState));
    }

    public void testJobIsEligibleForMigration_givenOpenJobAndAndMigrationIsDisabled() {
        Job openJob = JobTests.buildJobBuilder("open-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(openJob, false);

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(openJob.getId()), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(openJob.getId()),
            new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metaData(MetaData.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
            )
            .build();

        Settings settings = newSettings(false);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.jobIsEligibleForMigration(openJob.getId(), clusterState));
    }

    public void testJobIsEligibleForMigration_givenClosedJob() {
        Job closedJob = JobTests.buildJobBuilder("closed-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(closedJob, false);

        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metaData, routingTable);

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(metaData.putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .routingTable(routingTable.build())
                .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertTrue(check.jobIsEligibleForMigration(closedJob.getId(), clusterState));
    }

    public void testDatafeedIsEligibleForMigration_givenNodesNotUpToVersion() {
        // mixed 6.5 and 6.6 nodes
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .nodes(DiscoveryNodes.builder()
                .add(new DiscoveryNode("node_id1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.V_6_5_0))
                .add(new DiscoveryNode("node_id2", new TransportAddress(InetAddress.getLoopbackAddress(), 9301), Version.V_6_6_0)))
            .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.datafeedIsEligibleForMigration("pre-min-version", clusterState));
    }

    public void testDatafeedIsEligibleForMigration_givenDatafeedNotInClusterState() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests")).build();
        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.datafeedIsEligibleForMigration("not-in-state", clusterState));
    }

    public void testDatafeedIsEligibleForMigration_givenStartedDatafeed() {
        Job openJob = JobTests.buildJobBuilder("open-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(openJob, false);
        mlMetadata.putDatafeed(createCompatibleDatafeed(openJob.getId()), Collections.emptyMap());
        String datafeedId = "df-" + openJob.getId();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId(datafeedId), MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams(datafeedId, 0L),
            new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metaData(MetaData.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
            )
            .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.datafeedIsEligibleForMigration(datafeedId, clusterState));
    }

    public void testDatafeedIsEligibleForMigration_givenStartedDatafeedAndMigrationIsDisabled() {
        Job openJob = JobTests.buildJobBuilder("open-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(openJob, false);
        mlMetadata.putDatafeed(createCompatibleDatafeed(openJob.getId()), Collections.emptyMap());
        String datafeedId = "df-" + openJob.getId();

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId(datafeedId), MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams(datafeedId, 0L),
            new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metaData(MetaData.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
            )
            .build();

        Settings settings = newSettings(false);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.datafeedIsEligibleForMigration(datafeedId, clusterState));
    }

    public void testDatafeedIsEligibleForMigration_givenStoppedDatafeed() {
        Job job = JobTests.buildJobBuilder("closed-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(job, false);
        mlMetadata.putDatafeed(createCompatibleDatafeed(job.getId()), Collections.emptyMap());
        String datafeedId = "df-" + job.getId();

        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metaData, routingTable);

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(metaData.putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .routingTable(routingTable.build())
                .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertTrue(check.datafeedIsEligibleForMigration(datafeedId, clusterState));
    }

    private void givenClusterSettings(Settings settings) {
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(Collections.singletonList(
                MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION)));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    private static Settings newSettings(boolean migrationEnabled) {
        return Settings.builder()
                .put(MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION.getKey(), migrationEnabled)
                .build();
    }

    private DatafeedConfig createCompatibleDatafeed(String jobId) {
        // create a datafeed without aggregations or anything
        // else that may cause validation errors
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("df-" + jobId, jobId);
        datafeedBuilder.setIndices(Collections.singletonList("my_index"));
        return datafeedBuilder.build();
    }
}
