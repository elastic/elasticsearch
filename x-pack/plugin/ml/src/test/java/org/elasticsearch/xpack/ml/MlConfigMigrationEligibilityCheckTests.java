/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;
import org.junit.Before;

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

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testCanStartMigration_givenMigrationIsDisabled() {
        Settings settings = newSettings(false);
        givenClusterSettings(settings);
        ClusterState clusterState = mock(ClusterState.class);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertFalse(check.canStartMigration(clusterState));
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
        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);
        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metadata(metadata)
                .build();

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);
        assertFalse(check.canStartMigration(clusterState));
    }

    private void addMlConfigIndex(Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(MlConfigIndex.indexName());
        indexMetadata.settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        metadata.put(indexMetadata);
        Index index = new Index(MlConfigIndex.indexName(), "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
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

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(deletingJob.getId()),
            MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(deletingJob.getId()),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metadata(Metadata.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build())
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

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(openJob.getId()), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(openJob.getId()),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metadata(Metadata.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build())
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

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(openJob.getId()), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(openJob.getId()),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metadata(Metadata.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build())
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

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metadata(metadata.putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .routingTable(routingTable.build())
                .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertTrue(check.jobIsEligibleForMigration(closedJob.getId(), clusterState));
    }

    public void testJobIsEligibleForMigration_givenOpenAndUnallocatedJob() {
        Job openJob = JobTests.buildJobBuilder("open-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(openJob, false);

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(openJob.getId()), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams(openJob.getId()),
                new PersistentTasksCustomMetadata.Assignment(null, "no assignment"));

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metadata(metadata
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build())
                )
                .routingTable(routingTable.build())
                .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertTrue(check.jobIsEligibleForMigration(openJob.getId(), clusterState));
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
        mlMetadata.putDatafeed(createCompatibleDatafeed(openJob.getId()), Collections.emptyMap(), xContentRegistry());
        String datafeedId = "df-" + openJob.getId();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId(datafeedId), MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams(datafeedId, 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metadata(Metadata.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build())
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
        mlMetadata.putDatafeed(createCompatibleDatafeed(openJob.getId()), Collections.emptyMap(), xContentRegistry());
        String datafeedId = "df-" + openJob.getId();

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId(datafeedId), MlTasks.DATAFEED_TASK_NAME,
            new StartDatafeedAction.DatafeedParams(datafeedId, 0L),
            new PersistentTasksCustomMetadata.Assignment("node-1", "test assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
            .metadata(Metadata.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build())
                .putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build())
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
        mlMetadata.putDatafeed(createCompatibleDatafeed(job.getId()), Collections.emptyMap(), xContentRegistry());
        String datafeedId = "df-" + job.getId();

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metadata(metadata.putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .routingTable(routingTable.build())
                .build();

        Settings settings = newSettings(true);
        givenClusterSettings(settings);

        MlConfigMigrationEligibilityCheck check = new MlConfigMigrationEligibilityCheck(settings, clusterService);

        assertTrue(check.datafeedIsEligibleForMigration(datafeedId, clusterState));
    }

    public void testDatafeedIsEligibleForMigration_givenUnallocatedDatafeed() {
        Job job = JobTests.buildJobBuilder("closed-job").build();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder().putJob(job, false);
        mlMetadata.putDatafeed(createCompatibleDatafeed(job.getId()), Collections.emptyMap(), xContentRegistry());
        String datafeedId = "df-" + job.getId();

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId(datafeedId), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(datafeedId, 0L),
                new PersistentTasksCustomMetadata.Assignment(null, "no assignment"));

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metadata(metadata
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
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
