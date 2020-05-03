/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
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
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.MlConfigMigrator;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlConfigMigratorIT extends MlSingleNodeTestCase {

    private final IndexNameExpressionResolver expressionResolver = new IndexNameExpressionResolver();
    private ClusterService clusterService;

    @Before
    public void setUpTests() {
        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings(), new HashSet<>(Collections.singletonList(
                MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION)));
        Metadata metadata = mock(Metadata.class);
        SortedMap<String, IndexAbstraction> indicesMap = new TreeMap<>();
        when(metadata.getIndicesLookup()).thenReturn(indicesMap);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);
    }

    public void testWriteConfigToIndex() throws InterruptedException {

        final String indexJobId =  "job-already-migrated";
        // Add a job to the index
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client(), xContentRegistry());
        Job indexJob = buildJobBuilder(indexJobId).build();
        // Same as index job but has extra fields in its custom settings
        // which will be used to check the config was overwritten
        Job migratedJob = MlConfigMigrator.updateJobForMigration(indexJob);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        // put a job representing a previously migrated job
        blockingCall(actionListener -> jobConfigProvider.putJob(migratedJob, actionListener), indexResponseHolder, exceptionHolder);

        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(nodeSettings(), client(), clusterService, expressionResolver);

        AtomicReference<Set<String>> failedIdsHolder = new AtomicReference<>();
        Job foo = buildJobBuilder("foo").build();
        // try to write foo and 'job-already-migrated' which does not have the custom setting field
        assertNull(indexJob.getCustomSettings());

        blockingCall(actionListener -> mlConfigMigrator.writeConfigToIndex(Collections.emptyList(),
                Arrays.asList(indexJob, foo), actionListener),
                failedIdsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(failedIdsHolder.get(), empty());

        // Check job foo has been indexed and job-already-migrated has been overwritten
        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", true, false, actionListener),
                jobsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(jobsHolder.get(), hasSize(2));
        Job fooJob = jobsHolder.get().get(0).build();
        assertEquals("foo", fooJob.getId());
        // this job won't have been marked as migrated as calling
        // MlConfigMigrator.writeConfigToIndex directly does not do that
        assertNull(fooJob.getCustomSettings());
        Job alreadyMigratedJob = jobsHolder.get().get(1).build();
        assertEquals("job-already-migrated", alreadyMigratedJob.getId());
        assertNull(alreadyMigratedJob.getCustomSettings());
    }

    public void testMigrateConfigs() throws InterruptedException, IOException {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("job-foo").build(), false);
        mlMetadata.putJob(buildJobBuilder("job-bar").build(), false);

        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-1", "job-foo");
        builder.setIndices(Collections.singletonList("beats*"));
        mlMetadata.putDatafeed(builder.build(), Collections.emptyMap(), xContentRegistry());

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(metadata.putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .routingTable(routingTable.build())
                .build();
        when(clusterService.state()).thenReturn(clusterState);
        List<Metadata.Custom> customs = new ArrayList<>();
        doAnswer(invocation -> {
                ClusterStateUpdateTask listener = (ClusterStateUpdateTask) invocation.getArguments()[1];
                ClusterState result = listener.execute(clusterState);
                for (ObjectCursor<Metadata.Custom> value : result.metadata().customs().values()){
                    customs.add(value.value);
                }
                listener.clusterStateProcessed("source", mock(ClusterState.class), mock(ClusterState.class));
                return null;
        }).when(clusterService).submitStateUpdateTask(eq("remove-migrated-ml-configs"), any());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();

        // do the migration
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(nodeSettings(), client(), clusterService, expressionResolver);
        // the first time this is called mlmetadata will be snap-shotted
        blockingCall(actionListener -> mlConfigMigrator.migrateConfigs(clusterState, actionListener),
                responseHolder, exceptionHolder);

        // Verify that we have custom values in the new cluster state and that none of them is null
        assertThat(customs.size(), greaterThan(0));
        assertThat(customs.stream().anyMatch(Objects::isNull), is(false));
        assertNull(exceptionHolder.get());
        assertTrue(responseHolder.get());
        assertSnapshot(mlMetadata.build());

        // check the jobs have been migrated
        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client(), xContentRegistry());
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", true, true, actionListener),
                jobsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(jobsHolder.get(), hasSize(2));
        assertTrue(jobsHolder.get().get(0).build().getCustomSettings().containsKey(MlConfigMigrator.MIGRATED_FROM_VERSION));
        assertEquals("job-bar", jobsHolder.get().get(0).build().getId());
        assertTrue(jobsHolder.get().get(1).build().getCustomSettings().containsKey(MlConfigMigrator.MIGRATED_FROM_VERSION));
        assertEquals("job-foo", jobsHolder.get().get(1).build().getId());

        // check datafeeds are migrated
        DatafeedConfigProvider datafeedConfigProvider = new DatafeedConfigProvider(client(), xContentRegistry());
        AtomicReference<List<DatafeedConfig.Builder>> datafeedsHolder = new AtomicReference<>();
        blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*", true, actionListener),
                datafeedsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(datafeedsHolder.get(), hasSize(1));
        assertEquals("df-1", datafeedsHolder.get().get(0).getId());
    }

    public void testExistingSnapshotDoesNotBlockMigration() throws InterruptedException {
        // define the configs
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("job-foo").build(), false);

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(metadata.putCustom(MlMetadata.TYPE, mlMetadata.build()))
            .routingTable(routingTable.build())
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        // index a doc with the same Id as the config snapshot
        PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        AnomalyDetectorsIndex.createStateIndexAndAliasIfNecessary(client(), clusterService.state(), expressionResolver, future);
        future.actionGet();

        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorsIndex.jobStateIndexWriteAlias()).id("ml-config")
                .source(Collections.singletonMap("a_field", "a_value"))
                .opType(DocWriteRequest.OpType.CREATE)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        client().index(indexRequest).actionGet();

        doAnswer(invocation -> {
            ClusterStateUpdateTask listener = (ClusterStateUpdateTask) invocation.getArguments()[1];
            listener.clusterStateProcessed("source", mock(ClusterState.class), mock(ClusterState.class));
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("remove-migrated-ml-configs"), any());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();

        // do the migration
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(nodeSettings(), client(), clusterService, expressionResolver);
        // writing the snapshot should fail because the doc already exists
        // in which case the migration should continue
        blockingCall(actionListener -> mlConfigMigrator.migrateConfigs(clusterState, actionListener),
                responseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertTrue(responseHolder.get());

        // check the jobs have been migrated
        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client(), xContentRegistry());
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", true, true, actionListener),
                jobsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(jobsHolder.get(), hasSize(1));
        assertTrue(jobsHolder.get().get(0).build().getCustomSettings().containsKey(MlConfigMigrator.MIGRATED_FROM_VERSION));
        assertEquals("job-foo", jobsHolder.get().get(0).build().getId());
    }

    public void testMigrateConfigs_GivenLargeNumberOfJobsAndDatafeeds() throws InterruptedException {
        int jobCount = randomIntBetween(150, 201);
        int datafeedCount = randomIntBetween(150, jobCount);

        // and jobs and datafeeds clusterstate
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        for (int i = 0; i < jobCount; i++) {
            mlMetadata.putJob(buildJobBuilder("job-" + i).build(), false);
        }
        for (int i = 0; i < datafeedCount; i++) {
            DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-" + i, "job-" + i);
            builder.setIndices(Collections.singletonList("beats*"));
            mlMetadata.putDatafeed(builder.build(), Collections.emptyMap(), xContentRegistry());
        }

        Metadata.Builder metadata = Metadata.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();
        addMlConfigIndex(metadata, routingTable);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(metadata.putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .routingTable(routingTable.build())
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        doAnswer(invocation -> {
            ClusterStateUpdateTask listener = (ClusterStateUpdateTask) invocation.getArguments()[1];
            listener.clusterStateProcessed("source", mock(ClusterState.class), mock(ClusterState.class));
            return null;
        }).when(clusterService).submitStateUpdateTask(eq("remove-migrated-ml-configs"), any());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();

        // do the migration
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(nodeSettings(), client(), clusterService, expressionResolver);
        blockingCall(actionListener -> mlConfigMigrator.migrateConfigs(clusterState, actionListener),
            responseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertTrue(responseHolder.get());

        // check the jobs have been migrated
        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client(), xContentRegistry());
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", true, true, actionListener),
            jobsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(jobsHolder.get(), hasSize(jobCount));

        // check datafeeds are migrated
        DatafeedConfigProvider datafeedConfigProvider = new DatafeedConfigProvider(client(), xContentRegistry());
        AtomicReference<List<DatafeedConfig.Builder>> datafeedsHolder = new AtomicReference<>();
        blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*", true, actionListener),
            datafeedsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(datafeedsHolder.get(), hasSize(datafeedCount));
    }

    public void testMigrateConfigs_GivenNoJobsOrDatafeeds() throws InterruptedException {
        // Add empty ML metadata
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder()
                .putCustom(MlMetadata.TYPE, mlMetadata.build()))
            .build();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();

        // do the migration
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(nodeSettings(), client(), clusterService, expressionResolver);
        blockingCall(actionListener -> mlConfigMigrator.migrateConfigs(clusterState, actionListener),
            responseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertFalse(responseHolder.get());
    }

    public void testMigrateConfigsWithoutTasks_GivenMigrationIsDisabled() throws InterruptedException {
        Settings settings = Settings.builder().put(nodeSettings())
                .put(MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION.getKey(), false)
                .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(Collections.singletonList(
                MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION)));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        // and jobs and datafeeds clusterstate
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("job-foo").build(), false);
        mlMetadata.putJob(buildJobBuilder("job-bar").build(), false);
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-1", "job-foo");
        builder.setIndices(Collections.singletonList("beats*"));
        mlMetadata.putDatafeed(builder.build(), Collections.emptyMap(), xContentRegistry());

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(Metadata.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();

        // do the migration
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(settings, client(), clusterService, expressionResolver);
        blockingCall(actionListener -> mlConfigMigrator.migrateConfigs(clusterState, actionListener),
                responseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertFalse(responseHolder.get());

        // check the jobs have not been migrated
        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client(), xContentRegistry());
        blockingCall(actionListener -> jobConfigProvider.expandJobs("*", true, true, actionListener),
                jobsHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertThat(jobsHolder.get().isEmpty(), is(true));

        // check datafeeds have not been migrated
        DatafeedConfigProvider datafeedConfigProvider = new DatafeedConfigProvider(client(), xContentRegistry());
        AtomicReference<List<DatafeedConfig.Builder>> datafeedsHolder = new AtomicReference<>();
        blockingCall(actionListener -> datafeedConfigProvider.expandDatafeedConfigs("*", true, actionListener),
                datafeedsHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertThat(datafeedsHolder.get().isEmpty(), is(true));
    }

    public void assertSnapshot(MlMetadata expectedMlMetadata) throws IOException {
        client().admin().indices().prepareRefresh(AnomalyDetectorsIndex.jobStateIndexPattern()).get();
        SearchResponse searchResponse = client()
            .prepareSearch(AnomalyDetectorsIndex.jobStateIndexPattern())
            .setSize(1)
            .setQuery(QueryBuilders.idsQuery().addIds("ml-config"))
            .get();

        assertThat(searchResponse.getHits().getHits().length, greaterThan(0));

        try (InputStream stream = searchResponse.getHits().getAt(0).getSourceRef().streamInput();
             XContentParser parser = XContentFactory.xContent(XContentType.JSON)
                     .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, stream)) {
            MlMetadata recoveredMeta = MlMetadata.LENIENT_PARSER.apply(parser, null).build();
            assertEquals(expectedMlMetadata, recoveredMeta);
        }
    }

    private void addMlConfigIndex(Metadata.Builder metadata, RoutingTable.Builder routingTable) {
        IndexMetadata.Builder indexMetadata = IndexMetadata.builder(AnomalyDetectorsIndex.configIndexName());
        indexMetadata.settings(Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        metadata.put(indexMetadata);
        Index index = new Index(AnomalyDetectorsIndex.configIndexName(), "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));
    }

    public void testConfigIndexIsCreated() throws Exception {
        // and jobs and datafeeds clusterstate
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("job-foo").build(), false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(nodeSettings(), client(), clusterService, expressionResolver);

        // if the cluster state has a job config and the index does not
        // exist it should be created
        blockingCall(actionListener -> mlConfigMigrator.migrateConfigs(clusterState, actionListener),
                responseHolder, exceptionHolder);

        assertBusy(() -> assertTrue(configIndexExists()));
    }

    private boolean configIndexExists() {
        return ESIntegTestCase.indexExists(AnomalyDetectorsIndex.configIndexName(), client());
    }
}


