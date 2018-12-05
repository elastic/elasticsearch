/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.ml.MlConfigMigrator;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.datafeed.persistence.DatafeedConfigProvider;
import org.elasticsearch.xpack.ml.job.persistence.JobConfigProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class MlConfigMigratorIT extends MlSingleNodeTestCase {

    public void testWriteConfigToIndex() throws InterruptedException {

        final String indexJobId =  "job-already-migrated";
        // Add a job to the index
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client());
        Job indexJob = buildJobBuilder(indexJobId).build();
        // Same as index job but has extra fields in its custom settings
        // which will be used to check the config was overwritten
        Job migratedJob = MlConfigMigrator.updateJobForMigration(indexJob);

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<IndexResponse> indexResponseHolder = new AtomicReference<>();
        // put a job representing a previously migrated job
        blockingCall(actionListener -> jobConfigProvider.putJob(migratedJob, actionListener), indexResponseHolder, exceptionHolder);

        ClusterService clusterService = mock(ClusterService.class);
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(client(), clusterService);

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

    public void testMigrateConfigs() throws InterruptedException {

        // and jobs and datafeeds clusterstate
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("job-foo").build(), false);
        mlMetadata.putJob(buildJobBuilder("job-bar").build(), false);
        DatafeedConfig.Builder builder = new DatafeedConfig.Builder("df-1", "job-foo");
        builder.setIndices(Collections.singletonList("beats*"));
        mlMetadata.putDatafeed(builder.build(), Collections.emptyMap());

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();

        ClusterService clusterService = mock(ClusterService.class);
        doAnswer(invocation -> {
                ClusterStateUpdateTask listener = (ClusterStateUpdateTask) invocation.getArguments()[1];
                listener.clusterStateProcessed("source", mock(ClusterState.class), mock(ClusterState.class));
                return null;
        }).when(clusterService).submitStateUpdateTask(eq("remove-migrated-ml-configs"), any());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        AtomicReference<Boolean> responseHolder = new AtomicReference<>();

        // do the migration
        MlConfigMigrator mlConfigMigrator = new MlConfigMigrator(client(), clusterService);
        blockingCall(actionListener -> mlConfigMigrator.migrateConfigsWithoutTasks(clusterState, actionListener),
                responseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertTrue(responseHolder.get());

        // check the jobs have been migrated
        AtomicReference<List<Job.Builder>> jobsHolder = new AtomicReference<>();
        JobConfigProvider jobConfigProvider = new JobConfigProvider(client());
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
}


