/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlConfigMigratorTests extends ESTestCase {

    public void testNonDeletingJobs() {
        Job job1 = JobTests.buildJobBuilder("openjob1").build();
        Job job2 = JobTests.buildJobBuilder("openjob2").build();
        Job deletingJob = JobTests.buildJobBuilder("deleting-job").setDeleting(true).build();

        assertThat(MlConfigMigrator.nonDeletingJobs(Arrays.asList(job1, job2, deletingJob)), containsInAnyOrder(job1, job2));
    }

    public void testClosedJobConfigs() {
        Job openJob1 = JobTests.buildJobBuilder("openjob1").build();
        Job openJob2 = JobTests.buildJobBuilder("openjob2").build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder()
                .putJob(openJob1, false)
                .putJob(openJob2, false)
                .putDatafeed(createCompatibleDatafeed(openJob1.getId()), Collections.emptyMap());

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, PersistentTasksCustomMetaData.builder().build())
                )
                .build();

        assertThat(MlConfigMigrator.closedJobConfigs(clusterState), containsInAnyOrder(openJob1, openJob2));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId("openjob1"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo-1"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));

        clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
                )
                .build();

        assertThat(MlConfigMigrator.closedJobConfigs(clusterState), containsInAnyOrder(openJob2));
    }

    public void testStoppedDatafeedConfigs() {
        Job openJob1 = JobTests.buildJobBuilder("openjob1").build();
        Job openJob2 = JobTests.buildJobBuilder("openjob2").build();
        DatafeedConfig datafeedConfig1 = createCompatibleDatafeed(openJob1.getId());
        DatafeedConfig datafeedConfig2 = createCompatibleDatafeed(openJob2.getId());
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder()
                .putJob(openJob1, false)
                .putJob(openJob2, false)
                .putDatafeed(datafeedConfig1, Collections.emptyMap())
                .putDatafeed(datafeedConfig2, Collections.emptyMap());

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, PersistentTasksCustomMetaData.builder().build())
                )
                .build();

        assertThat(MlConfigMigrator.stoppedDatafeedConfigs(clusterState), containsInAnyOrder(datafeedConfig1, datafeedConfig2));


        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId("openjob1"), MlTasks.JOB_TASK_NAME, new OpenJobAction.JobParams("foo-1"),
                new PersistentTasksCustomMetaData.Assignment("node-1", "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId(datafeedConfig1.getId()), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(datafeedConfig1.getId(), 0L),
                new PersistentTasksCustomMetaData.Assignment("node-2", "test assignment"));

        clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
                )
                .build();

        assertThat(MlConfigMigrator.stoppedDatafeedConfigs(clusterState), containsInAnyOrder(datafeedConfig2));
    }

    public void testUpdateJobForMigration() {
        Job.Builder oldJob = JobTests.buildJobBuilder("pre-migration");
        Version oldVersion = Version.V_6_3_0;
        oldJob.setJobVersion(oldVersion);

        Job migratedJob = MlConfigMigrator.updateJobForMigration(oldJob.build());
        assertEquals(Version.CURRENT, migratedJob.getJobVersion());
        assertTrue(migratedJob.getCustomSettings().containsKey(MlConfigMigrator.MIGRATED_FROM_VERSION));
        assertEquals(oldVersion, migratedJob.getCustomSettings().get(MlConfigMigrator.MIGRATED_FROM_VERSION));
    }

    public void testUpdateJobForMigration_GivenV54Job() {
        Job.Builder oldJob = JobTests.buildJobBuilder("pre-migration");
        // v5.4 jobs did not have a version and should not have a new one set
        oldJob.setJobVersion(null);

        Job migratedJob = MlConfigMigrator.updateJobForMigration(oldJob.build());
        assertNull(migratedJob.getJobVersion());
        assertTrue(migratedJob.getCustomSettings().containsKey(MlConfigMigrator.MIGRATED_FROM_VERSION));
    }

    public void testFilterFailedJobConfigWrites() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(JobTests.buildJobBuilder("foo").build());
        jobs.add(JobTests.buildJobBuilder("bar").build());
        jobs.add(JobTests.buildJobBuilder("baz").build());

        assertThat(MlConfigMigrator.filterFailedJobConfigWrites(Collections.emptySet(), jobs), hasSize(3));
        assertThat(MlConfigMigrator.filterFailedJobConfigWrites(Collections.singleton(Job.documentId("bar")), jobs),
                contains("foo", "baz"));
    }

    public void testFilterFailedDatafeedConfigWrites() {
        List<DatafeedConfig> datafeeds = new ArrayList<>();
        datafeeds.add(createCompatibleDatafeed("foo"));
        datafeeds.add(createCompatibleDatafeed("bar"));
        datafeeds.add(createCompatibleDatafeed("baz"));

        assertThat(MlConfigMigrator.filterFailedDatafeedConfigWrites(Collections.emptySet(), datafeeds), hasSize(3));
        assertThat(MlConfigMigrator.filterFailedDatafeedConfigWrites(Collections.singleton(DatafeedConfig.documentId("df-foo")), datafeeds),
                contains("df-bar", "df-baz"));
    }

    public void testDocumentsNotWritten() {
        BulkItemResponse ok = mock(BulkItemResponse.class);
        when(ok.isFailed()).thenReturn(false);

        BulkItemResponse failed = mock(BulkItemResponse.class);
        when(failed.isFailed()).thenReturn(true);
        BulkItemResponse.Failure failure = mock(BulkItemResponse.Failure.class);
        when(failure.getId()).thenReturn("failed-doc-id");
        when(failure.getCause()).thenReturn(mock(IllegalStateException.class));
        when(failed.getFailure()).thenReturn(failure);

        BulkResponse bulkResponse = new BulkResponse(new BulkItemResponse[] {ok, failed}, 1L);
        Set<String> docsIds = MlConfigMigrator.documentsNotWritten(bulkResponse);
        assertThat(docsIds, contains("failed-doc-id"));
    }

    public void testRemoveJobsAndDatafeeds_removeAll() {
        Job job1 = JobTests.buildJobBuilder("job1").build();
        Job job2 = JobTests.buildJobBuilder("job2").build();
        DatafeedConfig datafeedConfig1 = createCompatibleDatafeed(job1.getId());
        DatafeedConfig datafeedConfig2 = createCompatibleDatafeed(job2.getId());
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder()
                .putJob(job1, false)
                .putJob(job2, false)
                .putDatafeed(datafeedConfig1, Collections.emptyMap())
                .putDatafeed(datafeedConfig2, Collections.emptyMap());

        MlConfigMigrator.RemovalResult removalResult = MlConfigMigrator.removeJobsAndDatafeeds(
                Arrays.asList("job1", "job2"), Arrays.asList("df-job1", "df-job2"), mlMetadata.build());

        assertThat(removalResult.mlMetadata.getJobs().keySet(), empty());
        assertThat(removalResult.mlMetadata.getDatafeeds().keySet(), empty());
        assertThat(removalResult.removedJobIds, contains("job1", "job2"));
        assertThat(removalResult.removedDatafeedIds, contains("df-job1", "df-job2"));
    }

    public void testRemoveJobsAndDatafeeds_removeSome() {
        Job job1 = JobTests.buildJobBuilder("job1").build();
        Job job2 = JobTests.buildJobBuilder("job2").build();
        DatafeedConfig datafeedConfig1 = createCompatibleDatafeed(job1.getId());
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder()
                .putJob(job1, false)
                .putJob(job2, false)
                .putDatafeed(datafeedConfig1, Collections.emptyMap());

        MlConfigMigrator.RemovalResult removalResult = MlConfigMigrator.removeJobsAndDatafeeds(
                Arrays.asList("job1", "job-none"), Collections.singletonList("df-none"), mlMetadata.build());

        assertThat(removalResult.mlMetadata.getJobs().keySet(), contains("job2"));
        assertThat(removalResult.mlMetadata.getDatafeeds().keySet(), contains("df-job1"));
        assertThat(removalResult.removedJobIds, contains("job1"));
        assertThat(removalResult.removedDatafeedIds, empty());
    }

    public void testLimitWrites_GivenBelowLimit() {
        MlConfigMigrator.JobsAndDatafeeds jobsAndDatafeeds = MlConfigMigrator.limitWrites(Collections.emptyList(), Collections.emptyMap());
        assertThat(jobsAndDatafeeds.datafeedConfigs, empty());
        assertThat(jobsAndDatafeeds.jobs, empty());

        List<DatafeedConfig> datafeeds = new ArrayList<>();
        Map<String, Job> jobs = new HashMap<>();

        int numDatafeeds = MlConfigMigrator.MAX_BULK_WRITE_SIZE / 2;
        for (int i=0; i<numDatafeeds; i++) {
            String jobId = "job" + i;
            jobs.put(jobId, JobTests.buildJobBuilder(jobId).build());
            datafeeds.add(createCompatibleDatafeed(jobId));
        }

        jobsAndDatafeeds = MlConfigMigrator.limitWrites(datafeeds, jobs);
        assertThat(jobsAndDatafeeds.datafeedConfigs, hasSize(numDatafeeds));
        assertThat(jobsAndDatafeeds.jobs, hasSize(numDatafeeds));
    }

    public void testLimitWrites_GivenAboveLimit() {
        List<DatafeedConfig> datafeeds = new ArrayList<>();
        Map<String, Job> jobs = new HashMap<>();

        int numDatafeeds = MlConfigMigrator.MAX_BULK_WRITE_SIZE / 2 + 10;
        for (int i=0; i<numDatafeeds; i++) {
            String jobId = "job" + i;
            jobs.put(jobId, JobTests.buildJobBuilder(jobId).build());
            datafeeds.add(createCompatibleDatafeed(jobId));
        }

        MlConfigMigrator.JobsAndDatafeeds jobsAndDatafeeds = MlConfigMigrator.limitWrites(datafeeds, jobs);
        assertEquals(MlConfigMigrator.MAX_BULK_WRITE_SIZE, jobsAndDatafeeds.totalCount());
        assertThat(jobsAndDatafeeds.datafeedConfigs, hasSize(MlConfigMigrator.MAX_BULK_WRITE_SIZE / 2));
        assertThat(jobsAndDatafeeds.jobs, hasSize(MlConfigMigrator.MAX_BULK_WRITE_SIZE / 2));

        // assert that for each datafeed its corresponding job is selected
        Set<String> selectedJobIds = jobsAndDatafeeds.jobs.stream().map(Job::getId).collect(Collectors.toSet());
        Set<String> datafeedJobIds = jobsAndDatafeeds.datafeedConfigs.stream().map(DatafeedConfig::getJobId).collect(Collectors.toSet());
        assertEquals(selectedJobIds, datafeedJobIds);
    }

    public void testLimitWrites_GivenMoreJobsThanDatafeeds() {
        List<DatafeedConfig> datafeeds = new ArrayList<>();
        Map<String, Job> jobs = new HashMap<>();

        int numDatafeeds = MlConfigMigrator.MAX_BULK_WRITE_SIZE / 2 - 10;
        for (int i=0; i<numDatafeeds; i++) {
            String jobId = "job" + i;
            jobs.put(jobId, JobTests.buildJobBuilder(jobId).build());
            datafeeds.add(createCompatibleDatafeed(jobId));
        }

        for (int i=numDatafeeds; i<numDatafeeds + 40; i++) {
            String jobId = "job" + i;
            jobs.put(jobId, JobTests.buildJobBuilder(jobId).build());
        }

        MlConfigMigrator.JobsAndDatafeeds jobsAndDatafeeds = MlConfigMigrator.limitWrites(datafeeds, jobs);
        assertEquals(MlConfigMigrator.MAX_BULK_WRITE_SIZE, jobsAndDatafeeds.totalCount());
        assertThat(jobsAndDatafeeds.datafeedConfigs, hasSize(numDatafeeds));
        assertThat(jobsAndDatafeeds.jobs, hasSize(MlConfigMigrator.MAX_BULK_WRITE_SIZE - numDatafeeds));

        // assert that for each datafeed its corresponding job is selected
        Set<String> selectedJobIds = jobsAndDatafeeds.jobs.stream().map(Job::getId).collect(Collectors.toSet());
        Set<String> datafeedJobIds = jobsAndDatafeeds.datafeedConfigs.stream().map(DatafeedConfig::getJobId).collect(Collectors.toSet());
        assertTrue(selectedJobIds.containsAll(datafeedJobIds));
    }

    public void testLimitWrites_GivenNullJob() {
        List<DatafeedConfig> datafeeds = Collections.singletonList(createCompatibleDatafeed("no-job-for-this-datafeed"));
        MlConfigMigrator.JobsAndDatafeeds jobsAndDatafeeds = MlConfigMigrator.limitWrites(datafeeds, Collections.emptyMap());

        assertThat(jobsAndDatafeeds.datafeedConfigs, hasSize(1));
        assertThat(jobsAndDatafeeds.jobs, empty());
    }

    private DatafeedConfig createCompatibleDatafeed(String jobId) {
        // create a datafeed without aggregations or anything
        // else that may cause validation errors
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("df-" + jobId, jobId);
        datafeedBuilder.setIndices(Collections.singletonList("my_index"));
        return datafeedBuilder.build();
    }
}
