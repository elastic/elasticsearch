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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MlConfigMigratorTests extends ESTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    public void testNonDeletingJobs() {
        Job job1 = JobTests.buildJobBuilder("openjob1").build();
        Job job2 = JobTests.buildJobBuilder("openjob2").build();
        Job deletingJob = JobTests.buildJobBuilder("deleting-job").setDeleting(true).build();

        assertThat(MlConfigMigrator.nonDeletingJobs(Arrays.asList(job1, job2, deletingJob)), containsInAnyOrder(job1, job2));
    }

    public void testClosedOrUnallocatedJobs() {
        Job closedJob = JobTests.buildJobBuilder("closedjob").build();
        Job jobWithoutAllocation = JobTests.buildJobBuilder("jobwithoutallocation").build();
        Job openJob = JobTests.buildJobBuilder("openjob").build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder()
                .putJob(closedJob, false)
                .putJob(jobWithoutAllocation, false)
                .putJob(openJob, false)
                .putDatafeed(createCompatibleDatafeed(closedJob.getId()), Collections.emptyMap(), xContentRegistry());

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId("jobwithoutallocation"), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams("jobwithoutallocation"),
                new PersistentTasksCustomMetaData.Assignment(null, "test assignment"));
        tasksBuilder.addTask(MlTasks.jobTaskId("openjob"), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams("openjob"),
                new PersistentTasksCustomMetaData.Assignment("node1", "test assignment"));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                .localNodeId("node1")
                .masterNodeId("node1")
                .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
                )
                .nodes(nodes)
                .build();

        assertThat(MlConfigMigrator.closedOrUnallocatedJobs(clusterState), containsInAnyOrder(closedJob, jobWithoutAllocation));
    }

    public void testStoppedDatafeedConfigs() {
        Job job1 = JobTests.buildJobBuilder("job1").build();
        Job job2 = JobTests.buildJobBuilder("job2").build();
        Job job3 = JobTests.buildJobBuilder("job3").build();
        DatafeedConfig stopppedDatafeed = createCompatibleDatafeed(job1.getId());
        DatafeedConfig datafeedWithoutAllocation = createCompatibleDatafeed(job2.getId());
        DatafeedConfig startedDatafeed = createCompatibleDatafeed(job3.getId());

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder()
                .putJob(job1, false)
                .putJob(job2, false)
                .putJob(job3, false)
                .putDatafeed(stopppedDatafeed, Collections.emptyMap(), xContentRegistry())
                .putDatafeed(datafeedWithoutAllocation, Collections.emptyMap(), xContentRegistry())
                .putDatafeed(startedDatafeed, Collections.emptyMap(), xContentRegistry());

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.datafeedTaskId(stopppedDatafeed.getId()), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(stopppedDatafeed.getId(), 0L),
                new PersistentTasksCustomMetaData.Assignment(null, "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId(startedDatafeed.getId()), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(stopppedDatafeed.getId(), 0L),
                new PersistentTasksCustomMetaData.Assignment("node1", "test assignment"));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                .localNodeId("node1")
                .masterNodeId("node1")
                .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("migratortests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build())
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
                )
                .nodes(nodes)
                .build();

        assertThat(MlConfigMigrator.stopppedOrUnallocatedDatafeeds(clusterState),
                containsInAnyOrder(stopppedDatafeed, datafeedWithoutAllocation));
    }

    public void testUpdateJobForMigration() {
        Job.Builder oldJob = JobTests.buildJobBuilder("pre-migration");
        Version oldVersion = VersionUtils.randomVersion(random());
        oldJob.setJobVersion(oldVersion);

        Job migratedJob = MlConfigMigrator.updateJobForMigration(oldJob.build());
        assertEquals(Version.CURRENT, migratedJob.getJobVersion());
        assertTrue(migratedJob.getCustomSettings().containsKey(MlConfigMigrator.MIGRATED_FROM_VERSION));
        assertEquals(oldVersion.toString(), migratedJob.getCustomSettings().get(MlConfigMigrator.MIGRATED_FROM_VERSION));
    }

    public void testUpdateJobForMigration_GivenV54Job() {
        Job.Builder oldJob = JobTests.buildJobBuilder("pre-migration");
        // v5.4 jobs did not have a version and should not have a new one set
        oldJob.setJobVersion(null);

        Job migratedJob = MlConfigMigrator.updateJobForMigration(oldJob.build());
        assertNull(migratedJob.getJobVersion());
        assertTrue(migratedJob.getCustomSettings().containsKey(MlConfigMigrator.MIGRATED_FROM_VERSION));
    }

    public void testSerialisationOfUpdatedJob() throws IOException {
        Job migratedJob = MlConfigMigrator.updateJobForMigration(JobTests.buildJobBuilder("pre-migration").build(new Date()));
        Job copy = copyWriteable(migratedJob, new NamedWriteableRegistry(Collections.emptyList()), Job::new, Version.CURRENT);
        assertEquals(migratedJob, copy);
    }

    public void testFilterFailedJobConfigWrites() {
        List<Job> jobs = new ArrayList<>();
        jobs.add(JobTests.buildJobBuilder("foo").build());
        jobs.add(JobTests.buildJobBuilder("bar").build());
        jobs.add(JobTests.buildJobBuilder("baz").build());

        assertThat(MlConfigMigrator.filterFailedJobConfigWrites(Collections.emptySet(), jobs), hasSize(3));
        assertThat(MlConfigMigrator.filterFailedJobConfigWrites(Collections.singleton(Job.documentId("bar")), jobs),
                contains(jobs.get(0), jobs.get(2)));
    }

    public void testFilterFailedDatafeedConfigWrites() {
        List<DatafeedConfig> datafeeds = new ArrayList<>();
        datafeeds.add(createCompatibleDatafeed("foo"));
        datafeeds.add(createCompatibleDatafeed("bar"));
        datafeeds.add(createCompatibleDatafeed("baz"));

        assertThat(MlConfigMigrator.filterFailedDatafeedConfigWrites(Collections.emptySet(), datafeeds), hasSize(3));
        assertThat(MlConfigMigrator.filterFailedDatafeedConfigWrites(Collections.singleton(DatafeedConfig.documentId("df-foo")), datafeeds),
                contains(datafeeds.get(1), datafeeds.get(2)));
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
                .putDatafeed(datafeedConfig1, Collections.emptyMap(), xContentRegistry())
                .putDatafeed(datafeedConfig2, Collections.emptyMap(), xContentRegistry());

        MlConfigMigrator.RemovalResult removalResult = MlConfigMigrator.removeJobsAndDatafeeds(
                Arrays.asList(job1, job2), Arrays.asList(datafeedConfig1, datafeedConfig2), mlMetadata.build());

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
                .putDatafeed(datafeedConfig1, Collections.emptyMap(), xContentRegistry());

        MlConfigMigrator.RemovalResult removalResult = MlConfigMigrator.removeJobsAndDatafeeds(
                Arrays.asList(job1, JobTests.buildJobBuilder("job-none").build()),
                Collections.singletonList(createCompatibleDatafeed("job-none")), mlMetadata.build());

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

    public void testRewritePersistentTaskParams() {
        Map<String, Job> jobs = new HashMap<>();
        Job closedJob = JobTests.buildJobBuilder("closed-job").build();
        Job unallocatedJob = JobTests.buildJobBuilder("job-to-update").build();
        Job allocatedJob = JobTests.buildJobBuilder("allocated-job").build();
        jobs.put(closedJob.getId(), closedJob);
        jobs.put(unallocatedJob.getId(), unallocatedJob);
        jobs.put(allocatedJob.getId(), allocatedJob);

        Map<String, DatafeedConfig> datafeeds = new HashMap<>();
        DatafeedConfig stoppedDatafeed = createCompatibleDatafeed(closedJob.getId());
        DatafeedConfig unallocatedDatafeed = createCompatibleDatafeed(unallocatedJob.getId());
        DatafeedConfig allocatedDatafeed = createCompatibleDatafeed(allocatedJob.getId());
        datafeeds.put(stoppedDatafeed.getId(), stoppedDatafeed);
        datafeeds.put(unallocatedDatafeed.getId(), unallocatedDatafeed);
        datafeeds.put(allocatedDatafeed.getId(), allocatedDatafeed);

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        // job tasks
        tasksBuilder.addTask(MlTasks.jobTaskId(unallocatedJob.getId()), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams(unallocatedJob.getId()),
                new PersistentTasksCustomMetaData.Assignment(null, "no assignment"));
        tasksBuilder.addTask(MlTasks.jobTaskId(allocatedJob.getId()), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams(allocatedJob.getId()),
                new PersistentTasksCustomMetaData.Assignment("node1", "test assignment"));
        // datafeed tasks
        tasksBuilder.addTask(MlTasks.datafeedTaskId(unallocatedDatafeed.getId()), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(unallocatedDatafeed.getId(), 0L),
                new PersistentTasksCustomMetaData.Assignment(null, "no assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId(allocatedDatafeed.getId()), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(allocatedDatafeed.getId(), 0L),
                new PersistentTasksCustomMetaData.Assignment("node1", "test assignment"));

        PersistentTasksCustomMetaData originalTasks = tasksBuilder.build();
        OpenJobAction.JobParams originalUnallocatedTaskParams = (OpenJobAction.JobParams) originalTasks.getTask(
                MlTasks.jobTaskId(unallocatedJob.getId())).getParams();
        assertNull(originalUnallocatedTaskParams.getJob());
        StartDatafeedAction.DatafeedParams originalUnallocatedDatafeedParams = (StartDatafeedAction.DatafeedParams) originalTasks.getTask(
                MlTasks.datafeedTaskId(unallocatedDatafeed.getId())).getParams();
        assertNull(originalUnallocatedDatafeedParams.getJobId());

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                .localNodeId("node1")
                .masterNodeId("node1")
                .build();

        PersistentTasksCustomMetaData modifedTasks = MlConfigMigrator.rewritePersistentTaskParams(jobs, datafeeds, originalTasks, nodes);

        // The unallocated task should be modifed
        OpenJobAction.JobParams modifedUnallocatedTaskParams =
                (OpenJobAction.JobParams) modifedTasks.getTask(MlTasks.jobTaskId(unallocatedJob.getId())).getParams();
        assertNotEquals(originalUnallocatedTaskParams, modifedUnallocatedTaskParams);
        assertEquals(unallocatedJob, modifedUnallocatedTaskParams.getJob());

        // the allocated task should not be modified
        OpenJobAction.JobParams allocatedJobParams =
                (OpenJobAction.JobParams) modifedTasks.getTask(MlTasks.jobTaskId(allocatedJob.getId())).getParams();
        assertEquals(null, allocatedJobParams.getJob());
        OpenJobAction.JobParams originalAllocatedJobParams =
                (OpenJobAction.JobParams) originalTasks.getTask(MlTasks.jobTaskId(allocatedJob.getId())).getParams();
        assertEquals(originalAllocatedJobParams, allocatedJobParams);


        // unallocated datafeed should be updated
        StartDatafeedAction.DatafeedParams modifiedUnallocatedDatafeedParams = (StartDatafeedAction.DatafeedParams) modifedTasks.getTask(
                MlTasks.datafeedTaskId(unallocatedDatafeed.getId())).getParams();
        assertNotEquals(originalUnallocatedDatafeedParams, modifiedUnallocatedDatafeedParams);
        assertEquals(unallocatedDatafeed.getJobId(), modifiedUnallocatedDatafeedParams.getJobId());
        assertEquals(unallocatedDatafeed.getIndices(), modifiedUnallocatedDatafeedParams.getDatafeedIndices());

        // allocated datafeed will not be updated
        StartDatafeedAction.DatafeedParams allocatedDatafeedParams = (StartDatafeedAction.DatafeedParams) modifedTasks.getTask(
                MlTasks.datafeedTaskId(allocatedDatafeed.getId())).getParams();
        assertNull(allocatedDatafeedParams.getJobId());
        assertThat(allocatedDatafeedParams.getDatafeedIndices(), empty());
        StartDatafeedAction.DatafeedParams originalAllocatedDatafeedParams = (StartDatafeedAction.DatafeedParams) originalTasks.getTask(
                MlTasks.datafeedTaskId(allocatedDatafeed.getId())).getParams();
        assertEquals(originalAllocatedDatafeedParams, allocatedDatafeedParams);
    }

    public void testRewritePersistentTaskParams_GivenNoUnallocatedTasks() {
        Map<String, Job> jobs = new HashMap<>();
        Job allocatedJob = JobTests.buildJobBuilder("allocated-job").build();
        jobs.put(allocatedJob.getId(), allocatedJob);

        Map<String, DatafeedConfig> datafeeds = new HashMap<>();
        DatafeedConfig allocatedDatafeed = createCompatibleDatafeed(allocatedJob.getId());
        datafeeds.put(allocatedDatafeed.getId(), allocatedDatafeed);

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        tasksBuilder.addTask(MlTasks.jobTaskId(allocatedJob.getId()), MlTasks.JOB_TASK_NAME,
                new OpenJobAction.JobParams(allocatedJob.getId()),
                new PersistentTasksCustomMetaData.Assignment("node1", "test assignment"));
        tasksBuilder.addTask(MlTasks.datafeedTaskId(allocatedDatafeed.getId()), MlTasks.DATAFEED_TASK_NAME,
                new StartDatafeedAction.DatafeedParams(allocatedDatafeed.getId(), 0L),
                new PersistentTasksCustomMetaData.Assignment("node1", "test assignment"));

        DiscoveryNodes nodes = DiscoveryNodes.builder()
                .add(new DiscoveryNode("node1", new TransportAddress(InetAddress.getLoopbackAddress(), 9300), Version.CURRENT))
                .localNodeId("node1")
                .masterNodeId("node1")
                .build();

        PersistentTasksCustomMetaData originalTasks = tasksBuilder.build();
        PersistentTasksCustomMetaData modifedTasks = MlConfigMigrator.rewritePersistentTaskParams(jobs, datafeeds, originalTasks, nodes);
        assertThat(originalTasks, sameInstance(modifedTasks));
    }

    private DatafeedConfig createCompatibleDatafeed(String jobId) {
        // create a datafeed without aggregations or anything
        // else that may cause validation errors
        DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder("df-" + jobId, jobId);
        datafeedBuilder.setIndices(Collections.singletonList("my_index"));
        return datafeedBuilder.build();
    }
}
