/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MLMetadataField;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.JobStorageDeletionTask;
import org.elasticsearch.xpack.ml.MlSingleNodeTestCase;
import org.elasticsearch.xpack.ml.job.JobManager;
import org.elasticsearch.xpack.ml.job.UpdateJobProcessNotifier;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JobManagerIT extends MlSingleNodeTestCase {

    private JobManager jobManager;
    private AnalysisRegistry analysisRegistry;

    @Before
    public void createComponents() throws Exception {
        JobProvider jobProvider = new JobProvider(client(), Settings.EMPTY);
        Auditor auditor = new Auditor(client(), "test_node");

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(this.nodeSettings(),
                Collections.singleton(MachineLearningField.MAX_MODEL_MEMORY_LIMIT));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        UpdateJobProcessNotifier updateJobProcessNotifier = mock(UpdateJobProcessNotifier.class);

        jobManager = new JobManager(node().getEnvironment(), Settings.EMPTY, jobProvider, clusterService, auditor,
                this.client(), updateJobProcessNotifier);
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(node().getEnvironment());
        waitForMlTemplates();
    }

    public void testGetMissingJob() throws InterruptedException {
        AtomicReference<Job> jobHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        blockingCall(actionListener -> jobManager.getJob("missing", actionListener), jobHolder, exceptionHolder);

        assertNull(jobHolder.get());
        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testPutJob_setsCreateTime() throws InterruptedException {
        final String jobId = "is-create-time-set";
        AtomicReference<PutJobAction.Response> responseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        PutJobAction.Request putJobRequest = new PutJobAction.Request(createJob(jobId));
        blockingCall(actionListener -> {
                    try {
                        jobManager.putJob(putJobRequest, analysisRegistry, createEmptyClusterState(), actionListener);
                    } catch (IOException e) {
                        exceptionHolder.set(e);
                    }
                },
                responseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertNotNull(responseHolder.get());

        Job createdJob = responseHolder.get().getResponse();
        assertNotNull(createdJob.getCreateTime());
        Date now = new Date();
        // job create time should be within the last second
        assertThat(now.getTime(), greaterThanOrEqualTo(createdJob.getCreateTime().getTime()));
        assertThat(now.getTime() - 1000, lessThanOrEqualTo(createdJob.getCreateTime().getTime()));

        // Need to set the create time for the job objects to be equal
        Job expectedJob = putJobRequest.getJobBuilder().build(createdJob.getCreateTime());
        assertEquals(expectedJob, createdJob);

        // Check we can't create another job with the same Id
        // TODO For now this only works if we add the job to the cluster state
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(putJobRequest.getJobBuilder().build(), false);
        ClusterState clusterStateWithJob = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MLMetadataField.TYPE, mlMetadata.build()))
                .build();

        responseHolder.set(null);
        blockingCall(actionListener -> {
                    try {
                        jobManager.putJob(putJobRequest, analysisRegistry, clusterStateWithJob, actionListener);
                    } catch (IOException e) {
                        exceptionHolder.set(e);
                    }
                },
                responseHolder, exceptionHolder);

        assertNull(responseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceAlreadyExistsException.class));
    }

    public void testCrud() throws InterruptedException {
        final String jobId = "crud-job";

        AtomicReference<PutJobAction.Response> putJobResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create job
        PutJobAction.Request putJobRequest = new PutJobAction.Request(createJob(jobId));
        blockingCall(actionListener -> {
                    try {
                        jobManager.putJob(putJobRequest, analysisRegistry, createEmptyClusterState(), actionListener);
                    } catch (IOException e) {
                        exceptionHolder.set(e);
                    }
                },
                putJobResponseHolder, exceptionHolder);

        assertNull(exceptionHolder.get());
        assertNotNull(putJobResponseHolder.get());

        // Read Job
        AtomicReference<Job> getJobResponseHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobManager.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());

        // Need to set the create time for the job objects to be equal
        Job expectedJob = putJobRequest.getJobBuilder().build(getJobResponseHolder.get().getCreateTime());
        assertEquals(expectedJob, getJobResponseHolder.get());

        // Update Job
        putJobResponseHolder.set(null);
        JobUpdate jobUpdate = new JobUpdate.Builder(jobId).setDescription("This job has been updated").build();
        UpdateJobAction.Request updateJobRequest = new UpdateJobAction.Request(jobId, jobUpdate);
        blockingCall(actionListener -> jobManager.updateJob(updateJobRequest, actionListener),
                putJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertEquals("This job has been updated", putJobResponseHolder.get().getResponse().getDescription());

        // Delete Job
        AtomicReference<DeleteJobAction.Response> deleteJobResponseHolder = new AtomicReference<>();
        JobStorageDeletionTask deletionTask = new JobStorageDeletionTask(1L, "deletion-task", "deletion-action", "deletion-task",
                TaskId.EMPTY_TASK_ID, Collections.emptyMap());
        DeleteJobAction.Request deleteJobRequest = new DeleteJobAction.Request(jobId);
        blockingCall(actionListener -> jobManager.deleteJob(deleteJobRequest, deletionTask, actionListener),
                deleteJobResponseHolder, exceptionHolder);
        assertNull(exceptionHolder.get());
        assertTrue(deleteJobResponseHolder.get().isAcknowledged());

        // Read deleted job
        getJobResponseHolder.set(null);
        blockingCall(actionListener -> jobManager.getJob(jobId, actionListener), getJobResponseHolder, exceptionHolder);
        assertNull(getJobResponseHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testExpandJobs_givenAll() throws InterruptedException {
        List<Job.Builder> jobs = new ArrayList<>();
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        Date createTime = new Date();
        for (int i = 0; i < 3; i++) {
            Job.Builder builder = createJob(Integer.toString(i));
            jobs.add(builder);
            mlMetadata.putJob(builder.build(createTime), false);
            // createTime must be set for mlMetadata.putJob
            // and unset for jobManager.putJob
            builder.setCreateTime(null);
        }
        // TODO For now this only works if we add the job to the cluster state
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MLMetadataField.TYPE, mlMetadata.build())).build();


        AtomicReference<PutJobAction.Response> putJobResponseHolder = new AtomicReference<>();
        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();

        // Create the jobs
        for (Job.Builder jobBuilder : jobs) {
            PutJobAction.Request putJobRequest = new PutJobAction.Request(jobBuilder);
            blockingCall(actionListener -> {
                        try {
                            jobManager.putJob(putJobRequest, analysisRegistry, createEmptyClusterState(), actionListener);
                        } catch (IOException e) {
                            exceptionHolder.set(e);
                        }
                    },
                    putJobResponseHolder, exceptionHolder);

            assertNull(exceptionHolder.get());
        }

        AtomicReference<QueryPage<Job>> expandedJobsHolder = new AtomicReference<>();
        blockingCall(actionListener -> jobManager.expandJobs("_all", true, clusterState, actionListener),
                expandedJobsHolder, exceptionHolder);

        QueryPage<Job> result = expandedJobsHolder.get();
        assertThat(result.count(), equalTo(3L));
        assertThat(result.results().get(0).getId(), equalTo("0"));
        assertThat(result.results().get(1).getId(), equalTo("1"));
        assertThat(result.results().get(2).getId(), equalTo("2"));

        blockingCall(actionListener -> jobManager.getAllJobs(actionListener),
                expandedJobsHolder, exceptionHolder);
        result = expandedJobsHolder.get();
        assertThat(result.count(), equalTo(3L));
        assertThat(result.results().get(0).getId(), equalTo("0"));
        assertThat(result.results().get(1).getId(), equalTo("1"));
        assertThat(result.results().get(2).getId(), equalTo("2"));
    }

    private Job.Builder createJob(String jobId) {
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d1.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId(jobId);
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(new DataDescription.Builder());
        return builder;
    }

    private ClusterState createEmptyClusterState() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("_name"));
        builder.metaData(MetaData.builder());
        return builder.build();
    }

    private <T> void blockingCall(Consumer<ActionListener<T>> function, AtomicReference<T> response, AtomicReference<Exception> error)
            throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<T> listener = ActionListener.wrap(
                r -> {
                    response.set(r);
                    latch.countDown();
                },
                e -> {
                    error.set(e);
                    latch.countDown();
                }
        );

        function.accept(listener);
        latch.await();
    }
}
