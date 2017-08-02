/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

import java.util.Collections;
import java.util.Date;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class JobManagerTests extends ESTestCase {

    private Client client;
    private ClusterService clusterService;
    private JobProvider jobProvider;
    private Auditor auditor;

    @Before
    public void setupMocks() {
        client = mock(Client.class);
        clusterService = mock(ClusterService.class);
        jobProvider = mock(JobProvider.class);
        auditor = mock(Auditor.class);
    }

    public void testGetJobOrThrowIfUnknown_GivenUnknownJob() {
        ClusterState cs = createClusterState();
        ESTestCase.expectThrows(ResourceNotFoundException.class, () -> JobManager.getJobOrThrowIfUnknown("foo", cs));
    }

    public void testGetJobOrThrowIfUnknown_GivenKnownJob() {
        Job job = buildJobBuilder("foo").build();
        MlMetadata mlMetadata = new MlMetadata.Builder().putJob(job, false).build();
        ClusterState cs = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata)).build();

        assertEquals(job, JobManager.getJobOrThrowIfUnknown("foo", cs));
    }

    public void testExpandJobs_GivenAll() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        for (int i = 0; i < 3; i++) {
            mlMetadata.putJob(buildJobBuilder(Integer.toString(i)).build(), false);
        }
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata.build())).build();

        JobManager jobManager = createJobManager();
        QueryPage<Job> result = jobManager.expandJobs("_all", true, clusterState);

        assertThat(result.count(), equalTo(3L));
        assertThat(result.results().get(0).getId(), equalTo("0"));
        assertThat(result.results().get(1).getId(), equalTo("1"));
        assertThat(result.results().get(2).getId(), equalTo("2"));
    }

    @SuppressWarnings("unchecked")
    public void testPutJob_AddsCreateTime() {
        JobManager jobManager = createJobManager();
        PutJobAction.Request putJobRequest = new PutJobAction.Request(createJob());

        doAnswer(invocation -> {
            AckedClusterStateUpdateTask<Boolean> task = (AckedClusterStateUpdateTask<Boolean>) invocation.getArguments()[1];
            task.onAllNodesAcked(null);
            return null;
        }).when(clusterService).submitStateUpdateTask(Matchers.eq("put-job-foo"), any(AckedClusterStateUpdateTask.class));

        ArgumentCaptor<Job> requestCaptor = ArgumentCaptor.forClass(Job.class);
        doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[2];
            listener.onResponse(true);
            return null;
        }).when(jobProvider).createJobResultIndex(requestCaptor.capture(), any(ClusterState.class), any(ActionListener.class));

        ClusterState clusterState = createClusterState();

        jobManager.putJob(putJobRequest, clusterState, new ActionListener<PutJobAction.Response>() {
            @Override
            public void onResponse(PutJobAction.Response response) {
                Job job = requestCaptor.getValue();
                assertNotNull(job.getCreateTime());
                Date now = new Date();
                // job create time should be within the last second
                assertThat(now.getTime(), greaterThanOrEqualTo(job.getCreateTime().getTime()));
                assertThat(now.getTime() - 1000, lessThanOrEqualTo(job.getCreateTime().getTime()));
            }

            @Override
            public void onFailure(Exception e) {
                fail(e.toString());
            }
        });
    }

    public void testPutJob_ThrowsIfJobExists() {
        JobManager jobManager = createJobManager();
        PutJobAction.Request putJobRequest = new PutJobAction.Request(createJob());

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("foo").build(), false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata.build())).build();

        jobManager.putJob(putJobRequest, clusterState, new ActionListener<PutJobAction.Response>() {
            @Override
            public void onResponse(PutJobAction.Response response) {
                fail("should have got an error");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof ResourceAlreadyExistsException);
            }
        });
    }

    private Job.Builder createJob() {
        Detector.Builder d1 = new Detector.Builder("info_content", "domain");
        d1.setOverFieldName("client");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d1.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId("foo");
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(new DataDescription.Builder());
        return builder;
    }

    private JobManager createJobManager() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        UpdateJobProcessNotifier notifier = mock(UpdateJobProcessNotifier.class);
        return new JobManager(settings, jobProvider, clusterService, auditor, client, notifier);
    }

    private ClusterState createClusterState() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("_name"));
        builder.metaData(MetaData.builder().putCustom(MlMetadata.TYPE, MlMetadata.EMPTY_METADATA));
        return builder.build();
    }
}
