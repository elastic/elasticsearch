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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobManagerTests extends ESTestCase {

    private Environment environment;
    private AnalysisRegistry analysisRegistry;
    private Client client;
    private ClusterService clusterService;
    private JobResultsProvider jobResultsProvider;
    private Auditor auditor;
    private UpdateJobProcessNotifier updateJobProcessNotifier;

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(environment);
        client = mock(Client.class);
        clusterService = mock(ClusterService.class);
        jobResultsProvider = mock(JobResultsProvider.class);
        auditor = mock(Auditor.class);
        updateJobProcessNotifier = mock(UpdateJobProcessNotifier.class);
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
    public void testPutJob_AddsCreateTime() throws IOException {
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
        }).when(jobResultsProvider).createJobResultIndex(requestCaptor.capture(), any(ClusterState.class), any(ActionListener.class));

        ClusterState clusterState = createClusterState();

        jobManager.putJob(putJobRequest, analysisRegistry, clusterState, new ActionListener<PutJobAction.Response>() {
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

    public void testPutJob_ThrowsIfJobExists() throws IOException {
        JobManager jobManager = createJobManager();
        PutJobAction.Request putJobRequest = new PutJobAction.Request(createJob());

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(buildJobBuilder("foo").build(), false);
        ClusterState clusterState = ClusterState.builder(new ClusterName("name"))
                .metaData(MetaData.builder().putCustom(MlMetadata.TYPE, mlMetadata.build())).build();

        jobManager.putJob(putJobRequest, analysisRegistry, clusterState, new ActionListener<PutJobAction.Response>() {
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

    public void testNotifyFilterChangedGivenNoop() {
        MlFilter filter = MlFilter.builder("my_filter").build();
        JobManager jobManager = createJobManager();

        jobManager.notifyFilterChanged(filter, Collections.emptySet(), Collections.emptySet());

        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testNotifyFilterChanged() {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(Collections.singletonList(
                detectorReferencingFilter.build()));

        Job.Builder jobReferencingFilter1 = buildJobBuilder("job-referencing-filter-1");
        jobReferencingFilter1.setAnalysisConfig(filterAnalysisConfig);
        Job.Builder jobReferencingFilter2 = buildJobBuilder("job-referencing-filter-2");
        jobReferencingFilter2.setAnalysisConfig(filterAnalysisConfig);
        Job.Builder jobReferencingFilter3 = buildJobBuilder("job-referencing-filter-3");
        jobReferencingFilter3.setAnalysisConfig(filterAnalysisConfig);
        Job.Builder jobWithoutFilter = buildJobBuilder("job-without-filter");

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(jobReferencingFilter1.build(), false);
        mlMetadata.putJob(jobReferencingFilter2.build(), false);
        mlMetadata.putJob(jobReferencingFilter3.build(), false);
        mlMetadata.putJob(jobWithoutFilter.build(), false);

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(jobReferencingFilter1.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(jobReferencingFilter2.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(jobWithoutFilter.getId(), "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(true);
            return null;
        }).when(updateJobProcessNotifier).submitJobUpdate(any(), any());

        JobManager jobManager = createJobManager();

        MlFilter filter = MlFilter.builder("foo_filter").setItems("a", "b").build();

        jobManager.notifyFilterChanged(filter, new TreeSet<>(Arrays.asList("item 1", "item 2")),
                new TreeSet<>(Collections.singletonList("item 3")));

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any(ActionListener.class));

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo(jobReferencingFilter1.getId()));
        assertThat(capturedUpdateParams.get(0).getFilter(), equalTo(filter));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo(jobReferencingFilter2.getId()));
        assertThat(capturedUpdateParams.get(1).getFilter(), equalTo(filter));

        verify(auditor).info(jobReferencingFilter1.getId(), "Filter [foo_filter] has been modified; added items: " +
                "['item 1', 'item 2'], removed items: ['item 3']");
        verify(auditor).info(jobReferencingFilter2.getId(), "Filter [foo_filter] has been modified; added items: " +
                "['item 1', 'item 2'], removed items: ['item 3']");
        verify(auditor).info(jobReferencingFilter3.getId(), "Filter [foo_filter] has been modified; added items: " +
                "['item 1', 'item 2'], removed items: ['item 3']");
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testNotifyFilterChangedGivenOnlyAddedItems() {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(Collections.singletonList(
                detectorReferencingFilter.build()));

        Job.Builder jobReferencingFilter = buildJobBuilder("job-referencing-filter");
        jobReferencingFilter.setAnalysisConfig(filterAnalysisConfig);

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(jobReferencingFilter.build(), false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        JobManager jobManager = createJobManager();

        MlFilter filter = MlFilter.builder("foo_filter").build();

        jobManager.notifyFilterChanged(filter, new TreeSet<>(Arrays.asList("a", "b")), Collections.emptySet());

        verify(auditor).info(jobReferencingFilter.getId(), "Filter [foo_filter] has been modified; added items: ['a', 'b']");
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testNotifyFilterChangedGivenOnlyRemovedItems() {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(Collections.singletonList(
                detectorReferencingFilter.build()));

        Job.Builder jobReferencingFilter = buildJobBuilder("job-referencing-filter");
        jobReferencingFilter.setAnalysisConfig(filterAnalysisConfig);

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(jobReferencingFilter.build(), false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        JobManager jobManager = createJobManager();

        MlFilter filter = MlFilter.builder("foo_filter").build();

        jobManager.notifyFilterChanged(filter, Collections.emptySet(), new TreeSet<>(Arrays.asList("a", "b")));

        verify(auditor).info(jobReferencingFilter.getId(), "Filter [foo_filter] has been modified; removed items: ['a', 'b']");
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testUpdateProcessOnCalendarChanged() {
        Job.Builder job1 = buildJobBuilder("job-1");
        Job.Builder job2 = buildJobBuilder("job-2");
        Job.Builder job3 = buildJobBuilder("job-3");
        Job.Builder job4 = buildJobBuilder("job-4");

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(job1.build(), false);
        mlMetadata.putJob(job2.build(), false);
        mlMetadata.putJob(job3.build(), false);
        mlMetadata.putJob(job4.build(), false);

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job1.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(job2.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(job3.getId(), "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        JobManager jobManager = createJobManager();

        jobManager.updateProcessOnCalendarChanged(Arrays.asList("job-1", "job-3", "job-4"));

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any(ActionListener.class));

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo(job1.getId()));
        assertThat(capturedUpdateParams.get(0).isUpdateScheduledEvents(), is(true));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo(job3.getId()));
        assertThat(capturedUpdateParams.get(1).isUpdateScheduledEvents(), is(true));
    }

    public void testUpdateProcessOnCalendarChanged_GivenGroups() {
        Job.Builder job1 = buildJobBuilder("job-1");
        job1.setGroups(Collections.singletonList("group-1"));
        Job.Builder job2 = buildJobBuilder("job-2");
        job2.setGroups(Collections.singletonList("group-1"));
        Job.Builder job3 = buildJobBuilder("job-3");

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(job1.build(), false);
        mlMetadata.putJob(job2.build(), false);
        mlMetadata.putJob(job3.build(), false);

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(job1.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(job2.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(job3.getId(), "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build())
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        JobManager jobManager = createJobManager();

        jobManager.updateProcessOnCalendarChanged(Collections.singletonList("group-1"));

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any(ActionListener.class));

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo(job1.getId()));
        assertThat(capturedUpdateParams.get(0).isUpdateScheduledEvents(), is(true));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo(job2.getId()));
        assertThat(capturedUpdateParams.get(1).isUpdateScheduledEvents(), is(true));
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
        ClusterSettings clusterSettings = new ClusterSettings(environment.settings(),
                Collections.singleton(MachineLearningField.MAX_MODEL_MEMORY_LIMIT));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new JobManager(environment, environment.settings(), jobResultsProvider, clusterService,
                auditor, client, updateJobProcessNotifier);
    }

    private ClusterState createClusterState() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("_name"));
        builder.metaData(MetaData.builder());
        return builder.build();
    }
}
