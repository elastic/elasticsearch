/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.ml.job.persistence.JobProvider;
import org.elasticsearch.xpack.ml.job.persistence.MockClientBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.notifications.Auditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobManagerTests extends ESTestCase {

    private Environment environment;
    private AnalysisRegistry analysisRegistry;
    private ClusterService clusterService;
    private JobProvider jobProvider;
    private Auditor auditor;
    private UpdateJobProcessNotifier updateJobProcessNotifier;

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(environment);
        clusterService = mock(ClusterService.class);
        jobProvider = mock(JobProvider.class);
        auditor = mock(Auditor.class);
        updateJobProcessNotifier = mock(UpdateJobProcessNotifier.class);
    }

    public void testNotifyFilterChangedGivenNoop() {
        MlFilter filter = MlFilter.builder("my_filter").build();
        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        jobManager.notifyFilterChanged(filter, Collections.emptySet(), Collections.emptySet());

        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testNotifyFilterChanged() throws IOException {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(Collections.singletonList(
                detectorReferencingFilter.build()));

        List<BytesReference> docsAsBytes = new ArrayList<>();
        Job.Builder jobReferencingFilter1 = buildJobBuilder("job-referencing-filter-1");
        jobReferencingFilter1.setAnalysisConfig(filterAnalysisConfig);
        docsAsBytes.add(toBytesReference(jobReferencingFilter1.build()));

        Job.Builder jobReferencingFilter2 = buildJobBuilder("job-referencing-filter-2");
        jobReferencingFilter2.setAnalysisConfig(filterAnalysisConfig);
        docsAsBytes.add(toBytesReference(jobReferencingFilter2.build()));

        Job.Builder jobReferencingFilter3 = buildJobBuilder("job-referencing-filter-3");
        jobReferencingFilter3.setAnalysisConfig(filterAnalysisConfig);
        docsAsBytes.add(toBytesReference(jobReferencingFilter3.build()));

        Job.Builder jobWithoutFilter = buildJobBuilder("job-without-filter");
        docsAsBytes.add(toBytesReference(jobWithoutFilter.build()));

        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask(jobReferencingFilter1.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(jobReferencingFilter2.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(jobWithoutFilter.getId(), "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(true);
            return null;
        }).when(updateJobProcessNotifier).submitJobUpdate(any(), any());

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(AnomalyDetectorsIndex.jobConfigIndexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

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

    public void testNotifyFilterChangedGivenOnlyAddedItems() throws IOException {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(Collections.singletonList(
                detectorReferencingFilter.build()));

        Job.Builder jobReferencingFilter = buildJobBuilder("job-referencing-filter");
        jobReferencingFilter.setAnalysisConfig(filterAnalysisConfig);

        List<BytesReference> docsAsBytes = Collections.singletonList(toBytesReference(jobReferencingFilter.build()));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(AnomalyDetectorsIndex.jobConfigIndexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        MlFilter filter = MlFilter.builder("foo_filter").build();

        jobManager.notifyFilterChanged(filter, new TreeSet<>(Arrays.asList("a", "b")), Collections.emptySet());

        verify(auditor).info(jobReferencingFilter.getId(), "Filter [foo_filter] has been modified; added items: ['a', 'b']");
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testNotifyFilterChangedGivenOnlyRemovedItems() throws IOException {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(Collections.singletonList(
                detectorReferencingFilter.build()));

        Job.Builder jobReferencingFilter = buildJobBuilder("job-referencing-filter");
        jobReferencingFilter.setAnalysisConfig(filterAnalysisConfig);
        List<BytesReference> docsAsBytes = Collections.singletonList(toBytesReference(jobReferencingFilter.build()));

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(jobReferencingFilter.build(), false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(AnomalyDetectorsIndex.jobConfigIndexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

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

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        JobManager jobManager = createJobManager(mockClientBuilder.build());

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

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        JobManager jobManager = createJobManager(mockClientBuilder.build());

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

    private JobManager createJobManager(Client client) {
        ClusterSettings clusterSettings = new ClusterSettings(environment.settings(),
                Collections.singleton(MachineLearningField.MAX_MODEL_MEMORY_LIMIT));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        return new JobManager(environment, environment.settings(), jobProvider, clusterService, auditor, client, updateJobProcessNotifier);
    }

    private BytesReference toBytesReference(ToXContent content) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            content.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(xContentBuilder);
        }

    }
}
