/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlConfigVersion;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.MlConfigMigrationEligibilityCheck;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsPersister;
import org.elasticsearch.xpack.ml.job.persistence.JobResultsProvider;
import org.elasticsearch.xpack.ml.job.persistence.MockClientBuilder;
import org.elasticsearch.xpack.ml.job.process.autodetect.UpdateParams;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.job.task.OpenJobPersistentTasksExecutorTests.addJobTask;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JobManagerTests extends ESTestCase {

    private AnalysisRegistry analysisRegistry;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private JobResultsProvider jobResultsProvider;
    private JobResultsPersister jobResultsPersister;
    private AnomalyDetectionAuditor auditor;
    private UpdateJobProcessNotifier updateJobProcessNotifier;

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        return new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(TestEnvironment.newEnvironment(settings));
        clusterService = mock(ClusterService.class);
        givenClusterSettings(settings);

        jobResultsProvider = mock(JobResultsProvider.class);
        jobResultsPersister = mock(JobResultsPersister.class);
        auditor = mock(AnomalyDetectionAuditor.class);
        updateJobProcessNotifier = mock(UpdateJobProcessNotifier.class);

        ExecutorService executorService = mock(ExecutorService.class);
        threadPool = mock(ThreadPool.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
    }

    public void testGetJobNotInIndexOrCluster() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(MlMetadata.TYPE, mlMetadata.build()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        // job document does not exist
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        MockClientBuilder mockClientBuilder = new MockClientBuilder("jm-test");
        mockClientBuilder.get(getResponse);

        JobManager jobManager = createJobManager(mockClientBuilder.build());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        jobManager.getJob("non-job", ActionListener.wrap(job -> fail("Job not expected"), exceptionHolder::set));

        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testNotifyFilterChangedGivenNoop() {
        MlFilter filter = MlFilter.builder("my_filter").build();
        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        jobManager.notifyFilterChanged(
            filter,
            Collections.emptySet(),
            Collections.emptySet(),
            ActionTestUtils.assertNoFailureListener(r -> {})
        );

        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public void testNotifyFilterChanged() throws IOException {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(detectorReferencingFilter.build())
        );

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

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(jobReferencingFilter1.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(jobReferencingFilter2.getId(), "node_id", JobState.OPENED, tasksBuilder);
        addJobTask(jobWithoutFilter.getId(), "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[1];
            listener.onResponse(true);
            return null;
        }).when(updateJobProcessNotifier).submitJobUpdate(any(), any());

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(MlConfigIndex.indexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        MlFilter filter = MlFilter.builder("foo_filter").setItems("a", "b").build();

        jobManager.notifyFilterChanged(
            filter,
            new TreeSet<>(Arrays.asList("item 1", "item 2")),
            new TreeSet<>(Collections.singletonList("item 3")),
            ActionTestUtils.assertNoFailureListener(r -> {})
        );

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any(ActionListener.class));

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo(jobReferencingFilter1.getId()));
        assertThat(capturedUpdateParams.get(0).getFilter(), equalTo(filter));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo(jobReferencingFilter2.getId()));
        assertThat(capturedUpdateParams.get(1).getFilter(), equalTo(filter));

        verify(auditor).info(
            jobReferencingFilter1.getId(),
            "Filter [foo_filter] has been modified; added items: " + "['item 1', 'item 2'], removed items: ['item 3']"
        );
        verify(auditor).info(
            jobReferencingFilter2.getId(),
            "Filter [foo_filter] has been modified; added items: " + "['item 1', 'item 2'], removed items: ['item 3']"
        );
        verify(auditor).info(
            jobReferencingFilter3.getId(),
            "Filter [foo_filter] has been modified; added items: " + "['item 1', 'item 2'], removed items: ['item 3']"
        );
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testNotifyFilterChangedGivenOnlyAddedItems() throws IOException {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(detectorReferencingFilter.build())
        );

        Job.Builder jobReferencingFilter = buildJobBuilder("job-referencing-filter");
        jobReferencingFilter.setAnalysisConfig(filterAnalysisConfig);

        List<BytesReference> docsAsBytes = Collections.singletonList(toBytesReference(jobReferencingFilter.build()));

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(MlConfigIndex.indexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        MlFilter filter = MlFilter.builder("foo_filter").build();

        jobManager.notifyFilterChanged(
            filter,
            new TreeSet<>(Arrays.asList("a", "b")),
            Collections.emptySet(),
            ActionTestUtils.assertNoFailureListener(r -> {})
        );

        verify(auditor).info(jobReferencingFilter.getId(), "Filter [foo_filter] has been modified; added items: ['a', 'b']");
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testNotifyFilterChangedGivenOnlyRemovedItems() throws IOException {
        Detector.Builder detectorReferencingFilter = new Detector.Builder("count", null);
        detectorReferencingFilter.setByFieldName("foo");
        DetectionRule filterRule = new DetectionRule.Builder(RuleScope.builder().exclude("foo", "foo_filter")).build();
        detectorReferencingFilter.setRules(Collections.singletonList(filterRule));
        AnalysisConfig.Builder filterAnalysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(detectorReferencingFilter.build())
        );

        Job.Builder jobReferencingFilter = buildJobBuilder("job-referencing-filter");
        jobReferencingFilter.setAnalysisConfig(filterAnalysisConfig);
        List<BytesReference> docsAsBytes = Collections.singletonList(toBytesReference(jobReferencingFilter.build()));

        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(MlConfigIndex.indexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        MlFilter filter = MlFilter.builder("foo_filter").build();

        jobManager.notifyFilterChanged(
            filter,
            Collections.emptySet(),
            new TreeSet<>(Arrays.asList("a", "b")),
            ActionTestUtils.assertNoFailureListener(r -> {})
        );

        verify(auditor).info(jobReferencingFilter.getId(), "Filter [foo_filter] has been modified; removed items: ['a', 'b']");
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testUpdateProcessOnCalendarChanged() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job-1", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-2", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-3", "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        // For the JobConfigProvider expand groups search.
        // The search will not return any results
        mockClientBuilder.prepareSearchFields(MlConfigIndex.indexName(), Collections.emptyList());

        JobManager jobManager = createJobManager(mockClientBuilder.build());

        jobManager.updateProcessOnCalendarChanged(
            Arrays.asList("job-1", "job-3", "job-4"),
            ActionTestUtils.assertNoFailureListener(r -> {})
        );

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any());

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo("job-1"));
        assertThat(capturedUpdateParams.get(0).isUpdateScheduledEvents(), is(true));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo("job-3"));
        assertThat(capturedUpdateParams.get(1).isUpdateScheduledEvents(), is(true));
    }

    public void testUpdateProcessOnCalendarChanged_GivenGroups() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask("job-1", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-2", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-3", "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
            .metadata(Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, tasksBuilder.build()))
            .build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        // For the JobConfigProvider expand groups search.
        // group-1 will expand to job-1 and job-2
        List<Map<String, DocumentField>> fieldHits = new ArrayList<>();
        fieldHits.add(
            Collections.singletonMap(
                Job.ID.getPreferredName(),
                new DocumentField(Job.ID.getPreferredName(), Collections.singletonList("job-1"))
            )
        );
        fieldHits.add(
            Collections.singletonMap(
                Job.ID.getPreferredName(),
                new DocumentField(Job.ID.getPreferredName(), Collections.singletonList("job-2"))
            )
        );

        mockClientBuilder.prepareSearchFields(MlConfigIndex.indexName(), fieldHits);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        jobManager.updateProcessOnCalendarChanged(Collections.singletonList("group-1"), ActionTestUtils.assertNoFailureListener(r -> {}));

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any());

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo("job-1"));
        assertThat(capturedUpdateParams.get(0).isUpdateScheduledEvents(), is(true));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo("job-2"));
        assertThat(capturedUpdateParams.get(1).isUpdateScheduledEvents(), is(true));
    }

    public void testValidateCategorizationAnalyzer_GivenValid() throws IOException {

        List<String> categorizationFilters = randomBoolean() ? Collections.singletonList("query: .*") : null;
        CategorizationAnalyzerConfig c = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(categorizationFilters);
        Job.Builder jobBuilder = createCategorizationJob(c, null);
        JobManager.validateCategorizationAnalyzerOrSetDefault(jobBuilder, analysisRegistry, MlConfigVersion.CURRENT);

        Job job = jobBuilder.build(new Date());
        assertThat(
            job.getAnalysisConfig().getCategorizationAnalyzerConfig(),
            equalTo(CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(categorizationFilters))
        );
    }

    public void testValidateCategorizationAnalyzer_GivenInvalid() {

        CategorizationAnalyzerConfig c = new CategorizationAnalyzerConfig.Builder().setAnalyzer("does_not_exist").build();
        Job.Builder jobBuilder = createCategorizationJob(c, null);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> JobManager.validateCategorizationAnalyzerOrSetDefault(jobBuilder, analysisRegistry, MlConfigVersion.CURRENT)
        );

        assertThat(e.getMessage(), equalTo("Failed to find global analyzer [does_not_exist]"));
    }

    public void testSetDefaultCategorizationAnalyzer_GivenAllNewNodes() throws IOException {

        List<String> categorizationFilters = randomBoolean() ? Collections.singletonList("query: .*") : null;
        Job.Builder jobBuilder = createCategorizationJob(null, categorizationFilters);
        JobManager.validateCategorizationAnalyzerOrSetDefault(jobBuilder, analysisRegistry, MlConfigVersion.CURRENT);

        Job job = jobBuilder.build(new Date());
        assertThat(
            job.getAnalysisConfig().getCategorizationAnalyzerConfig(),
            equalTo(CategorizationAnalyzerConfig.buildStandardCategorizationAnalyzer(categorizationFilters))
        );
    }

    private Job.Builder createCategorizationJob(
        CategorizationAnalyzerConfig categorizationAnalyzerConfig,
        List<String> categorizationFilters
    ) {
        Detector.Builder d = new Detector.Builder("count", null).setByFieldName("mlcategory");
        AnalysisConfig.Builder ac = new AnalysisConfig.Builder(Collections.singletonList(d.build())).setCategorizationFieldName("message")
            .setCategorizationAnalyzerConfig(categorizationAnalyzerConfig)
            .setCategorizationFilters(categorizationFilters);

        Job.Builder builder = new Job.Builder();
        builder.setId("cat");
        builder.setAnalysisConfig(ac);
        builder.setDataDescription(new DataDescription.Builder());
        return builder;
    }

    private JobManager createJobManager(Client client) {
        return new JobManager(
            jobResultsProvider,
            jobResultsPersister,
            clusterService,
            auditor,
            threadPool,
            client,
            updateJobProcessNotifier,
            xContentRegistry(),
            TestIndexNameExpressionResolver.newInstance(),
            () -> ByteSizeValue.ZERO
        );
    }

    private BytesReference toBytesReference(ToXContent content) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            content.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(xContentBuilder);
        }
    }

    private void givenClusterSettings(Settings settings) {
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            new HashSet<>(
                Arrays.asList(MachineLearningField.MAX_MODEL_MEMORY_LIMIT, MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION)
            )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }
}
