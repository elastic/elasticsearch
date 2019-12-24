/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.RuleScope;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
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
import org.mockito.Matchers;
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
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.action.TransportOpenJobActionTests.addJobTask;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
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
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
                .build();
        environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(environment);
        clusterService = mock(ClusterService.class);
        givenClusterSettings(settings);

        jobResultsProvider = mock(JobResultsProvider.class);
        jobResultsPersister = mock(JobResultsPersister.class);
        auditor = mock(AnomalyDetectionAuditor.class);
        updateJobProcessNotifier = mock(UpdateJobProcessNotifier.class);

        ExecutorService executorService = mock(ExecutorService.class);
        threadPool = mock(ThreadPool.class);
        org.elasticsearch.mock.orig.Mockito.doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
    }

    public void testGetJobNotInIndexOrCluster() {
        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        // job document does not exist
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        MockClientBuilder mockClientBuilder = new MockClientBuilder("jm-test");
        mockClientBuilder.get(getResponse);

        JobManager jobManager = createJobManager(mockClientBuilder.build());

        AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        jobManager.getJob("non-job", ActionListener.wrap(
                job -> fail("Job not expected"),
                e -> exceptionHolder.set(e)
        ));

        assertNotNull(exceptionHolder.get());
        assertThat(exceptionHolder.get(), instanceOf(ResourceNotFoundException.class));
    }

    public void testGetJobFromClusterWhenNotInIndex() {
        String clusterJobId = "cluster-job";
        Job clusterJob = buildJobBuilder(clusterJobId).build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(clusterJob, false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        // job document does not exist
        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        MockClientBuilder mockClientBuilder = new MockClientBuilder("jm-test");
        mockClientBuilder.get(getResponse);

        JobManager jobManager = createJobManager(mockClientBuilder.build());

        AtomicReference<Job> jobHolder = new AtomicReference<>();
        jobManager.getJob(clusterJobId, ActionListener.wrap(
                job -> jobHolder.set(job),
                e -> fail(e.getMessage())
        ));

        assertNotNull(jobHolder.get());
        assertEquals(clusterJob, jobHolder.get());
    }

    public void testExpandJobsFromClusterStateAndIndex() throws IOException {
        Job csJobFoo1 = buildJobBuilder("foo-cs-1").build();
        Job csJobFoo2 = buildJobBuilder("foo-cs-2").build();
        Job csJobBar = buildJobBuilder("bar-cs").build();

        MlMetadata.Builder mlMetadata = new MlMetadata.Builder();
        mlMetadata.putJob(csJobFoo1, false);
        mlMetadata.putJob(csJobFoo2, false);
        mlMetadata.putJob(csJobBar, false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlMetadata.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);


        List<BytesReference> docsAsBytes = new ArrayList<>();

        Job.Builder indexJobFoo = buildJobBuilder("foo-index");
        docsAsBytes.add(toBytesReference(indexJobFoo.build()));

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(AnomalyDetectorsIndex.configIndexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());


        AtomicReference<QueryPage<Job>> jobsHolder = new AtomicReference<>();
        jobManager.expandJobs("_all", true, ActionListener.wrap(
            jobs -> jobsHolder.set(jobs),
             e -> fail(e.getMessage())
        ));

        assertNotNull(jobsHolder.get());
        assertThat(jobsHolder.get().results(), hasSize(4));
        List<String> jobIds = jobsHolder.get().results().stream().map(Job::getId).collect(Collectors.toList());
        assertThat(jobIds, contains("bar-cs", "foo-cs-1", "foo-cs-2", "foo-index"));

        jobsHolder.set(null);
        jobManager.expandJobs("foo*", true, ActionListener.wrap(
                jobs -> jobsHolder.set(jobs),
                e -> fail(e.getMessage())
        ));

        assertNotNull(jobsHolder.get());
        assertThat(jobsHolder.get().results(), hasSize(3));
        jobIds = jobsHolder.get().results().stream().map(Job::getId).collect(Collectors.toList());
        assertThat(jobIds, contains("foo-cs-1", "foo-cs-2", "foo-index"));
    }

    @SuppressWarnings("unchecked")
    public void testPutJob_AddsCreateTime() throws IOException {
        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        JobManager jobManager = createJobManager(mockClientBuilder.build());

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

    public void testPutJob_ThrowsIfJobExistsInClusterState() throws IOException {
        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        JobManager jobManager = createJobManager(mockClientBuilder.build());

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
        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        jobManager.notifyFilterChanged(filter, Collections.emptySet(), Collections.emptySet(), ActionListener.wrap(
                r -> {},
                e -> fail(e.getMessage())
        ));

        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
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
        mockClientBuilder.prepareSearch(AnomalyDetectorsIndex.configIndexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        MlFilter filter = MlFilter.builder("foo_filter").setItems("a", "b").build();

        jobManager.notifyFilterChanged(filter, new TreeSet<>(Arrays.asList("item 1", "item 2")),
                new TreeSet<>(Collections.singletonList("item 3")), ActionListener.wrap(
                        r -> {},
                        e -> fail(e.getMessage())
                ));

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

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(AnomalyDetectorsIndex.configIndexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        MlFilter filter = MlFilter.builder("foo_filter").build();

        jobManager.notifyFilterChanged(filter, new TreeSet<>(Arrays.asList("a", "b")), Collections.emptySet(),
                ActionListener.wrap(
                        r -> {},
                        e -> fail(e.getMessage())
                ));

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

        PersistentTasksCustomMetaData.Builder tasksBuilder = PersistentTasksCustomMetaData.builder();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        mockClientBuilder.prepareSearch(AnomalyDetectorsIndex.configIndexName(), docsAsBytes);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        MlFilter filter = MlFilter.builder("foo_filter").build();

        jobManager.notifyFilterChanged(filter, Collections.emptySet(), new TreeSet<>(Arrays.asList("a", "b")),
                ActionListener.wrap(
                        r -> {},
                        e -> fail(e.getMessage())
                ));

        verify(auditor).info(jobReferencingFilter.getId(), "Filter [foo_filter] has been modified; removed items: ['a', 'b']");
        Mockito.verifyNoMoreInteractions(auditor, updateJobProcessNotifier);
    }

    public void testUpdateJob_notAllowedPreMigration() {
        MlMetadata.Builder mlmetadata = new MlMetadata.Builder().putJob(buildJobBuilder("closed-job-not-migrated").build(), false);

        MetaData.Builder metaData = MetaData.builder();
        RoutingTable.Builder routingTable = RoutingTable.builder();

        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(AnomalyDetectorsIndex.configIndexName());
        indexMetaData.settings(Settings.builder()
                .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
        );
        metaData.put(indexMetaData);
        Index index = new Index(AnomalyDetectorsIndex.configIndexName(), "_uuid");
        ShardId shardId = new ShardId(index, 0);
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId, true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
        shardRouting = shardRouting.initialize("node_id", null, 0L);
        shardRouting = shardRouting.moveToStarted();
        routingTable.add(IndexRoutingTable.builder(index)
                .addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(shardRouting).build()));

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(metaData.putCustom(MlMetadata.TYPE, mlmetadata.build()))
                .routingTable(routingTable.build())
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        JobManager jobManager = createJobManager(new MockClientBuilder("jobmanager-test").build());
        jobManager.updateJob(new UpdateJobAction.Request("closed-job-not-migrated", null), ActionListener.wrap(
                response -> fail("response not expected: " + response),
                exception -> {
                    assertThat(exception, instanceOf(ElasticsearchStatusException.class));
                }
        ));

    }

    public void testUpdateProcessOnCalendarChanged() {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job-1", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-2", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-3", "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        // For the JobConfigProvider expand groups search.
        // The search will not return any results
        mockClientBuilder.prepareSearchFields(AnomalyDetectorsIndex.configIndexName(), Collections.emptyList());

        JobManager jobManager = createJobManager(mockClientBuilder.build());

        jobManager.updateProcessOnCalendarChanged(Arrays.asList("job-1", "job-3", "job-4"),
                ActionListener.wrap(
                        r -> {},
                        e -> fail(e.getMessage())
                ));

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any());

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo("job-1"));
        assertThat(capturedUpdateParams.get(0).isUpdateScheduledEvents(), is(true));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo("job-3"));
        assertThat(capturedUpdateParams.get(1).isUpdateScheduledEvents(), is(true));
    }

    public void testUpdateProcessOnCalendarChanged_GivenGroups() throws IOException {
        PersistentTasksCustomMetaData.Builder tasksBuilder =  PersistentTasksCustomMetaData.builder();
        addJobTask("job-1", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-2", "node_id", JobState.OPENED, tasksBuilder);
        addJobTask("job-3", "node_id", JobState.OPENED, tasksBuilder);

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name"))
                .metaData(MetaData.builder()
                        .putCustom(PersistentTasksCustomMetaData.TYPE, tasksBuilder.build()))
                .build();
        when(clusterService.state()).thenReturn(clusterState);

        MockClientBuilder mockClientBuilder = new MockClientBuilder("cluster-test");
        // For the JobConfigProvider expand groups search.
        // group-1 will expand to job-1 and job-2
        List<Map<String, DocumentField>> fieldHits = new ArrayList<>();
        fieldHits.add(Collections.singletonMap(Job.ID.getPreferredName(),
                new DocumentField(Job.ID.getPreferredName(), Collections.singletonList("job-1"))));
        fieldHits.add(Collections.singletonMap(Job.ID.getPreferredName(),
                new DocumentField(Job.ID.getPreferredName(), Collections.singletonList("job-2"))));


        mockClientBuilder.prepareSearchFields(AnomalyDetectorsIndex.configIndexName(), fieldHits);
        JobManager jobManager = createJobManager(mockClientBuilder.build());

        jobManager.updateProcessOnCalendarChanged(Collections.singletonList("group-1"),
                ActionListener.wrap(
                        r -> {},
                        e -> fail(e.getMessage())
                ));

        ArgumentCaptor<UpdateParams> updateParamsCaptor = ArgumentCaptor.forClass(UpdateParams.class);
        verify(updateJobProcessNotifier, times(2)).submitJobUpdate(updateParamsCaptor.capture(), any());

        List<UpdateParams> capturedUpdateParams = updateParamsCaptor.getAllValues();
        assertThat(capturedUpdateParams.size(), equalTo(2));
        assertThat(capturedUpdateParams.get(0).getJobId(), equalTo("job-1"));
        assertThat(capturedUpdateParams.get(0).isUpdateScheduledEvents(), is(true));
        assertThat(capturedUpdateParams.get(1).getJobId(), equalTo("job-2"));
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

    private JobManager createJobManager(Client client) {
        return new JobManager(
            environment,
            environment.settings(),
            jobResultsProvider,
            jobResultsPersister,
            clusterService,
            auditor,
            threadPool,
            client,
            updateJobProcessNotifier,
            xContentRegistry());
    }

    private ClusterState createClusterState() {
        ClusterState.Builder builder = ClusterState.builder(new ClusterName("_name"));
        builder.metaData(MetaData.builder());
        return builder.build();
    }

    private BytesReference toBytesReference(ToXContent content) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            content.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(xContentBuilder);
        }
    }

    private void givenClusterSettings(Settings settings) {
        ClusterSettings clusterSettings = new ClusterSettings(settings, new HashSet<>(Arrays.asList(
            MachineLearningField.MAX_MODEL_MEMORY_LIMIT,
            MlConfigMigrationEligibilityCheck.ENABLE_CONFIG_MIGRATION)));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }
}
