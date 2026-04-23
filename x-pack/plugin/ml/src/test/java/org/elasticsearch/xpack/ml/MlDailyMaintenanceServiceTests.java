/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobTaskState;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class MlDailyMaintenanceServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private Client client;
    private ClusterService clusterService;
    private AnomalyDetectionAuditor auditor;
    private MlAssignmentNotifier mlAssignmentNotifier;

    @Before
    public void setUpTests() {
        threadPool = new TestThreadPool("MlDailyMaintenanceServiceTests");
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        clusterService = mock(ClusterService.class);
        auditor = mock(AnomalyDetectionAuditor.class);
        mlAssignmentNotifier = mock(MlAssignmentNotifier.class);
    }

    @After
    public void stop() {
        terminate(threadPool);
    }

    public void testScheduledTriggering() throws InterruptedException {
        when(clusterService.state()).thenReturn(createClusterState(false));
        doAnswer(withResponse(new DeleteExpiredDataAction.Response(true))).when(client)
            .execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        doAnswer(withResponse(new GetJobsAction.Response(new QueryPage<>(Collections.emptyList(), 0, new ParseField(""))))).when(client)
            .execute(same(GetJobsAction.INSTANCE), any(), any());

        int triggerCount = randomIntBetween(1, 3);
        executeMaintenanceTriggers(triggerCount);

        verify(client, times(triggerCount)).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(client, times(2 * triggerCount)).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier, times(triggerCount)).auditUnassignedMlTasks(eq(Metadata.DEFAULT_PROJECT_ID), any(), any());
    }

    public void testScheduledTriggeringWhileUpgradeModeIsEnabled() throws InterruptedException {
        when(clusterService.state()).thenReturn(createClusterState(true));

        int triggerCount = randomIntBetween(1, 3);
        executeMaintenanceTriggers(triggerCount);

        verify(clusterService, times(triggerCount)).state();
        verifyNoMoreInteractions(client, clusterService, mlAssignmentNotifier);
    }

    public void testBothTasksAreTriggered_BothTasksSucceed() throws InterruptedException {
        assertThatBothTasksAreTriggered(
            withResponse(new DeleteExpiredDataAction.Response(true)),
            withResponse(new GetJobsAction.Response(new QueryPage<>(Collections.emptyList(), 0, new ParseField(""))))
        );
    }

    public void testBothTasksAreTriggered_DeleteExpiredDataTaskFails() throws InterruptedException {
        assertThatBothTasksAreTriggered(
            withResponse(new DeleteExpiredDataAction.Response(false)),
            withResponse(new GetJobsAction.Response(new QueryPage<>(Collections.emptyList(), 0, new ParseField(""))))
        );
    }

    public void testBothTasksAreTriggered_DeleteExpiredDataTaskFailsWithException() throws InterruptedException {
        assertThatBothTasksAreTriggered(
            withException(new ElasticsearchException("exception thrown by DeleteExpiredDataAction")),
            withResponse(new GetJobsAction.Response(new QueryPage<>(Collections.emptyList(), 0, new ParseField(""))))
        );
    }

    public void testBothTasksAreTriggered_DeleteJobsTaskFails() throws InterruptedException {
        assertThatBothTasksAreTriggered(
            withResponse(new DeleteExpiredDataAction.Response(true)),
            withException(new ElasticsearchException("exception thrown by GetJobsAction"))
        );
    }

    public void testBothTasksAreTriggered_BothTasksFail() throws InterruptedException {
        assertThatBothTasksAreTriggered(
            withException(new ElasticsearchException("exception thrown by DeleteExpiredDataAction")),
            withException(new ElasticsearchException("exception thrown by GetJobsAction"))
        );
    }

    public void testNoAnomalyDetectionTasksWhenDisabled() throws InterruptedException {
        when(clusterService.state()).thenReturn(createClusterState(false));

        executeMaintenanceTriggers(1, false, randomBoolean(), randomBoolean(), randomBoolean());

        verify(client, never()).threadPool();
        verify(client, never()).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(client, never()).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier, Mockito.atLeast(1)).auditUnassignedMlTasks(eq(Metadata.DEFAULT_PROJECT_ID), any(), any());
    }

    private void assertThatBothTasksAreTriggered(Answer<?> deleteExpiredDataAnswer, Answer<?> getJobsAnswer) throws InterruptedException {
        when(clusterService.state()).thenReturn(createClusterState(false));
        doAnswer(deleteExpiredDataAnswer).when(client).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        doAnswer(getJobsAnswer).when(client).execute(same(GetJobsAction.INSTANCE), any(), any());

        executeMaintenanceTriggers(1);

        verify(client, times(3)).threadPool();
        verify(client, times(1)).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(client, times(2)).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier, Mockito.atLeast(1)).auditUnassignedMlTasks(eq(Metadata.DEFAULT_PROJECT_ID), any(), any());
    }

    public void testJobInDeletingStateAlreadyHasDeletionTask() throws InterruptedException {
        String jobId = "job-in-state-deleting";
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("test", 123),
            "test",
            "test",
            DeleteJobAction.NAME,
            "delete-job-" + jobId,
            null,
            0,
            0,
            true,
            false,
            new TaskId("test", 456),
            Collections.emptyMap()
        );

        when(clusterService.state()).thenReturn(createClusterState(false));
        doAnswer(withResponse(new DeleteExpiredDataAction.Response(true))).when(client)
            .execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(jobId);
        when(job.isDeleting()).thenReturn(true);
        doAnswer(withResponse(new GetJobsAction.Response(new QueryPage<>(Collections.singletonList(job), 1, new ParseField(""))))).when(
            client
        ).execute(same(GetJobsAction.INSTANCE), any(), any());
        doAnswer(withResponse(new ListTasksResponse(Collections.singletonList(taskInfo), Collections.emptyList(), Collections.emptyList())))
            .when(client)
            .execute(same(TransportListTasksAction.TYPE), any(), any());

        executeMaintenanceTriggers(1);

        verify(client, times(4)).threadPool();
        verify(client, times(2)).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(client).execute(same(TransportListTasksAction.TYPE), any(), any());
        verify(client).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier).auditUnassignedMlTasks(eq(Metadata.DEFAULT_PROJECT_ID), any(), any());
        verifyNoMoreInteractions(client, mlAssignmentNotifier);
    }

    public void testJobGetsDeleted() throws InterruptedException {
        testJobInDeletingStateDoesNotHaveDeletionTask(true);
    }

    public void testJobDoesNotGetDeleted() throws InterruptedException {
        testJobInDeletingStateDoesNotHaveDeletionTask(false);
    }

    private void testJobInDeletingStateDoesNotHaveDeletionTask(boolean deleted) throws InterruptedException {
        String jobId = "job-in-state-deleting";
        when(clusterService.state()).thenReturn(createClusterState(false));
        doAnswer(withResponse(new DeleteExpiredDataAction.Response(true))).when(client)
            .execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(jobId);
        when(job.isDeleting()).thenReturn(true);
        doAnswer(withResponse(new GetJobsAction.Response(new QueryPage<>(Collections.singletonList(job), 1, new ParseField(""))))).when(
            client
        ).execute(same(GetJobsAction.INSTANCE), any(), any());
        doAnswer(withResponse(new ListTasksResponse(Collections.emptyList(), Collections.emptyList(), Collections.emptyList()))).when(
            client
        ).execute(same(TransportListTasksAction.TYPE), any(), any());
        doAnswer(withResponse(AcknowledgedResponse.of(deleted))).when(client).execute(same(DeleteJobAction.INSTANCE), any(), any());

        executeMaintenanceTriggers(1);

        verify(client, times(5)).threadPool();
        verify(client, times(2)).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(client).execute(same(TransportListTasksAction.TYPE), any(), any());
        verify(client).execute(same(DeleteJobAction.INSTANCE), any(), any());
        verify(client).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier).auditUnassignedMlTasks(eq(Metadata.DEFAULT_PROJECT_ID), any(), any());
        verifyNoMoreInteractions(client, mlAssignmentNotifier);
    }

    public void testJobInResettingState_doesNotHaveResetTask() throws InterruptedException {
        testJobInResettingState(false);
    }

    public void testJobInResettingState_hasResetTask() throws InterruptedException {
        testJobInResettingState(true);
    }

    private void testJobInResettingState(boolean hasResetTask) throws InterruptedException {
        String jobId = "job-in-state-resetting";
        when(clusterService.state()).thenReturn(createClusterState(false));
        doAnswer(withResponse(new DeleteExpiredDataAction.Response(true))).when(client)
            .execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        Job job = mock(Job.class);
        when(job.getId()).thenReturn(jobId);
        when(job.isDeleting()).thenReturn(false);
        when(job.isResetting()).thenReturn(true);
        doAnswer(withResponse(new GetJobsAction.Response(new QueryPage<>(List.of(job), 1, new ParseField(""))))).when(client)
            .execute(same(GetJobsAction.INSTANCE), any(), any());
        List<TaskInfo> tasks = hasResetTask
            ? List.of(
                new TaskInfo(
                    new TaskId("test", 123),
                    "test",
                    "test",
                    ResetJobAction.NAME,
                    "job-" + jobId,
                    null,
                    0,
                    0,
                    true,
                    false,
                    new TaskId("test", 456),
                    Collections.emptyMap()
                )
            )
            : List.of();
        doAnswer(withResponse(new ListTasksResponse(tasks, List.of(), List.of()))).when(client)
            .execute(same(TransportListTasksAction.TYPE), any(), any());
        doAnswer(withResponse(AcknowledgedResponse.of(true))).when(client).execute(same(ResetJobAction.INSTANCE), any(), any());

        executeMaintenanceTriggers(1);

        verify(client, times(hasResetTask ? 4 : 5)).threadPool();
        verify(client, times(2)).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(client).execute(same(TransportListTasksAction.TYPE), any(), any());
        if (hasResetTask == false) {
            verify(client).execute(same(ResetJobAction.INSTANCE), any(), any());
        }
        verify(client).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier).auditUnassignedMlTasks(eq(Metadata.DEFAULT_PROJECT_ID), any(), any());
        verifyNoMoreInteractions(client, mlAssignmentNotifier);
    }

    public void testHasIlm() {
        List<IlmTestCase> testCases = List.of(
            new IlmTestCase(true, true, true, "ILM should be active when both settings are enabled"),
            new IlmTestCase(false, false, true, "ILM should be inactive when index policy is missing"),
            new IlmTestCase(false, true, false, "ILM should be inactive when ML setting is disabled"),
            new IlmTestCase(false, false, false, "ILM should be inactive when both settings are disabled")
        );

        String indexName = randomAlphaOfLength(10);
        for (IlmTestCase testCase : testCases) {
            MlDailyMaintenanceService service = createService(indexName, testCase.hasIlmPolicy, testCase.isIlmEnabled);
            assertThat(testCase.description, service.hasIlm(indexName), equalTo(testCase.expected));
        }
    }

    public void testCloseIdleJobsDisabledWhenTimeoutIsNegative() throws InterruptedException {
        MlDailyMaintenanceService service = createMaintenanceService();
        service.setIdleJobAutoCloseTimeout(TimeValue.MINUS_ONE);

        CountDownLatch latch = new CountDownLatch(1);
        service.triggerCloseIdleJobsWithStoppedDatafeeds(ActionListener.wrap(r -> {
            assertTrue(r.isAcknowledged());
            latch.countDown();
        }, e -> fail(e.getMessage())));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        verify(client, never()).execute(same(CloseJobAction.INSTANCE), any(), any());
    }

    public void testCloseIdleJobsNoCandidatesWhenAllHaveDatafeeds() throws InterruptedException {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "job-1", JobState.OPENED);
        addDatafeedTask(tasksBuilder, "datafeed-1", "job-1");

        when(clusterService.state()).thenReturn(createClusterStateWithTasks(tasksBuilder.build()));

        MlDailyMaintenanceService service = createMaintenanceService();

        CountDownLatch latch = new CountDownLatch(1);
        service.triggerCloseIdleJobsWithStoppedDatafeeds(ActionListener.wrap(r -> {
            assertTrue(r.isAcknowledged());
            latch.countDown();
        }, e -> fail(e.getMessage())));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        verify(client, never()).execute(same(TransportSearchAction.TYPE), any(), any());
        verify(client, never()).execute(same(CloseJobAction.INSTANCE), any(), any());
    }

    public void testCloseIdleJobsClosesJobWithOldData() throws InterruptedException {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "idle-job", JobState.OPENED);

        when(clusterService.state()).thenReturn(createClusterStateWithTasks(tasksBuilder.build()));
        doAnswer(withDatafeedResponse("idle-job")).when(client).execute(same(GetDatafeedsAction.INSTANCE), any(), any());

        long threeDaysAgo = threadPool.absoluteTimeInMillis() - TimeValue.timeValueHours(72).millis();
        doAnswer(withSearchResponse("idle-job", threeDaysAgo)).when(client).execute(same(TransportSearchAction.TYPE), any(), any());
        doAnswer(withResponse(new CloseJobAction.Response(true))).when(client).execute(same(CloseJobAction.INSTANCE), any(), any());

        MlDailyMaintenanceService service = createMaintenanceService();

        CountDownLatch latch = new CountDownLatch(1);
        service.triggerCloseIdleJobsWithStoppedDatafeeds(ActionListener.wrap(r -> {
            assertTrue(r.isAcknowledged());
            latch.countDown();
        }, e -> fail(e.getMessage())));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        verify(client).execute(same(CloseJobAction.INSTANCE), any(), any());
    }

    public void testCloseIdleJobsSkipsJobWithRecentData() throws InterruptedException {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "active-job", JobState.OPENED);

        when(clusterService.state()).thenReturn(createClusterStateWithTasks(tasksBuilder.build()));
        doAnswer(withDatafeedResponse("active-job")).when(client).execute(same(GetDatafeedsAction.INSTANCE), any(), any());

        long oneHourAgo = threadPool.absoluteTimeInMillis() - TimeValue.timeValueHours(1).millis();
        doAnswer(withSearchResponse("active-job", oneHourAgo)).when(client).execute(same(TransportSearchAction.TYPE), any(), any());

        MlDailyMaintenanceService service = createMaintenanceService();

        CountDownLatch latch = new CountDownLatch(1);
        service.triggerCloseIdleJobsWithStoppedDatafeeds(ActionListener.wrap(r -> {
            assertTrue(r.isAcknowledged());
            latch.countDown();
        }, e -> fail(e.getMessage())));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        verify(client, never()).execute(same(CloseJobAction.INSTANCE), any(), any());
    }

    public void testCloseIdleJobsSkipsClosedJobs() throws InterruptedException {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "closed-job", JobState.CLOSED);

        when(clusterService.state()).thenReturn(createClusterStateWithTasks(tasksBuilder.build()));

        MlDailyMaintenanceService service = createMaintenanceService();

        CountDownLatch latch = new CountDownLatch(1);
        service.triggerCloseIdleJobsWithStoppedDatafeeds(ActionListener.wrap(r -> {
            assertTrue(r.isAcknowledged());
            latch.countDown();
        }, e -> fail(e.getMessage())));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        verify(client, never()).execute(same(TransportSearchAction.TYPE), any(), any());
        verify(client, never()).execute(same(CloseJobAction.INSTANCE), any(), any());
    }

    public void testCloseIdleJobsSkipsJobsWithoutConfiguredDatafeed() throws InterruptedException {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "no-datafeed-job", JobState.OPENED);

        when(clusterService.state()).thenReturn(createClusterStateWithTasks(tasksBuilder.build()));
        doAnswer(withDatafeedResponse()).when(client).execute(same(GetDatafeedsAction.INSTANCE), any(), any());

        MlDailyMaintenanceService service = createMaintenanceService();

        CountDownLatch latch = new CountDownLatch(1);
        service.triggerCloseIdleJobsWithStoppedDatafeeds(ActionListener.wrap(r -> {
            assertTrue(r.isAcknowledged());
            latch.countDown();
        }, e -> fail(e.getMessage())));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        verify(client, never()).execute(same(TransportSearchAction.TYPE), any(), any());
        verify(client, never()).execute(same(CloseJobAction.INSTANCE), any(), any());
    }

    public void testCloseIdleJobsSkipsJobWithNoDataCounts() throws InterruptedException {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "no-data-job", JobState.OPENED);

        when(clusterService.state()).thenReturn(createClusterStateWithTasks(tasksBuilder.build()));
        doAnswer(withDatafeedResponse("no-data-job")).when(client).execute(same(GetDatafeedsAction.INSTANCE), any(), any());

        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
            SearchResponse response = SearchResponseUtils.successfulResponse(SearchHits.EMPTY_WITH_TOTAL_HITS);
            try {
                listener.onResponse(response);
            } finally {
                response.decRef();
            }
            return null;
        }).when(client).execute(same(TransportSearchAction.TYPE), any(), any());

        MlDailyMaintenanceService service = createMaintenanceService();

        CountDownLatch latch = new CountDownLatch(1);
        service.triggerCloseIdleJobsWithStoppedDatafeeds(ActionListener.wrap(r -> {
            assertTrue(r.isAcknowledged());
            latch.countDown();
        }, e -> fail(e.getMessage())));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        verify(client, never()).execute(same(CloseJobAction.INSTANCE), any(), any());
    }

    private MlDailyMaintenanceService createMaintenanceService() {
        return new MlDailyMaintenanceService(
            Settings.EMPTY,
            threadPool,
            client,
            clusterService,
            auditor,
            mlAssignmentNotifier,
            () -> TimeValue.timeValueDays(1),
            TestIndexNameExpressionResolver.newInstance(),
            true,
            true,
            true,
            true
        );
    }

    private static void addJobTask(PersistentTasksCustomMetadata.Builder builder, String jobId, JobState state) {
        builder.addTask(
            MlTasks.jobTaskId(jobId),
            MlTasks.JOB_TASK_NAME,
            new OpenJobAction.JobParams(jobId),
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        );
        builder.updateTaskState(MlTasks.jobTaskId(jobId), new JobTaskState(state, builder.getLastAllocationId(), null, null));
    }

    private static void addDatafeedTask(PersistentTasksCustomMetadata.Builder builder, String datafeedId, String jobId) {
        StartDatafeedAction.DatafeedParams params = new StartDatafeedAction.DatafeedParams(datafeedId, 0);
        params.setJobId(jobId);
        builder.addTask(
            MlTasks.datafeedTaskId(datafeedId),
            MlTasks.DATAFEED_TASK_NAME,
            params,
            PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT
        );
    }

    private static ClusterState createClusterStateWithTasks(PersistentTasksCustomMetadata tasks) {
        return ClusterState.builder(new ClusterName("MlDailyMaintenanceServiceTests"))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, tasks)
                    .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().build())
            )
            .nodes(DiscoveryNodes.builder().build())
            .build();
    }

    @SuppressWarnings("unchecked")
    private static Answer<Void> withDatafeedResponse(String... jobIds) {
        return invocationOnMock -> {
            ActionListener<GetDatafeedsAction.Response> listener = (ActionListener<GetDatafeedsAction.Response>) invocationOnMock
                .getArguments()[2];
            List<DatafeedConfig> datafeeds = new java.util.ArrayList<>();
            for (String jobId : jobIds) {
                datafeeds.add(new DatafeedConfig.Builder("datafeed-" + jobId, jobId).setIndices(List.of("index")).build());
            }
            listener.onResponse(new GetDatafeedsAction.Response(new QueryPage<>(datafeeds, datafeeds.size(), new ParseField(""))));
            return null;
        };
    }

    @SuppressWarnings("unchecked")
    private static Answer<Void> withSearchResponse(String jobId, long latestRecordTimestamp) {
        return invocationOnMock -> {
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocationOnMock.getArguments()[2];
            try {
                XContentBuilder sourceBuilder = JsonXContent.contentBuilder();
                sourceBuilder.startObject();
                sourceBuilder.field("latest_record_timestamp", latestRecordTimestamp);
                sourceBuilder.endObject();
                SearchHit hit = SearchHit.unpooled(0);
                hit.sourceRef(BytesReference.bytes(sourceBuilder));
                SearchHits hits = SearchHits.unpooled(new SearchHit[] { hit }, null, 1.0f);
                SearchResponse response = SearchResponseUtils.successfulResponse(hits);
                try {
                    listener.onResponse(response);
                } finally {
                    response.decRef();
                }
            } catch (Exception e) {
                listener.onFailure(e);
            }
            return null;
        };
    }

    private MlDailyMaintenanceService createService(String indexName, boolean hasIlmPolicy, boolean isIlmEnabled) {
        AdminClient adminClient = mock(AdminClient.class);
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        @SuppressWarnings("unchecked")
        ActionFuture<GetIndexResponse> actionFuture = mock(ActionFuture.class);

        when(client.admin()).thenReturn(adminClient);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(indicesAdminClient.getIndex(any())).thenReturn(actionFuture);

        Settings.Builder indexSettings = Settings.builder();
        if (hasIlmPolicy) {
            indexSettings.put("index.lifecycle.name", "ml-policy");
        }
        GetIndexResponse getIndexResponse = new GetIndexResponse(
            new String[] { indexName },
            Map.of(),
            Map.of(),
            Map.of(indexName, indexSettings.build()),
            Map.of(),
            Map.of()
        );
        when(actionFuture.actionGet()).thenReturn(getIndexResponse);

        return new MlDailyMaintenanceService(
            Settings.EMPTY,
            threadPool,
            client,
            clusterService,
            auditor,
            mlAssignmentNotifier,
            () -> TimeValue.timeValueDays(1),
            TestIndexNameExpressionResolver.newInstance(),
            true,
            true,
            true,
            isIlmEnabled
        );
    }

    private record IlmTestCase(boolean expected, boolean hasIlmPolicy, boolean isIlmEnabled, String description) {}

    private void executeMaintenanceTriggers(int triggerCount) throws InterruptedException {
        executeMaintenanceTriggers(triggerCount, true, true, true, true);
    }

    private void executeMaintenanceTriggers(
        int triggerCount,
        boolean isAnomalyDetectionEnabled,
        boolean isDataFrameAnalyticsEnabled,
        boolean isNlpEnabled,
        boolean isIlmEnabled
    ) throws InterruptedException {
        // The scheduleProvider is called upon scheduling. The latch waits for (triggerCount + 1)
        // schedules to happen, which means that the maintenance task is executed triggerCount
        // times. The first triggerCount invocations of the scheduleProvider return 100ms, which
        // is the time between the executed maintenance tasks.
        // After that, maintenance task (triggerCount + 1) is scheduled after 100sec, the latch is
        // released, the service is closed, and the method returns. Task (triggerCount + 1) is
        // therefore never executed.
        CountDownLatch latch = new CountDownLatch(triggerCount + 1);
        Supplier<TimeValue> scheduleProvider = () -> {
            latch.countDown();
            return TimeValue.timeValueMillis(latch.getCount() > 0 ? 100 : 100_000);
        };
        try (
            MlDailyMaintenanceService service = new MlDailyMaintenanceService(
                Settings.EMPTY,
                threadPool,
                client,
                clusterService,
                auditor,
                mlAssignmentNotifier,
                scheduleProvider,
                TestIndexNameExpressionResolver.newInstance(),
                isAnomalyDetectionEnabled,
                isDataFrameAnalyticsEnabled,
                isNlpEnabled,
                isIlmEnabled
            )
        ) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }
    }

    public void testCollectOpenJobsWithoutRunningDatafeeds_noTasks() {
        ClusterState state = createClusterState(false);
        assertThat(MlDailyMaintenanceService.collectOpenJobsWithoutRunningDatafeeds(state), empty());
    }

    public void testCollectOpenJobsWithoutRunningDatafeeds_openJobWithRunningDatafeed() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "job-1", JobState.OPENED);
        addDatafeedTask(tasksBuilder, "datafeed-1", "job-1");
        ClusterState state = createClusterStateWithTasks(tasksBuilder.build());

        assertThat(MlDailyMaintenanceService.collectOpenJobsWithoutRunningDatafeeds(state), empty());
    }

    public void testCollectOpenJobsWithoutRunningDatafeeds_openJobWithoutDatafeed() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "job-1", JobState.OPENED);
        ClusterState state = createClusterStateWithTasks(tasksBuilder.build());

        assertThat(MlDailyMaintenanceService.collectOpenJobsWithoutRunningDatafeeds(state), contains("job-1"));
    }

    public void testCollectOpenJobsWithoutRunningDatafeeds_closedJobIsExcluded() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "job-open", JobState.OPENED);
        addJobTask(tasksBuilder, "job-closed", JobState.CLOSED);
        ClusterState state = createClusterStateWithTasks(tasksBuilder.build());

        Set<String> result = MlDailyMaintenanceService.collectOpenJobsWithoutRunningDatafeeds(state);
        assertThat(result, contains("job-open"));
        assertThat(result, not(hasItem("job-closed")));
    }

    public void testCollectOpenJobsWithoutRunningDatafeeds_mixedJobs() {
        PersistentTasksCustomMetadata.Builder tasksBuilder = PersistentTasksCustomMetadata.builder();
        addJobTask(tasksBuilder, "job-with-datafeed", JobState.OPENED);
        addDatafeedTask(tasksBuilder, "datafeed-1", "job-with-datafeed");
        addJobTask(tasksBuilder, "job-without-datafeed", JobState.OPENED);
        addJobTask(tasksBuilder, "job-closed", JobState.CLOSED);
        ClusterState state = createClusterStateWithTasks(tasksBuilder.build());

        Set<String> result = MlDailyMaintenanceService.collectOpenJobsWithoutRunningDatafeeds(state);
        assertThat(result, contains("job-without-datafeed"));
        assertThat(result, not(hasItem("job-with-datafeed")));
        assertThat(result, not(hasItem("job-closed")));
    }

    private static ClusterState createClusterState(boolean isUpgradeMode) {
        return ClusterState.builder(new ClusterName("MlDailyMaintenanceServiceTests"))
            .metadata(
                Metadata.builder()
                    .putCustom(PersistentTasksCustomMetadata.TYPE, PersistentTasksCustomMetadata.builder().build())
                    .putCustom(MlMetadata.TYPE, new MlMetadata.Builder().isUpgradeMode(isUpgradeMode).build())
            )
            .nodes(DiscoveryNodes.builder().build())
            .build();
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withException(Exception e) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onFailure(e);
            return null;
        };
    }
}
