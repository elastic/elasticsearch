/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.ResetJobAction;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
    private MlAssignmentNotifier mlAssignmentNotifier;

    @Before
    public void setUpTests() {
        threadPool = new TestThreadPool("MlDailyMaintenanceServiceTests");
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        clusterService = mock(ClusterService.class);
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

        executeMaintenanceTriggers(1, false, randomBoolean(), randomBoolean());

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

    private void executeMaintenanceTriggers(int triggerCount) throws InterruptedException {
        executeMaintenanceTriggers(triggerCount, true, true, true);
    }

    private void executeMaintenanceTriggers(
        int triggerCount,
        boolean isAnomalyDetectionEnabled,
        boolean isDataFrameAnalyticsEnabled,
        boolean isNlpEnabled
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
                mlAssignmentNotifier,
                scheduleProvider,
                isAnomalyDetectionEnabled,
                isDataFrameAnalyticsEnabled,
                isNlpEnabled
            )
        ) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }
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
