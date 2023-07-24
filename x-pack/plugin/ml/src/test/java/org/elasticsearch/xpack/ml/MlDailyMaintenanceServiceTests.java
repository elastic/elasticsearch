/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
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
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
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

        int triggerCount = randomIntBetween(2, 4);
        CountDownLatch latch = new CountDownLatch(triggerCount);
        try (MlDailyMaintenanceService service = createService(latch, client)) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }

        verify(client, times(triggerCount - 1)).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(client, times(triggerCount - 1)).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier, times(triggerCount - 1)).auditUnassignedMlTasks(any(), any());
    }

    public void testScheduledTriggeringWhileUpgradeModeIsEnabled() throws InterruptedException {
        when(clusterService.state()).thenReturn(createClusterState(true));

        int triggerCount = randomIntBetween(2, 4);
        CountDownLatch latch = new CountDownLatch(triggerCount);
        try (MlDailyMaintenanceService service = createService(latch, client)) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }

        verify(clusterService, times(triggerCount - 1)).state();
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

        CountDownLatch latch = new CountDownLatch(2);
        try (MlDailyMaintenanceService service = createService(latch, client, false, randomBoolean(), randomBoolean())) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }

        verify(client, never()).threadPool();
        verify(client, never()).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(client, never()).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier, Mockito.atLeast(1)).auditUnassignedMlTasks(any(), any());
    }

    private void assertThatBothTasksAreTriggered(Answer<?> deleteExpiredDataAnswer, Answer<?> getJobsAnswer) throws InterruptedException {
        when(clusterService.state()).thenReturn(createClusterState(false));
        doAnswer(deleteExpiredDataAnswer).when(client).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        doAnswer(getJobsAnswer).when(client).execute(same(GetJobsAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(2);
        try (MlDailyMaintenanceService service = createService(latch, client)) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }

        verify(client, Mockito.atLeast(2)).threadPool();
        verify(client, Mockito.atLeast(1)).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(client, Mockito.atLeast(1)).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier, Mockito.atLeast(1)).auditUnassignedMlTasks(any(), any());
    }

    public void testJobInDeletingStateAlreadyHasDeletionTask() throws InterruptedException {
        String jobId = "job-in-state-deleting";
        TaskInfo taskInfo = new TaskInfo(
            new TaskId("test", 123),
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
            .execute(same(ListTasksAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(2);
        try (MlDailyMaintenanceService service = createService(latch, client)) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }

        verify(client, times(3)).threadPool();
        verify(client).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(client).execute(same(ListTasksAction.INSTANCE), any(), any());
        verify(client).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier).auditUnassignedMlTasks(any(), any());
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
        ).execute(same(ListTasksAction.INSTANCE), any(), any());
        doAnswer(withResponse(AcknowledgedResponse.of(deleted))).when(client).execute(same(DeleteJobAction.INSTANCE), any(), any());

        CountDownLatch latch = new CountDownLatch(2);
        try (MlDailyMaintenanceService service = createService(latch, client)) {
            service.start();
            latch.await(5, TimeUnit.SECONDS);
        }

        verify(client, times(4)).threadPool();
        verify(client).execute(same(GetJobsAction.INSTANCE), any(), any());
        verify(client).execute(same(ListTasksAction.INSTANCE), any(), any());
        verify(client).execute(same(DeleteJobAction.INSTANCE), any(), any());
        verify(client).execute(same(DeleteExpiredDataAction.INSTANCE), any(), any());
        verify(mlAssignmentNotifier).auditUnassignedMlTasks(any(), any());
        verifyNoMoreInteractions(client, mlAssignmentNotifier);
    }

    private MlDailyMaintenanceService createService(CountDownLatch latch, Client client) {
        return createService(latch, client, true, true, true);
    }

    private MlDailyMaintenanceService createService(
        CountDownLatch latch,
        Client client,
        boolean isAnomalyDetectionEnabled,
        boolean isDataFrameAnalyticsEnabled,
        boolean isNlpEnabled
    ) {
        return new MlDailyMaintenanceService(Settings.EMPTY, threadPool, client, clusterService, mlAssignmentNotifier, () -> {
            // We need to be careful that an unexpected iteration doesn't get squeezed in by the maintenance threadpool in
            // between the latch getting counted down to zero and the main test thread stopping the maintenance service.
            // This could happen if the main test thread happens to be waiting for a CPU for the whole 100ms after the
            // latch counts down to zero.
            if (latch.getCount() > 0) {
                latch.countDown();
                return TimeValue.timeValueMillis(100);
            } else {
                return TimeValue.timeValueHours(1);
            }
        }, isAnomalyDetectionEnabled, isDataFrameAnalyticsEnabled, isNlpEnabled);
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
