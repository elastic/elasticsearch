/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.junit.Before;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportFinalizeJobExecutionActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private Client client;
    private ClusterService clusterService;

    @Before
    @SuppressWarnings("unchecked")
    private void setupMocks() {
        ExecutorService executorService = mock(ExecutorService.class);
        threadPool = mock(ThreadPool.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        client = mock(Client.class);
        doAnswer( invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return null;
        }).when(client).execute(eq(UpdateAction.INSTANCE), any(), any());
        when(client.threadPool()).thenReturn(threadPool);

        clusterService = mock(ClusterService.class);

        doAnswer( invocationOnMock -> {
            ClusterStateUpdateTask updateTask = (ClusterStateUpdateTask)invocationOnMock.getArguments()[1];
            updateTask.clusterStateProcessed(null, null, null);
            return null;
        }).when(clusterService).submitStateUpdateTask(anyString(), any());
    }

    public void testOperation_noJobsInClusterState() {
        TransportFinalizeJobExecutionAction action = createAction();

        ClusterState clusterState = ClusterState.builder(new ClusterName("finalize-job-action-tests")).build();

        FinalizeJobExecutionAction.Request request = new FinalizeJobExecutionAction.Request(new String[]{"index-job1", "index-job2"});
        AtomicReference<AcknowledgedResponse> ack = new AtomicReference<>();
        action.masterOperation(request, clusterState, ActionListener.wrap(
                ack::set,
                e -> assertNull(e.getMessage())
        ));

        assertTrue(ack.get().isAcknowledged());
        verify(client, times(2)).execute(eq(UpdateAction.INSTANCE), any(), any());
        verify(clusterService, never()).submitStateUpdateTask(any(), any());
    }

    public void testOperation_jobInClusterState() {
        TransportFinalizeJobExecutionAction action = createAction();

        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createFareQuoteJob("cs-job").build(new Date()), false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("finalize-job-action-tests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlBuilder.build()))
                .build();

        FinalizeJobExecutionAction.Request request = new FinalizeJobExecutionAction.Request(new String[]{"cs-job"});
        AtomicReference<AcknowledgedResponse> ack = new AtomicReference<>();
        action.masterOperation(request, clusterState, ActionListener.wrap(
                ack::set,
                e -> fail(e.getMessage())
        ));

        assertTrue(ack.get().isAcknowledged());
        verify(client, never()).execute(eq(UpdateAction.INSTANCE), any(), any());
        verify(clusterService, times(1)).submitStateUpdateTask(any(), any());
    }

    public void testOperation_jobsInBothClusterAndIndex() {
        TransportFinalizeJobExecutionAction action = createAction();

        MlMetadata.Builder mlBuilder = new MlMetadata.Builder();
        mlBuilder.putJob(BaseMlIntegTestCase.createFareQuoteJob("cs-job").build(new Date()), false);

        ClusterState clusterState = ClusterState.builder(new ClusterName("finalize-job-action-tests"))
                .metaData(MetaData.builder()
                        .putCustom(MlMetadata.TYPE, mlBuilder.build()))
                .build();

        FinalizeJobExecutionAction.Request request =
                new FinalizeJobExecutionAction.Request(new String[]{"index-job", "cs-job"});
        AtomicReference<AcknowledgedResponse> ack = new AtomicReference<>();
        action.masterOperation(request, clusterState, ActionListener.wrap(
                ack::set,
                e -> assertNull(e.getMessage())
        ));

        assertTrue(ack.get().isAcknowledged());
        // The job in the clusterstate should not be updated in the index
        verify(client, times(1)).execute(eq(UpdateAction.INSTANCE), any(), any());
        verify(clusterService, times(1)).submitStateUpdateTask(any(), any());
    }

    private TransportFinalizeJobExecutionAction createAction() {
        return new TransportFinalizeJobExecutionAction(Settings.EMPTY, mock(TransportService.class), clusterService,
                threadPool, mock(ActionFilters.class), mock(IndexNameExpressionResolver.class), client);

    }
}
