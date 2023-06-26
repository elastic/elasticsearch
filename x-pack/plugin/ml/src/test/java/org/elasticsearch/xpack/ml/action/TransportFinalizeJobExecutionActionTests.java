/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.junit.Before;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportFinalizeJobExecutionActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private Client client;

    @Before
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setupMocks() {
        ExecutorService executorService = mock(ExecutorService.class);
        threadPool = mock(ThreadPool.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);

        client = mock(Client.class);
        doAnswer(invocationOnMock -> {
            ActionListener listener = (ActionListener) invocationOnMock.getArguments()[2];
            listener.onResponse(null);
            return null;
        }).when(client).execute(eq(UpdateAction.INSTANCE), any(), any());

        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testOperation() {
        ClusterService clusterService = mock(ClusterService.class);
        TransportFinalizeJobExecutionAction action = createAction(clusterService);

        ClusterState clusterState = ClusterState.builder(new ClusterName("finalize-job-action-tests")).build();

        FinalizeJobExecutionAction.Request request = new FinalizeJobExecutionAction.Request(new String[] { "job1", "job2" });
        AtomicReference<AcknowledgedResponse> ack = new AtomicReference<>();
        action.masterOperation(null, request, clusterState, ActionTestUtils.assertNoFailureListener(ack::set));

        assertTrue(ack.get().isAcknowledged());
        verify(client, times(2)).execute(eq(UpdateAction.INSTANCE), any(), any());
        verify(clusterService, never()).submitUnbatchedStateUpdateTask(any(), any());
    }

    private TransportFinalizeJobExecutionAction createAction(ClusterService clusterService) {
        return new TransportFinalizeJobExecutionAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            client
        );

    }
}
