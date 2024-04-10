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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.FlushTrainedModelCacheAction;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelCacheMetadataService;
import org.junit.Before;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportFlushTrainedModelCacheActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private TrainedModelCacheMetadataService modelCacheMetadataService;

    @Before
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setupMocks() {
        ExecutorService executorService = mock(ExecutorService.class);
        threadPool = mock(ThreadPool.class);
        org.mockito.Mockito.doAnswer(invocation -> {
            invocation.getArgument(0, Runnable.class).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));

        modelCacheMetadataService = mock(TrainedModelCacheMetadataService.class);
        doAnswer(invocationOnMock -> {
            ActionListener listener = invocationOnMock.getArgument(0, ActionListener.class);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(modelCacheMetadataService).refreshCacheVersion(any(ActionListener.class));
    }

    public void testOperation() {
        ClusterService clusterService = mock(ClusterService.class);
        TransportFlushTrainedModelCacheAction action = createAction(clusterService);

        ClusterState clusterState = ClusterState.builder(new ClusterName("flush-trained-model-cache-metadata-tests")).build();

        FlushTrainedModelCacheAction.Request request = new FlushTrainedModelCacheAction.Request();
        AtomicReference<AcknowledgedResponse> ack = new AtomicReference<>();
        ActionListener<AcknowledgedResponse> listener = ActionTestUtils.assertNoFailureListener(ack::set);

        action.masterOperation(null, request, clusterState, listener);

        assertTrue(ack.get().isAcknowledged());
        verify(modelCacheMetadataService).refreshCacheVersion(listener);
    }

    private TransportFlushTrainedModelCacheAction createAction(ClusterService clusterService) {
        return new TransportFlushTrainedModelCacheAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            modelCacheMetadataService
        );
    }
}
