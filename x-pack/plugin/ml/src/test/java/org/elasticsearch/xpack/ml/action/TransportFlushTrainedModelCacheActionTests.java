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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.FlushTrainedModelCacheAction;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelCacheMetadataService;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportFlushTrainedModelCacheActionTests extends ESTestCase {

    private ThreadPool threadPool;
    private TrainedModelCacheMetadataService modelCacheMetadataService;

    @Before
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void setupMocks() {
        threadPool = new TestThreadPool(getTestName());
        modelCacheMetadataService = mock(TrainedModelCacheMetadataService.class);
        doAnswer(invocationOnMock -> {
            ActionListener listener = invocationOnMock.getArgument(0, ActionListener.class);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(modelCacheMetadataService).updateCacheVersion(any(ActionListener.class));
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
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
        verify(modelCacheMetadataService).updateCacheVersion(listener);
    }

    private TransportFlushTrainedModelCacheAction createAction(ClusterService clusterService) {
        return new TransportFlushTrainedModelCacheAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            modelCacheMetadataService
        );
    }
}
