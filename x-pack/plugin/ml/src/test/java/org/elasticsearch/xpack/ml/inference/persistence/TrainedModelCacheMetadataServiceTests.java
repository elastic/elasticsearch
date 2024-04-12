/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.persistence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.action.FlushTrainedModelCacheAction;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelCacheMetadataService.CacheMetadataUpdateTask;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelCacheMetadataService.CacheMetadataUpdateTaskExecutor;
import org.elasticsearch.xpack.ml.inference.persistence.TrainedModelCacheMetadataService.RefreshCacheMetadataVersionTask;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TrainedModelCacheMetadataServiceTests extends ESTestCase {
    private ClusterService clusterService;
    private Client client;
    private MasterServiceTaskQueue<ClusterStateTaskListener> taskQueue;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpMocks() {
        clusterService = mockClusterService();
        client = mockClient();
        taskQueue = (MasterServiceTaskQueue<ClusterStateTaskListener>) mock(MasterServiceTaskQueue.class);

        Mockito.when(clusterService.createTaskQueue(eq(TrainedModelCacheMetadataService.TASK_QUEUE_NAME), any(), any()))
            .thenReturn(taskQueue);
    }

    public void testRefreshCacheVersionOnMasterNode() {
        final var taskExecutorCaptor = ArgumentCaptor.forClass(CacheMetadataUpdateTaskExecutor.class);
        final TrainedModelCacheMetadataService modelCacheMetadataService = new TrainedModelCacheMetadataService(clusterService, client);
        verify(clusterService).createTaskQueue(eq(TrainedModelCacheMetadataService.TASK_QUEUE_NAME), any(), taskExecutorCaptor.capture());

        DiscoveryNodes clusterNodes = mock(DiscoveryNodes.class);
        when(clusterNodes.getMasterNode()).thenReturn(mock(DiscoveryNode.class));
        when(clusterNodes.isLocalNodeElectedMaster()).thenReturn(true);

        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.clusterRecovered()).thenReturn(true);
        when(clusterState.nodes()).thenReturn(clusterNodes);

        modelCacheMetadataService.clusterChanged(new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE));

        @SuppressWarnings("unchecked")
        final var listener = (ActionListener<AcknowledgedResponse>) mock(ActionListener.class);
        modelCacheMetadataService.refreshCacheVersion(listener);

        ArgumentCaptor<CacheMetadataUpdateTask> updateTaskCaptor = ArgumentCaptor.forClass(RefreshCacheMetadataVersionTask.class);
        verify(taskQueue).submitTask(any(String.class), updateTaskCaptor.capture(), isNull());
        assertEquals(updateTaskCaptor.getValue().listener, listener);

        verify(client, never()).execute(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testRefreshCacheVersionOnNonMasterNode() {
        final var taskExecutorCaptor = ArgumentCaptor.forClass(CacheMetadataUpdateTaskExecutor.class);
        final TrainedModelCacheMetadataService modelCacheMetadataService = new TrainedModelCacheMetadataService(clusterService, client);
        verify(clusterService).createTaskQueue(eq(TrainedModelCacheMetadataService.TASK_QUEUE_NAME), any(), taskExecutorCaptor.capture());

        doAnswer(invocationOnMock -> {
            ActionListener<AcknowledgedResponse> listener = invocationOnMock.getArgument(2, ActionListener.class);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(any(ActionType.class), any(FlushTrainedModelCacheAction.Request.class), any(ActionListener.class));

        @SuppressWarnings("unchecked")
        final var listener = (ActionListener<AcknowledgedResponse>) mock(ActionListener.class);
        modelCacheMetadataService.refreshCacheVersion(listener);

        verify(client).execute(
            eq(FlushTrainedModelCacheAction.INSTANCE),
            any(FlushTrainedModelCacheAction.Request.class),
            any(ActionListener.class)
        );
        verify(listener).onResponse(eq(AcknowledgedResponse.TRUE));

        verify(taskQueue, never()).submitTask(any(String.class), any(RefreshCacheMetadataVersionTask.class), any(TimeValue.class));
    }

    private static Client mockClient() {
        final Client client = mock(Client.class);
        ThreadPool threadpool = mock(ThreadPool.class);
        when(threadpool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(client.threadPool()).thenReturn(threadpool);
        return client;
    }

    private static ClusterService mockClusterService() {
        final ClusterState clusterState = mock(ClusterState.class);
        Mockito.when(clusterState.metadata()).thenReturn(Metadata.EMPTY_METADATA);

        final ClusterService clusterService = mock(ClusterService.class);
        Mockito.when(clusterService.state()).thenReturn(clusterState);
        Mockito.when(clusterService.getClusterName()).thenReturn(ClusterName.DEFAULT);

        return clusterService;
    }
}
