/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.desiredbalance;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetDesiredBalanceActionTests extends ESTestCase {

    private final DesiredBalanceShardsAllocator desiredBalanceShardsAllocator = mock(DesiredBalanceShardsAllocator.class);
    private final ClusterState clusterState = mock(ClusterState.class);
    private final Metadata metadata = mock(Metadata.class);
    private final TransportGetDesiredBalanceAction transportGetDesiredBalanceAction = new TransportGetDesiredBalanceAction(
        mock(TransportService.class),
        mock(ClusterService.class),
        mock(ThreadPool.class),
        mock(ActionFilters.class),
        mock(IndexNameExpressionResolver.class),
        desiredBalanceShardsAllocator
    );
    @SuppressWarnings("unchecked")
    private final ActionListener<DesiredBalanceResponse> listener = mock(ActionListener.class);

    @Before
    public void setUpMocks() throws Exception {
        when(clusterState.metadata()).thenReturn(metadata);
    }

    public void testReturnsErrorIfAllocatorIsNotDesiredBalanced() throws Exception {
        when(metadata.settings()).thenReturn(Settings.builder().put("cluster.routing.allocation.type", "balanced").build());

        transportGetDesiredBalanceAction.masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<ResourceNotFoundException> exceptionArgumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(listener).onFailure(exceptionArgumentCaptor.capture());

        assertEquals(
            "Expected the shard balance allocator to be `desired_balance`, but got `balanced`",
            exceptionArgumentCaptor.getValue().getMessage()
        );
    }

    public void testReturnsErrorIfDesiredBalanceIsNotAvailable() throws Exception {
        when(metadata.settings()).thenReturn(Settings.builder().put("cluster.routing.allocation.type", "desired_balance").build());

        transportGetDesiredBalanceAction.masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);

        ArgumentCaptor<ResourceNotFoundException> exceptionArgumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(listener).onFailure(exceptionArgumentCaptor.capture());

        assertEquals("Desired balance is not computed yet", exceptionArgumentCaptor.getValue().getMessage());
    }
}
