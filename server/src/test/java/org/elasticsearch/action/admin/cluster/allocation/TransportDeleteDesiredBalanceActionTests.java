/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class TransportDeleteDesiredBalanceActionTests extends ESAllocationTestCase {

    public void testReturnsErrorIfAllocatorIsNotDesiredBalanced() throws Exception {

        var listener = spy(ActionListener.<ActionResponse.Empty>noop());

        new TransportDeleteDesiredBalanceAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            mock(AllocationService.class),
            mock(ShardsAllocator.class)
        ).masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), ClusterState.EMPTY_STATE, listener);

        ArgumentCaptor<ResourceNotFoundException> exceptionArgumentCaptor = ArgumentCaptor.forClass(ResourceNotFoundException.class);
        verify(listener).onFailure(exceptionArgumentCaptor.capture());

        final var exception = exceptionArgumentCaptor.getValue();
        assertEquals("Desired balance allocator is not in use, no desired balance found", exception.getMessage());
        assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
    }

    public void testDeleteDesiredBalance() throws Exception {

        var threadPool = new TestThreadPool(getTestName());

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("master")).localNodeId("master").masterNodeId("master").build())
            .build();

        var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool);

        var listener = new PlainActionFuture<ActionResponse.Empty>();
        var allocator = mock(DesiredBalanceShardsAllocator.class);
        var allocationService = mock(AllocationService.class);
        doAnswer(invocation -> invocation.getArgument(0, ClusterState.class)).when(allocationService)
            .reroute(any(), eq("reset-desired-balance"), any());

        var action = new TransportDeleteDesiredBalanceAction(
            mock(TransportService.class),
            clusterService,
            threadPool,
            mock(ActionFilters.class),
            mock(IndexNameExpressionResolver.class),
            allocationService,
            allocator
        );

        action.masterOperation(mock(Task.class), mock(DesiredBalanceRequest.class), clusterState, listener);
        ClusterServiceUtils.awaitNoPendingTasks(clusterService);

        try {
            assertThat(listener.get(), notNullValue());
            verify(allocator).resetDesiredBalance();
            verify(allocationService).reroute(any(), eq("reset-desired-balance"), any());
        } finally {
            clusterService.close();
            terminate(threadPool);
        }
    }
}
