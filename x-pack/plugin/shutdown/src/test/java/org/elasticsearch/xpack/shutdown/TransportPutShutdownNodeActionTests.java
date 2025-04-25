/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskExecutor.TaskContext;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.TransportPutShutdownNodeAction.PutShutdownNodeExecutor;
import org.elasticsearch.xpack.shutdown.TransportPutShutdownNodeAction.PutShutdownNodeTask;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransportPutShutdownNodeActionTests extends ESTestCase {

    private ClusterService clusterService;
    private TransportPutShutdownNodeAction action;

    // must use member mock for generic
    @Mock
    private TaskContext<PutShutdownNodeTask> taskContext;

    @Mock
    private MasterServiceTaskQueue<PutShutdownNodeTask> taskQueue;

    @Before
    public void init() {
        MockitoAnnotations.openMocks(this);
        // TODO: it takes almost 2 seconds to create these mocks....WHY?!?
        var threadPool = mock(ThreadPool.class);
        var transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        clusterService = mock(ClusterService.class);
        var allocationService = mock(AllocationService.class);
        when(allocationService.reroute(any(ClusterState.class), anyString(), any())).then(invocation -> invocation.getArgument(0));
        var actionFilters = mock(ActionFilters.class);
        when(clusterService.createTaskQueue(any(), any(), Mockito.<ClusterStateTaskExecutor<PutShutdownNodeTask>>any())).thenReturn(
            taskQueue
        );
        action = new TransportPutShutdownNodeAction(transportService, clusterService, allocationService, threadPool, actionFilters);
    }

    public void testNoop() throws Exception {
        var type = randomFrom(Type.REMOVE, Type.REPLACE, Type.RESTART);
        var allocationDelay = type == Type.RESTART ? TimeValue.timeValueMinutes(randomIntBetween(1, 3)) : null;
        var targetNodeName = type == Type.REPLACE ? randomAlphaOfLength(5) : null;
        var request = new PutShutdownNodeAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "node1",
            type,
            "sunsetting",
            allocationDelay,
            targetNodeName,
            null
        );
        var dummyNode = new DiscoveryNode(targetNodeName, "node1", "eph-node1", "abc", "abc", null, Map.of(), Set.of(), null);
        var state = ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(dummyNode).build()).build();
        action.masterOperation(null, request, state, ActionListener.noop());
        var updateTask = ArgumentCaptor.forClass(PutShutdownNodeTask.class);
        var taskExecutor = ArgumentCaptor.forClass(PutShutdownNodeExecutor.class);
        verify(clusterService).createTaskQueue(any(), any(), taskExecutor.capture());
        verify(taskQueue).submitTask(any(), updateTask.capture(), any());
        when(taskContext.getTask()).thenReturn(updateTask.getValue());
        ClusterState stableState = taskExecutor.getValue()
            .execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(state, List.of(taskContext), () -> null));

        // run the request again, there should be no call to submit an update task
        clearTaskQueueInvocations();
        action.masterOperation(null, request, stableState, ActionListener.noop());
        verifyNoInteractions(taskQueue);

        // run the request again with empty state, the update task should return the same state
        action.masterOperation(null, request, ClusterState.EMPTY_STATE, ActionListener.noop());
        verify(taskQueue).submitTask(any(), updateTask.capture(), any());
        when(taskContext.getTask()).thenReturn(updateTask.getValue());
        ClusterState gotState = taskExecutor.getValue()
            .execute(new ClusterStateTaskExecutor.BatchExecutionContext<>(stableState, List.of(taskContext), () -> null));
        assertThat(gotState, sameInstance(stableState));
    }

    @SuppressWarnings("unchecked")
    private void clearTaskQueueInvocations() {
        clearInvocations(taskQueue);
    }

    public void testGracePeriodOnlyForSigterm() throws Exception {
        Arrays.stream(Type.values()).filter(type -> type != Type.SIGTERM).forEach(type -> {
            var allocationDelay = type == Type.RESTART ? TimeValue.timeValueMinutes(randomIntBetween(1, 3)) : null;
            var targetNodeName = type == Type.REPLACE ? randomAlphaOfLength(5) : null;
            assertThat(
                format("type [%s] should work without grace period", type),
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    "node1",
                    type,
                    "test",
                    allocationDelay,
                    targetNodeName,
                    null
                ),
                notNullValue()
            );
            ActionRequestValidationException arve = new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "node1",
                type,
                "test",
                allocationDelay,
                targetNodeName,
                TimeValue.timeValueMinutes(5)
            ).validate();
            assertThat(
                format("type [%s] validation message should be due to grace period", type),
                arve.getMessage(),
                containsString("grace period is only valid for SIGTERM type shutdowns")
            );
        });

        assertThat(
            new PutShutdownNodeAction.Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "node1",
                Type.SIGTERM,
                "test",
                null,
                null,
                TimeValue.timeValueMinutes(5)
            ).validate(),
            nullValue()
        );

        assertThat(
            new PutShutdownNodeAction.Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "node1", Type.SIGTERM, "test", null, null, null)
                .validate()
                .getMessage(),
            containsString("grace period is required for SIGTERM shutdowns")
        );
    }
}
