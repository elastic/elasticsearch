/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.InternalDeleteInferenceEndpointsAction;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.junit.After;
import org.junit.Before;

import java.util.Set;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TransportInternalDeleteEndpointsActionTests extends ESTestCase {

    private TransportInternalDeleteEndpointsAction action;
    private ThreadPool threadPool;
    private ModelRegistry mockModelRegistry;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        mockModelRegistry = mock(ModelRegistry.class);
        action = new TransportInternalDeleteEndpointsAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            threadPool,
            mock(ActionFilters.class),
            mockModelRegistry
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testMasterOperation_DelegatesDeleteToModelRegistry() {
        var ids = Set.of("id-1", "id-2");
        doAnswer(invocation -> {
            invocation.<org.elasticsearch.action.ActionListener<Boolean>>getArgument(1).onResponse(true);
            return Void.TYPE;
        }).when(mockModelRegistry).deleteModels(eq(ids), any());

        var future = new TestPlainActionFuture<AcknowledgedResponse>();
        action.masterOperation(
            mock(Task.class),
            new InternalDeleteInferenceEndpointsAction.Request(ids, TimeValue.THIRTY_SECONDS),
            ClusterState.EMPTY_STATE,
            future
        );

        var response = future.actionGet(TEST_REQUEST_TIMEOUT);
        assertTrue(response.isAcknowledged());
        verify(mockModelRegistry).deleteModels(eq(ids), any());
    }

    public void testMasterOperation_AcknowledgedTrue_Even_WhenRegistryReturnsFalse() {
        var ids = Set.of("id-1");
        doAnswer(invocation -> {
            invocation.<org.elasticsearch.action.ActionListener<Boolean>>getArgument(1).onResponse(false);
            return Void.TYPE;
        }).when(mockModelRegistry).deleteModels(eq(ids), any());

        var future = new TestPlainActionFuture<AcknowledgedResponse>();
        action.masterOperation(
            mock(Task.class),
            new InternalDeleteInferenceEndpointsAction.Request(ids, TimeValue.THIRTY_SECONDS),
            ClusterState.EMPTY_STATE,
            future
        );

        var response = future.actionGet(TEST_REQUEST_TIMEOUT);
        assertTrue(response.isAcknowledged());
    }

    public void testMasterOperation_PropagatesFailure() {
        var ids = Set.of("id-1");
        doAnswer(invocation -> {
            invocation.<ActionListener<Boolean>>getArgument(1).onFailure(new RuntimeException("delete failed"));
            return Void.TYPE;
        }).when(mockModelRegistry).deleteModels(eq(ids), any());

        var future = new TestPlainActionFuture<AcknowledgedResponse>();
        action.masterOperation(
            mock(Task.class),
            new InternalDeleteInferenceEndpointsAction.Request(ids, TimeValue.THIRTY_SECONDS),
            ClusterState.EMPTY_STATE,
            future
        );

        var exception = expectThrows(RuntimeException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(exception.getMessage(), is("delete failed"));
    }
}
