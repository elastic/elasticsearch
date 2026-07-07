/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DeleteRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.inference.common.InferencePreferencesCache;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDeleteRegionPolicyActionTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityExecutors());
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulDelete_InvalidatesCache() {
        var cache = mock(InferencePreferencesCache.class);
        var invalidateCount = new AtomicInteger();
        doAnswer(invocation -> {
            invalidateCount.incrementAndGet();
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof DeleteRequest) {
                    var response = mock(DeleteResponse.class);
                    when(response.getResult()).thenReturn(DocWriteResponse.Result.DELETED);
                    listener.onResponse((Response) response);
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var action = new TransportDeleteRegionPolicyAction(mock(TransportService.class), mock(ActionFilters.class), client, cache);

        var listener = new TestPlainActionFuture<AcknowledgedResponse>();
        action.doExecute(mock(Task.class), new DeleteRegionPolicyAction.Request(), listener);

        assertThat(listener.actionGet(TEST_REQUEST_TIMEOUT), is(AcknowledgedResponse.TRUE));
        assertThat(invalidateCount.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulDelete_RefreshesAuthorizedEndpoints() {
        var cache = mock(InferencePreferencesCache.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var refreshCount = new AtomicInteger();
        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof DeleteRequest) {
                    var response = mock(DeleteResponse.class);
                    when(response.getResult()).thenReturn(DocWriteResponse.Result.DELETED);
                    listener.onResponse((Response) response);
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    refreshCount.incrementAndGet();
                    listener.onResponse((Response) ActionResponse.Empty.INSTANCE);
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var action = new TransportDeleteRegionPolicyAction(mock(TransportService.class), mock(ActionFilters.class), client, cache);

        var listener = new TestPlainActionFuture<AcknowledgedResponse>();
        action.doExecute(mock(Task.class), new DeleteRegionPolicyAction.Request(), listener);

        listener.actionGet(TEST_REQUEST_TIMEOUT);
        assertThat(refreshCount.get(), is(1));
    }

    @SuppressWarnings("unchecked")
    public void testSuccessfulDelete_SwallowsRefreshFailure() {
        var cache = mock(InferencePreferencesCache.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArgument(0)).onResponse(null);
            return null;
        }).when(cache).invalidate(any());

        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (request instanceof DeleteRequest) {
                    var response = mock(DeleteResponse.class);
                    when(response.getResult()).thenReturn(DocWriteResponse.Result.DELETED);
                    listener.onResponse((Response) response);
                } else if (request instanceof RefreshAuthorizedEndpointsAction.Request) {
                    listener.onFailure(new RuntimeException("refresh failed"));
                } else {
                    fail("unexpected request type: " + request);
                }
            }
        };

        var action = new TransportDeleteRegionPolicyAction(mock(TransportService.class), mock(ActionFilters.class), client, cache);

        var listener = new TestPlainActionFuture<AcknowledgedResponse>();
        action.doExecute(mock(Task.class), new DeleteRegionPolicyAction.Request(), listener);

        // The delete should still succeed even though the refresh failed
        assertThat(listener.actionGet(TEST_REQUEST_TIMEOUT), is(AcknowledgedResponse.TRUE));
    }

    @SuppressWarnings("unchecked")
    public void testNotFoundDelete_DoesNotInvalidateCache() {
        var cache = mock(InferencePreferencesCache.class);

        var client = new NoOpClient(threadPool) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                var response = mock(DeleteResponse.class);
                when(response.getResult()).thenReturn(DocWriteResponse.Result.NOT_FOUND);
                listener.onResponse((Response) response);
            }
        };

        var action = new TransportDeleteRegionPolicyAction(mock(TransportService.class), mock(ActionFilters.class), client, cache);

        var listener = new TestPlainActionFuture<AcknowledgedResponse>();
        action.doExecute(mock(Task.class), new DeleteRegionPolicyAction.Request(), listener);

        expectThrows(Exception.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
        verify(cache, never()).invalidate(any());
    }
}
