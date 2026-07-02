/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.inference.action.DeleteRegionPolicyAction;
import org.elasticsearch.xpack.core.inference.action.RefreshAuthorizedEndpointsAction;
import org.elasticsearch.xpack.core.inference.regionpolicy.RegionPolicyDoc;
import org.elasticsearch.xpack.inference.InferenceIndex;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportDeleteRegionPolicyActionTests extends ESTestCase {

    private static final ShardId SHARD_ID = new ShardId(InferenceIndex.INDEX_NAME, "_na_", 0);

    private Client mockClient;

    @Before
    public void init() {
        mockClient = mock(Client.class);
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
    }

    public void testDelete_TriggersAuthorizationRefresh_AndReturnsAcknowledged() {
        givenDeleteRespondsWith(deletedResponse());
        givenRefreshRespondsWith(ActionResponse.Empty.INSTANCE);

        var future = new TestPlainActionFuture<AcknowledgedResponse>();
        createAction(Settings.EMPTY).doExecute(null, new DeleteRegionPolicyAction.Request(), future);

        assertThat(future.actionGet(TEST_REQUEST_TIMEOUT), is(AcknowledgedResponse.TRUE));
        verify(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    public void testDelete_SwallowsRefreshFailure_AndStillReturnsAcknowledged() {
        givenDeleteRespondsWith(deletedResponse());
        givenRefreshFailsWith(new RuntimeException("refresh failed"));

        var future = new TestPlainActionFuture<AcknowledgedResponse>();
        createAction(Settings.EMPTY).doExecute(null, new DeleteRegionPolicyAction.Request(), future);

        assertThat(future.actionGet(TEST_REQUEST_TIMEOUT), is(AcknowledgedResponse.TRUE));
        verify(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    public void testDelete_WhenPolicyNotFound_FailsAndDoesNotRefresh() {
        givenDeleteRespondsWith(notFoundResponse());

        var future = new TestPlainActionFuture<AcknowledgedResponse>();
        createAction(Settings.EMPTY).doExecute(null, new DeleteRegionPolicyAction.Request(), future);

        expectThrows(ResourceNotFoundException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        verify(mockClient, never()).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    public void testDelete_WhenIndexNotFound_FailsAndDoesNotRefresh() {
        givenDeleteFailsWith(new IndexNotFoundException(InferenceIndex.INDEX_NAME));

        var future = new TestPlainActionFuture<AcknowledgedResponse>();
        createAction(Settings.EMPTY).doExecute(null, new DeleteRegionPolicyAction.Request(), future);

        expectThrows(ResourceNotFoundException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        verify(mockClient, never()).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    private static DeleteResponse deletedResponse() {
        return new DeleteResponse(SHARD_ID, RegionPolicyDoc.DOCUMENT_ID, 1L, 1L, 2L, true);
    }

    private static DeleteResponse notFoundResponse() {
        return new DeleteResponse(SHARD_ID, RegionPolicyDoc.DOCUMENT_ID, 1L, 1L, 1L, false);
    }

    private void givenDeleteRespondsWith(DeleteResponse response) {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(TransportDeleteAction.TYPE), any(), any());
    }

    private void givenDeleteFailsWith(Exception exception) {
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(mockClient).execute(eq(TransportDeleteAction.TYPE), any(), any());
    }

    private void givenRefreshRespondsWith(ActionResponse.Empty response) {
        doAnswer(invocation -> {
            ActionListener<ActionResponse.Empty> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    private void givenRefreshFailsWith(Exception exception) {
        doAnswer(invocation -> {
            ActionListener<ActionResponse.Empty> listener = invocation.getArgument(2);
            listener.onFailure(exception);
            return null;
        }).when(mockClient).execute(eq(RefreshAuthorizedEndpointsAction.INSTANCE), any(), any());
    }

    private TransportDeleteRegionPolicyAction createAction(Settings settings) {
        return new TransportDeleteRegionPolicyAction(settings, mock(TransportService.class), mock(ActionFilters.class), mockClient);
    }
}
