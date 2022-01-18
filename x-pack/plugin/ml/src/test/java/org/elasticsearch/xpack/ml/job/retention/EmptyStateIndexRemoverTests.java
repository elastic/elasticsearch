/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.retention;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

import java.util.Map;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class EmptyStateIndexRemoverTests extends ESTestCase {

    private Client client;
    private ActionListener<Boolean> listener;
    private ArgumentCaptor<DeleteIndexRequest> deleteIndexRequestCaptor;

    private EmptyStateIndexRemover remover;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpTests() {
        client = mock(Client.class);
        OriginSettingClient originSettingClient = MockOriginSettingClient.mockOriginSettingClient(client, ClientHelper.ML_ORIGIN);
        listener = mock(ActionListener.class);
        deleteIndexRequestCaptor = ArgumentCaptor.forClass(DeleteIndexRequest.class);

        remover = new EmptyStateIndexRemover(originSettingClient, new TaskId("test", 0L));
    }

    @After
    public void verifyNoOtherInteractionsWithMocks() {
        verify(client).settings();
        verify(client, atLeastOnce()).threadPool();
        verifyNoMoreInteractions(client, listener);
    }

    public void testRemove_TimedOut() {
        remover.remove(1.0f, listener, () -> true);

        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(listener).onResponse(false);
    }

    public void testRemove_NoStateIndices() {
        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        when(indicesStatsResponse.getIndices()).thenReturn(Map.of());
        doAnswer(withResponse(indicesStatsResponse)).when(client).execute(any(), any(), any());

        remover.remove(1.0f, listener, () -> false);

        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());
        inOrder.verify(listener).onResponse(true);
    }

    public void testRemove_NoEmptyStateIndices() {
        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        doReturn(
            Map.of(
                ".ml-state-a",
                indexStats(".ml-state-a", 1),
                ".ml-state-b",
                indexStats(".ml-state-b", 2),
                ".ml-state-c",
                indexStats(".ml-state-c", 1),
                ".ml-state-d",
                indexStats(".ml-state-d", 2)
            )
        ).when(indicesStatsResponse).getIndices();
        doAnswer(withResponse(indicesStatsResponse)).when(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());

        remover.remove(1.0f, listener, () -> false);

        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());
        inOrder.verify(listener).onResponse(true);
    }

    private void assertDeleteActionExecuted(boolean acknowledged) {
        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        doReturn(
            Map.of(
                ".ml-state-a",
                indexStats(".ml-state-a", 1),
                ".ml-state-b",
                indexStats(".ml-state-b", 0),
                ".ml-state-c",
                indexStats(".ml-state-c", 2),
                ".ml-state-d",
                indexStats(".ml-state-d", 0),
                ".ml-state-e",
                indexStats(".ml-state-e", 0)
            )
        ).when(indicesStatsResponse).getIndices();
        doAnswer(withResponse(indicesStatsResponse)).when(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());

        GetIndexResponse getIndexResponse = new GetIndexResponse(new String[] { ".ml-state-e" }, null, null, null, null, null);
        doAnswer(withResponse(getIndexResponse)).when(client).execute(eq(GetIndexAction.INSTANCE), any(), any());

        AcknowledgedResponse deleteIndexResponse = AcknowledgedResponse.of(acknowledged);
        doAnswer(withResponse(deleteIndexResponse)).when(client).execute(eq(DeleteIndexAction.INSTANCE), any(), any());

        remover.remove(1.0f, listener, () -> false);

        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());
        inOrder.verify(client).execute(eq(GetIndexAction.INSTANCE), any(), any());
        inOrder.verify(client).execute(eq(DeleteIndexAction.INSTANCE), deleteIndexRequestCaptor.capture(), any());
        inOrder.verify(listener).onResponse(acknowledged);

        DeleteIndexRequest deleteIndexRequest = deleteIndexRequestCaptor.getValue();
        assertThat(deleteIndexRequest.indices(), arrayContainingInAnyOrder(".ml-state-b", ".ml-state-d"));
    }

    public void testRemove_DeleteAcknowledged() {
        assertDeleteActionExecuted(true);
    }

    public void testRemove_DeleteNotAcknowledged() {
        assertDeleteActionExecuted(false);
    }

    public void testRemove_NoIndicesToRemove() {
        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        doReturn(Map.of(".ml-state-a", indexStats(".ml-state-a", 0))).when(indicesStatsResponse).getIndices();
        doAnswer(withResponse(indicesStatsResponse)).when(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());

        GetIndexResponse getIndexResponse = new GetIndexResponse(new String[] { ".ml-state-a" }, null, null, null, null, null);
        doAnswer(withResponse(getIndexResponse)).when(client).execute(eq(GetIndexAction.INSTANCE), any(), any());

        remover.remove(1.0f, listener, () -> false);

        InOrder inOrder = inOrder(client, listener);
        inOrder.verify(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());
        inOrder.verify(client).execute(eq(GetIndexAction.INSTANCE), any(), any());
        inOrder.verify(listener).onResponse(true);
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }

    private static IndexStats indexStats(String index, int docCount) {
        CommonStats indexTotalStats = mock(CommonStats.class);
        when(indexTotalStats.getDocs()).thenReturn(new DocsStats(docCount, 0, 0));
        IndexStats indexStats = mock(IndexStats.class);
        when(indexStats.getIndex()).thenReturn(index);
        when(indexStats.getTotal()).thenReturn(indexTotalStats);
        return indexStats;
    }
}
