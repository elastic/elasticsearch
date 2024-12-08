/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.analysis;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.ClosePointInTimeResponse;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.elasticsearch.xpack.ql.type.DefaultDataTypeRegistry;
import org.elasticsearch.xpack.sql.SqlTestUtils;
import org.elasticsearch.xpack.sql.action.SqlQueryRequest;
import org.elasticsearch.xpack.sql.action.SqlQueryRequestBuilder;
import org.elasticsearch.xpack.sql.action.SqlQueryResponse;
import org.elasticsearch.xpack.sql.action.SqlQueryTask;
import org.elasticsearch.xpack.sql.execution.PlanExecutor;
import org.elasticsearch.xpack.sql.plugin.TransportSqlQueryAction;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlVersion;
import org.mockito.ArgumentCaptor;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CancellationTests extends ESTestCase {

    public void testCancellationBeforeFieldCaps() throws InterruptedException {
        Client client = mock(Client.class);
        SqlQueryTask task = randomTask();
        TaskCancelHelper.cancel(task, "simulated");
        ClusterService mockClusterService = mockClusterService();

        IndexResolver indexResolver = indexResolver(client);
        PlanExecutor planExecutor = new PlanExecutor(client, indexResolver, new NamedWriteableRegistry(Collections.emptyList()));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        SqlQueryRequest request = new SqlQueryRequestBuilder(client).query("SELECT foo FROM bar").request();
        TransportSqlQueryAction.operation(planExecutor, task, request, new ActionListener<>() {
            @Override
            public void onResponse(SqlQueryResponse sqlSearchResponse) {
                fail("Shouldn't be here");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(TaskCancelledException.class));
                countDownLatch.countDown();
            }
        }, "", mock(TransportService.class), mockClusterService);
        countDownLatch.await();
        verify(client, times(1)).settings();
        verify(client, times(1)).threadPool();
        verifyNoMoreInteractions(client);
    }

    private Map<String, Map<String, FieldCapabilities>> fields(String[] indices) {
        FieldCapabilities fooField = new FieldCapabilities("foo", "integer", false, true, true, indices, null, null, emptyMap());
        FieldCapabilities categoryField = new FieldCapabilities(
            "event.category",
            "keyword",
            false,
            true,
            true,
            indices,
            null,
            null,
            emptyMap()
        );
        FieldCapabilities timestampField = new FieldCapabilities("@timestamp", "date", false, true, true, indices, null, null, emptyMap());
        Map<String, Map<String, FieldCapabilities>> fields = new HashMap<>();
        fields.put(fooField.getName(), singletonMap(fooField.getName(), fooField));
        fields.put(categoryField.getName(), singletonMap(categoryField.getName(), categoryField));
        fields.put(timestampField.getName(), singletonMap(timestampField.getName(), timestampField));
        return fields;
    }

    public void testCancellationBeforeSearch() throws InterruptedException {
        Client client = mock(Client.class);

        SqlQueryTask task = randomTask();
        ClusterService mockClusterService = mockClusterService();

        String[] indices = new String[] { "endgame" };

        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getIndices()).thenReturn(indices);
        when(fieldCapabilitiesResponse.get()).thenReturn(fields(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[1];
            TaskCancelHelper.cancel(task, "simulated");
            listener.onResponse(fieldCapabilitiesResponse);
            return null;
        }).when(client).fieldCaps(any(), any());

        IndexResolver indexResolver = indexResolver(client);
        PlanExecutor planExecutor = new PlanExecutor(client, indexResolver, new NamedWriteableRegistry(Collections.emptyList()));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        SqlQueryRequest request = new SqlQueryRequestBuilder(client).query("SELECT foo FROM " + indices[0]).request();
        TransportSqlQueryAction.operation(planExecutor, task, request, new ActionListener<>() {
            @Override
            public void onResponse(SqlQueryResponse sqlSearchResponse) {
                fail("Shouldn't be here");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(TaskCancelledException.class));
                countDownLatch.countDown();
            }
        }, "", mock(TransportService.class), mockClusterService);
        countDownLatch.await();
        verify(client, times(1)).fieldCaps(any(), any());
        verify(client, times(1)).settings();
        verify(client, times(1)).threadPool();
        verifyNoMoreInteractions(client);
    }

    public void testCancellationDuringSearchWithSearchHitCursor() throws InterruptedException {
        testCancellationDuringSearch("SELECT foo FROM endgame");
    }

    public void testCancellationDuringSearchWithCompositeAggCursor() throws InterruptedException {
        testCancellationDuringSearch("SELECT foo FROM endgame GROUP BY foo");
    }

    public void testCancellationDuringSearch(String query) throws InterruptedException {
        Client client = mock(Client.class);

        SqlQueryTask task = randomTask();
        String nodeId = randomAlphaOfLength(10);
        ClusterService mockClusterService = mockClusterService(nodeId);

        String[] indices = new String[] { "endgame" };
        BytesReference pitId = new BytesArray(randomAlphaOfLength(10));

        // Emulation of field capabilities
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getIndices()).thenReturn(indices);
        when(fieldCapabilitiesResponse.get()).thenReturn(fields(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[1];
            listener.onResponse(fieldCapabilitiesResponse);
            return null;
        }).when(client).fieldCaps(any(), any());

        // Emulation of open pit
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<OpenPointInTimeResponse> listener = (ActionListener<OpenPointInTimeResponse>) invocation.getArguments()[2];
            listener.onResponse(new OpenPointInTimeResponse(pitId, 1, 1, 0, 0));
            return null;
        }).when(client).execute(eq(TransportOpenPointInTimeAction.TYPE), any(), any());

        // Emulation of search cancellation
        ArgumentCaptor<SearchRequest> searchRequestCaptor = ArgumentCaptor.forClass(SearchRequest.class);
        when(client.prepareSearch(any())).thenReturn(new SearchRequestBuilder(client).setIndices(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            SearchRequest request = (SearchRequest) invocation.getArguments()[1];
            assertEquals(pitId, request.pointInTimeBuilder().getEncodedId());
            TaskId parentTask = request.getParentTask();
            assertNotNull(parentTask);
            assertEquals(task.getId(), parentTask.getId());
            assertEquals(nodeId, parentTask.getNodeId());
            @SuppressWarnings("unchecked")
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) invocation.getArguments()[2];
            listener.onFailure(new TaskCancelledException("cancelled"));
            return null;
        }).when(client).execute(eq(TransportSearchAction.TYPE), searchRequestCaptor.capture(), any());

        // Emulation of close pit
        doAnswer(invocation -> {
            ClosePointInTimeRequest request = (ClosePointInTimeRequest) invocation.getArguments()[1];
            assertEquals(pitId, request.getId());

            @SuppressWarnings("unchecked")
            ActionListener<ClosePointInTimeResponse> listener = (ActionListener<ClosePointInTimeResponse>) invocation.getArguments()[2];
            listener.onResponse(new ClosePointInTimeResponse(true, 1));
            return null;
        }).when(client).execute(eq(TransportClosePointInTimeAction.TYPE), any(), any());

        IndexResolver indexResolver = indexResolver(client);
        PlanExecutor planExecutor = new PlanExecutor(client, indexResolver, new NamedWriteableRegistry(Collections.emptyList()));
        SqlQueryRequest request = new SqlQueryRequestBuilder(client).query(query).request();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        TransportSqlQueryAction.operation(planExecutor, task, request, new ActionListener<>() {
            @Override
            public void onResponse(SqlQueryResponse sqlSearchResponse) {
                fail("Shouldn't be here");
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(TaskCancelledException.class));
                countDownLatch.countDown();
            }
        }, "", mock(TransportService.class), mockClusterService);
        assertTrue(countDownLatch.await(5, TimeUnit.SECONDS));
        // Final verification to ensure no more interaction
        verify(client).fieldCaps(any(), any());
        verify(client, times(1)).execute(eq(TransportOpenPointInTimeAction.TYPE), any(), any());
        verify(client, times(1)).execute(eq(TransportSearchAction.TYPE), any(), any());
        verify(client, times(1)).execute(eq(TransportClosePointInTimeAction.TYPE), any(), any());
        verify(client, times(1)).settings();
        verify(client, times(1)).threadPool();
        verifyNoMoreInteractions(client);
    }

    private ClusterService mockClusterService() {
        return mockClusterService(null);
    }

    private ClusterService mockClusterService(String nodeId) {
        final ClusterService mockClusterService = mock(ClusterService.class);
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        final ClusterName mockClusterName = mock(ClusterName.class);
        when(mockNode.getId()).thenReturn(nodeId == null ? randomAlphaOfLength(10) : nodeId);
        when(mockClusterService.localNode()).thenReturn(mockNode);
        when(mockClusterName.value()).thenReturn(randomAlphaOfLength(10));
        when(mockClusterService.getClusterName()).thenReturn(mockClusterName);
        return mockClusterService;
    }

    private static IndexResolver indexResolver(Client client) {
        return new IndexResolver(client, randomAlphaOfLength(10), DefaultDataTypeRegistry.INSTANCE, Collections::emptySet);
    }

    private static SqlQueryTask randomTask() {
        return SqlTestUtils.randomTask(randomLong(), randomFrom(Mode.values()), SqlVersion.fromString("1.2.3"));
    }

}
